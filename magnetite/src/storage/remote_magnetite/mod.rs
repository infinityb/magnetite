use std::any::Any;
use std::collections::{BTreeMap, VecDeque};
use std::fs::OpenOptions;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use futures::future::Future;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use metrics::counter;
use rand::{thread_rng, Rng};
use slab::Slab;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{broadcast, mpsc, oneshot, RwLock};
use tracing::{event, Level};

use magnetite_common::TorrentId;

use crate::control::messages::BusFindPieceFetcher;
use crate::model::{get_torrent_salsa, MagnetiteError, ProtocolViolation};
use crate::CommonInit;

use super::{
    disk_cache::DiskCacheWrapper,
    memory_cache::MemoryCacheWrapper,
    root_storage_service::RootStorageOpts,
    state_holder_system::{BusNewState, State},
    utils as storage_utils, GetPieceRequest, GetPieceRequestChannel, GetPieceRequestResolver,
    PieceStorageEngineDumb,
};

mod wire;

use self::wire::{
    read_request_from_buffer, read_response_from_buffer, write_request_to_buffer,
    write_response_to_buffer, MagnetiteWirePieceRequest, MagnetiteWirePieceResponse,
    MagnetiteWireRequest, MagnetiteWireRequestPayload, MagnetiteWireResponse,
    MagnetiteWireResponsePayload,
};

const BUF_SIZE_REQUEST: usize = 4 * 1024;
const BUF_SIZE_RESPONSE: usize = 64 * 1024;
const WINDOW_SIZE_HARD_LIMIT: usize = 4096;
const WINDOW_SIZE_START: usize = 5;
const TXID_RANDOM_MASK: u64 = 0xFFFF_FFFF_FFFF_0000;
const LATENCY_TARGET_MIN_MILLISECONDS: u32 = 180;
const LATENCY_TARGET_MAX_MILLISECONDS: u32 = 1800;
const CHUNK_SIZE: u32 = 32 * 1024; // 26ms @ 10Mbps

#[derive(Debug)]
struct PiecePendingRequestFactory {
    piece_slab_index: usize,
    content_key: TorrentId,
    piece_index: u32,
    piece_length: u32,
    piece_requested_length: u32,
}

impl PiecePendingRequestFactory {
    pub fn new(piece_slab_index: usize, gp: &GetPieceRequest) -> Self {
        let (_, fetch_length) =
            storage_utils::compute_offset_length(gp.piece_index, gp.piece_length, gp.total_length);

        let mut piece_length = gp.piece_length;
        if fetch_length < piece_length {
            piece_length = fetch_length;
        }

        PiecePendingRequestFactory {
            piece_slab_index,
            content_key: gp.content_key,
            piece_index: gp.piece_index,
            piece_length,
            piece_requested_length: 0,
        }
    }
}

impl Iterator for PiecePendingRequestFactory {
    type Item = PendingRequest;

    fn next(&mut self) -> Option<PendingRequest> {
        let mut remaining = self.piece_length - self.piece_requested_length;

        if CHUNK_SIZE < remaining {
            remaining = CHUNK_SIZE;
        }
        if remaining == 0 {
            return None;
        }

        let request_offset = self.piece_requested_length;
        self.piece_requested_length += remaining;

        let cookie: u64 = thread_rng().gen();
        let emit = PendingRequest::ChunkRequest(PendingChunkRequest {
            piece_slab_index: self.piece_slab_index,
            content_key: self.content_key,
            piece_index: self.piece_index,
            piece_offset: request_offset,
            fetch_length: remaining,
            cookie_data: cookie,
        });

        Some(emit)
    }
}

impl PendingRequest {
    fn make_request_state(&self, now: Instant) -> RequestState {
        let PendingRequest::ChunkRequest(ref p) = self;
        RequestState {
            piece_slab_index: p.piece_slab_index,
            req_submit_time: now,
            req_kind: MagnetiteRequestKind::Piece,
            txid_cookie: p.cookie_data & TXID_RANDOM_MASK,
            piece_offset: p.piece_offset,
            fetch_length: p.fetch_length,
        }
    }

    fn make_wire_request(&self, txid: usize) -> MagnetiteWireRequest {
        let PendingRequest::ChunkRequest(ref p) = self;
        let mut txid = txid as u64;
        assert_eq!(txid & TXID_RANDOM_MASK, 0);
        txid |= p.cookie_data & TXID_RANDOM_MASK;

        MagnetiteWireRequest {
            txid,
            payload: MagnetiteWireRequestPayload::Piece(MagnetiteWirePieceRequest {
                content_key: p.content_key,
                piece_index: p.piece_index,
                piece_offset: p.piece_offset,
                fetch_length: p.fetch_length,
            }),
        }
    }
}

#[derive(Debug)]
enum PendingRequest {
    ChunkRequest(PendingChunkRequest),
}

#[derive(Debug)]
struct PendingChunkRequest {
    piece_slab_index: usize,
    content_key: TorrentId,
    piece_index: u32,
    piece_offset: u32,
    fetch_length: u32,
    cookie_data: u64,
}

struct RequestState {
    piece_slab_index: usize,
    req_submit_time: Instant,
    req_kind: MagnetiteRequestKind,
    txid_cookie: u64,
    piece_offset: u32,
    fetch_length: u32,
}

struct PieceState {
    piece_length: u32,
    data_slices: BTreeMap<u32, Vec<u8>>,
    data: BytesMut,
    responder: oneshot::Sender<Result<Bytes, MagnetiteError>>,
}

impl PieceState {
    fn new(
        piece_length: u32,
        responder: oneshot::Sender<Result<Bytes, MagnetiteError>>,
    ) -> PieceState {
        PieceState {
            piece_length,
            data_slices: Default::default(),
            data: BytesMut::with_capacity(piece_length as usize),
            responder,
        }
    }

    // returns Ok(true) if the piece completed.
    fn merge_response(
        &mut self,
        rr: &RequestState,
        wr: &MagnetiteWireResponse,
    ) -> Result<bool, MagnetiteError> {
        match wr.payload {
            MagnetiteWireResponsePayload::Piece(ref resp) => {
                if resp.data.len() as u64 != u64::from(rr.fetch_length) {
                    return Err(ProtocolViolation.into());
                }

                self.data_slices.insert(rr.piece_offset, resp.data.clone());
            }
        }
        let mut total_length = 0;
        for v in self.data_slices.values() {
            total_length += v.len() as u64;
        }
        Ok(if total_length == self.piece_length as u64 {
            // we're done, let's reassemble
            for v in self.data_slices.values() {
                self.data.extend_from_slice(&v[..]);
            }
            true
        } else {
            false
        })
    }
}

#[derive(Clone)]
pub struct RemoteMagnetite {
    sender: mpsc::Sender<MagnetiteRequest>,
}

struct LatencyStats {
    acc: u32,
    history: VecDeque<u32>,
}

impl LatencyStats {
    fn new() -> LatencyStats {
        LatencyStats {
            acc: 0,
            history: Default::default(),
        }
    }

    fn add(&mut self, d: Duration) {
        let last_latency_ms = d.as_millis() as u32;
        if self.history.len() == 50 {
            self.acc -= self.history.pop_front().unwrap();
        }
        self.history.push_back(last_latency_ms);
        self.acc += last_latency_ms;
    }

    fn average(&self) -> Option<u32> {
        if self.history.is_empty() {
            None
        } else {
            Some(self.acc / self.history.len() as u32)
        }
    }
}

fn resolve_response(
    resp: MagnetiteWireResponse,
    inflight: &mut Slab<RequestState>,
    pieces: &mut Slab<PieceState>,
    latency_stats: &mut LatencyStats,
) -> Result<(), MagnetiteError> {
    let txid_cookie = resp.txid & TXID_RANDOM_MASK;
    let txid_value = (resp.txid & !TXID_RANDOM_MASK) as usize;

    let request_state = inflight.get(txid_value).ok_or_else(|| ProtocolViolation)?;

    if request_state.txid_cookie != txid_cookie {
        return Err(ProtocolViolation.into());
    }
    if request_state.req_kind != resp.kind() {
        return Err(ProtocolViolation.into());
    }

    let request_state = inflight.remove(txid_value);

    latency_stats.add(request_state.req_submit_time.elapsed());

    let piece_state = pieces.get_mut(request_state.piece_slab_index).unwrap();
    if piece_state.merge_response(&request_state, &resp)? {
        let PieceState {
            data, responder, ..
        } = pieces.remove(request_state.piece_slab_index);

        if let Err(..) = responder.send(Ok(data.freeze())) {
            event!(
                Level::WARN,
                "{}:{}: responded disappeared",
                file!(),
                line!()
            );
        }
    }

    Ok(())
}

fn run_connected<'a>(
    mut socket: TcpStream,
    incoming_request_queue: &'a mut mpsc::Receiver<GetPieceRequestResolver>,
) -> Pin<Box<dyn Future<Output = Result<bool, MagnetiteError>> + Send + 'a>> {
    async move {
        let mut current_window_size: usize = WINDOW_SIZE_START;
        let mut inflight: Slab<RequestState> = Default::default();
        // this shouldn't really exceed one or two elements, I think.
        let mut piece_state: Slab<PieceState> = Default::default();

        let mut wbuf = BytesMut::with_capacity(BUF_SIZE_REQUEST);
        let mut rbuf = BytesMut::with_capacity(BUF_SIZE_RESPONSE);
        let mut latency = LatencyStats::new();
        // get a piece request, convert it into chunk requests,
        // send remaining_window_size chunk requests, wait until a chunk returns
        // before sending one more.  Between sends, we can check if the piece future
        // is cancelled.
        //
        let mut split_request_queue: VecDeque<PendingRequest> = VecDeque::new();
        let mut request_queue_open = true;
        let mut connection_open = true;

        while connection_open {
            if let Some(average) = latency.average() {
                if average < LATENCY_TARGET_MIN_MILLISECONDS {
                    let prev_window_size = current_window_size;
                    current_window_size += 1;
                    if WINDOW_SIZE_HARD_LIMIT < current_window_size {
                        current_window_size = WINDOW_SIZE_HARD_LIMIT;
                    }
                    if prev_window_size != current_window_size {
                        event!(
                            Level::DEBUG,
                            "increased window size to {}",
                            current_window_size
                        );
                    }
                }
                if LATENCY_TARGET_MAX_MILLISECONDS < average {
                    let prev_window_size = current_window_size;
                    current_window_size /= 2;
                    if current_window_size < 2 {
                        current_window_size = 2;
                    }
                    if prev_window_size != current_window_size {
                        event!(
                            Level::INFO,
                            "halved window size to {}, current latency {}ms",
                            current_window_size,
                            average
                        );
                    }
                }
            }

            let take_count = if inflight.len() < current_window_size {
                current_window_size - inflight.len()
            } else {
                0
            };

            // opportunistically fill split_request_queue
            while split_request_queue.len() < take_count && request_queue_open {
                match incoming_request_queue.try_recv() {
                    Ok(GetPieceRequestResolver { request, resolver }) => {
                        let (_, fetch_length) = storage_utils::compute_offset_length(
                            request.piece_index,
                            request.piece_length,
                            request.total_length,
                        );

                        let ps = PieceState::new(fetch_length, resolver);
                        let pf = PiecePendingRequestFactory::new(piece_state.insert(ps), &request);
                        split_request_queue.extend(pf);
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Closed) => {
                        request_queue_open = false;
                    }
                };
            }

            for _ in 0..take_count {
                let now = Instant::now();
                if let Some(ri) = split_request_queue.pop_front() {
                    enqueue_request_partial(now, &mut wbuf, &mut inflight, ri)?;
                } else {
                    break;
                }
            }

            let to_write = wbuf.split().freeze();
            if !to_write.is_empty() {
                socket.write_all(&to_write[..]).await?;
            }
            drop(to_write);

            // now: either slab is full or split_request_queue is empty.  If slab is full, we shouldn't
            // take any more requests, and we must complete a response read.
            while inflight.len() == current_window_size {
                let is_eof = socket.read_buf(&mut rbuf).await? == 0;

                while let Some(resp) = read_response_from_buffer(&mut rbuf)? {
                    resolve_response(resp, &mut inflight, &mut piece_state, &mut latency)?;
                }

                if is_eof {
                    connection_open = false;
                    break;
                }
            }

            if !request_queue_open && inflight.is_empty() && split_request_queue.is_empty() {
                break;
            }

            if request_queue_open || !inflight.is_empty() {
                ::tokio::select! {
                    req = incoming_request_queue.recv(), if request_queue_open => {
                        if let Some(v) = req {
                            let GetPieceRequestResolver { request, resolver } = v;
                            let (_, fetch_length) = storage_utils::compute_offset_length(
                                request.piece_index, request.piece_length, request.total_length);

                            let ps = PieceState::new(fetch_length, resolver);
                            let pf = PiecePendingRequestFactory::new(piece_state.insert(ps), &request);
                            split_request_queue.extend(pf);
                        } else {
                            request_queue_open = false;
                        }
                    },
                    read_res = socket.read_buf(&mut rbuf), if !inflight.is_empty() => {
                        if read_res? == 0 {
                            connection_open = false;
                        }
                        while let Some(resp) = read_response_from_buffer(&mut rbuf)? {
                            resolve_response(resp, &mut inflight, &mut piece_state, &mut latency)?;
                        }
                    }
                }
            }
        }

        Ok(request_queue_open)
    }.boxed()
}

pub fn start_remote_magnetite_client(
    common: CommonInit,
    connect_address: &str,
    opts: RootStorageOpts,
) -> Pin<Box<dyn Future<Output = Result<(), failure::Error>> + Send + 'static>> {
    let (tx, rx) = mpsc::channel(10);
    let control = tokio::task::spawn(start_remote_magnetite_client_control_loop(
        common.clone(),
        tx,
        opts,
    ));
    let data = tokio::task::spawn(start_remote_magnetite_client_data_loop(
        common,
        rx,
        connect_address.to_string(),
    ));

    async {
        let (a, b) = futures::future::join(control, data).await;

        a??;
        b??;

        event!(
            Level::INFO,
            "shut down remote magnetite piece storage engine"
        );

        Ok(())
    }
    .boxed()
}

fn start_remote_magnetite_client_control_loop(
    common: CommonInit,
    requests: mpsc::Sender<GetPieceRequestResolver>,
    opts: RootStorageOpts,
) -> Pin<Box<dyn Future<Output = Result<(), failure::Error>> + Send + 'static>> {
    let CommonInit {
        ebus,
        init_sig,
        mut term_sig,
    } = common;

    let mut ebus_incoming = ebus.subscribe();
    drop(ebus);
    drop(init_sig); // we've finished initializing.

    let gp: GetPieceRequestChannel = requests.into();

    async move {

        let engine_tmp: Box<dyn PieceStorageEngineDumb + Send + Sync + 'static> = Box::new(gp);
        let mut engine: Arc<dyn PieceStorageEngineDumb + Send + Sync + 'static> = engine_tmp.into();
        if opts.disk_cache_size_bytes != 0 {
            let mut engine_disk_builder = DiskCacheWrapper::build_with_capacity_bytes(opts.disk_cache_size_bytes);
            
            if let Some(cr) = get_torrent_salsa(&opts.disk_crypto_secret, &TorrentId::zero()) {
                engine_disk_builder.set_crypto(cr);
            }

            let cache_file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open("magnetite.cache")?;
            let engine_disk = engine_disk_builder.build(cache_file.into(), engine);

            let engine_tmp: Box<dyn PieceStorageEngineDumb + Send + Sync + 'static> = Box::new(engine_disk);
            engine = engine_tmp.into();
        }
        if opts.memory_cache_size_bytes != 0 {
            let engine_mem = MemoryCacheWrapper::build_with_capacity_bytes(opts.memory_cache_size_bytes)
                .build(engine);

            let engine_tmp: Box<dyn PieceStorageEngineDumb + Send + Sync + 'static> = Box::new(engine_mem);
            engine = engine_tmp.into();
        }

        loop {
            tokio::select! {
                _ = &mut term_sig => {
                    drop(ebus_incoming);
                    break;
                }
                control_req = ebus_incoming.recv() => {
                    let creq = match control_req {
                        Ok(v) => v,
                        Err(broadcast::RecvError::Closed) => {
                            break;
                        }
                        Err(broadcast::RecvError::Lagged(..)) => {
                            event!(Level::ERROR, "piece fetch loop lagged - we're dropping requests");
                            continue;
                        },
                    };

                    if let Some(pf) = creq.downcast_ref::<BusFindPieceFetcher>() {
                        let pf: BusFindPieceFetcher = pf.clone();
                        tokio::task::spawn(pf.respond(engine.clone()));
                    };
                }
            }
        }

        event!(Level::INFO, "piece fetcher control system has shut down");

        Ok(())
    }.boxed()
}

fn start_remote_magnetite_client_data_loop(
    common: CommonInit,
    mut incoming_request_queue: mpsc::Receiver<GetPieceRequestResolver>,
    addr: String,
) -> Pin<Box<dyn Future<Output = Result<(), failure::Error>> + Send + 'static>> {
    async move {
        use tokio::time::delay_for;

        let CommonInit {
            ebus,
            init_sig,
            mut term_sig,
        } = common;

        drop(ebus);
        drop(init_sig); // we've finished initializing.

        const FAILURE_DELAY_MILLISECONDS_MAX: u64 = 60_000;
        const FAILURE_DELAY_MILLISECONDS_START: u64 = 100;
        let mut failure_delay_milliseconds: u64 = 0;

        loop {
            let term_sig_p = Pin::new(&mut term_sig);
            let connect = TcpStream::connect(&addr).boxed();
            let socket = match term_sig_p.await_with(connect).await {
                Ok(Ok(v)) => v,
                Ok(Err(err)) => {
                    event!(Level::ERROR, "upstream connection failure: {}", err);

                    failure_delay_milliseconds += failure_delay_milliseconds;
                    if FAILURE_DELAY_MILLISECONDS_MAX < failure_delay_milliseconds {
                        failure_delay_milliseconds = FAILURE_DELAY_MILLISECONDS_MAX;
                    }

                    delay_for(Duration::from_millis(failure_delay_milliseconds)).await;

                    continue;
                }
                Err(()) => {
                    break;
                }
            };

            failure_delay_milliseconds = FAILURE_DELAY_MILLISECONDS_START;

            let run_fut = run_connected(socket, &mut incoming_request_queue);
            let term_sig_p = Pin::new(&mut term_sig);
            match term_sig_p.await_with(run_fut).await {
                Ok(Ok(false)) => break,
                Ok(Ok(true)) => (),
                Ok(Err(err)) => {
                    event!(Level::ERROR, "upstream connection aborted: {}", err);
                    continue;
                }
                Err(()) => break,
            }
        }

        event!(Level::INFO, "piece fetcher data system has shut down");

        Ok(())
    }
    .boxed()
}

pub fn start_remote_magnetite_host_service(
    common: CommonInit,
    bind_address: &str,
) -> Pin<Box<dyn Future<Output = Result<(), failure::Error>> + Send + 'static>> {
    let bind_address = bind_address.to_string();
    async move {
        let CommonInit { mut ebus, mut init_sig, mut term_sig } = common;

        let mut ebus_incoming = ebus.subscribe();
        drop(init_sig);

        let (bus_req, mut rx) = BusFindPieceFetcher::pair();

        // FIXME: obviously this is bad.  Maybe we can use a barrier here.
        tokio::time::delay_for(Duration::from_millis(10)).await;

        let bus_req: Box<dyn Any + Send + Sync> = Box::new(bus_req);
        ebus.send(bus_req.into()).unwrap();
        drop(ebus);

        let piece_fetcher = rx.next().await.unwrap();

        let mut listener = TcpListener::bind(&bind_address).await.unwrap();

        event!(Level::INFO, "host backend bind successful: {:?}", bind_address);

        let state: State = Default::default();
        let state = Arc::new(RwLock::new(Arc::new(state)));

        loop {
            tokio::select! {
                _ = &mut term_sig => {
                    break;
                }
                control_req = ebus_incoming.recv() => {
                    let creq = match control_req {
                        Ok(v) => v,
                        Err(broadcast::RecvError::Closed) => {
                            break;
                        }
                        Err(broadcast::RecvError::Lagged(..)) => {
                            event!(Level::ERROR, "piece fetch loop lagged - we're dropping requests");
                            continue;
                        },
                    };
                    if let Some(ns) = creq.downcast_ref::<BusNewState>() {
                        let mut s = state.write().await;
                        *s = Arc::clone(&ns.state);
                    }
                    
                }
                res = listener.accept() => {
                    let (socket, addr) = match res {
                        Ok(v) => v,
                        Err(err) => {
                            event!(Level::ERROR, "listener error {:?}", err);
                            continue;
                        },
                    };

                    event!(Level::INFO, "got connection from {:?}", addr);
                    let piece_fetcher = piece_fetcher.clone();
                    let state = state.clone();
                    tokio::spawn(async move {
                        if let Err(err) = start_server(socket, piece_fetcher, state).await {
                            event!(Level::ERROR, "error: {}", err);
                        }
                    });
                }
            }
        }

        Ok(())
    }.boxed()
}

async fn start_server<S>(
    mut socket: TcpStream,
    engine: S,
    state: Arc<RwLock<Arc<State>>>,
) -> Result<(), MagnetiteError>
where
    S: PieceStorageEngineDumb,
{
    use std::collections::hash_map::HashMap;

    let mut wbuf = BytesMut::with_capacity(BUF_SIZE_RESPONSE);
    let mut rbuf = BytesMut::with_capacity(BUF_SIZE_REQUEST);

    loop {
        if socket.read_buf(&mut rbuf).await? == 0 {
            return Ok(());
        }

        let mut fast_cache: HashMap<(TorrentId, u32), Bytes> = Default::default();

        while let Some(req) = read_request_from_buffer(&mut rbuf)? {
            let req: MagnetiteWireRequest = req;

            match req.payload {
                MagnetiteWireRequestPayload::Piece(ref p) => {
                    let piece_offset = p.piece_offset as usize;
                    let fetch_length = p.fetch_length as usize;

                    let cache_key = (p.content_key, p.piece_index);

                    let piece = match fast_cache.get(&cache_key) {
                        Some(piece) => piece.clone(),
                        None => {
                            let s = state.read().await;

                            let content_info = s.get(&p.content_key).cloned().ok_or_else(|| {
                                event!(
                                    Level::ERROR,
                                    "failed to find content {}",
                                    p.content_key.hex()
                                );
                                ProtocolViolation
                            })?;

                            drop(s);

                            let piece_sha = content_info
                                .piece_shas
                                .get(p.piece_index as usize)
                                .ok_or_else(|| {
                                    event!(Level::ERROR, "protocol violation #1");
                                    ProtocolViolation
                                })?;

                            let req = GetPieceRequest {
                                content_key: p.content_key,
                                piece_sha: *piece_sha,
                                piece_length: content_info.piece_length,
                                total_length: content_info.total_length,
                                piece_index: p.piece_index,
                            };

                            event!(Level::DEBUG, "doing fetch for {:?}", req);

                            let piece_res = engine.get_piece_dumb(&req).await;

                            if let Err(ref err) = piece_res {
                                event!(Level::ERROR, "error fetching {:?}: {}", req, err);
                            };

                            let piece = piece_res?;

                            fast_cache.insert(cache_key, piece.clone());

                            piece
                        }
                    };

                    if piece.len() < piece_offset as usize {
                        event!(Level::ERROR, "protocol violation #2");
                        return Err(ProtocolViolation.into());
                    }

                    let piece_rest = &piece[piece_offset..];
                    if piece_rest.len() < fetch_length {
                        event!(Level::ERROR, "protocol violation #3");
                        return Err(ProtocolViolation.into());
                    }
                    let piece_rest = &piece_rest[..fetch_length];

                    let resp = MagnetiteWireResponse {
                        txid: req.txid,
                        payload: MagnetiteWireResponsePayload::Piece(MagnetiteWirePieceResponse {
                            data: piece_rest.to_vec(),
                        }),
                    };
                    write_response_to_buffer(&mut wbuf, &resp)?;

                    while !wbuf.is_empty() {
                        socket.write_buf(&mut wbuf).await?;
                    }
                }
            }
        }
    }
}

fn enqueue_request_partial(
    now: Instant,
    wbuf: &mut BytesMut,
    inflight: &mut Slab<RequestState>,
    payload: PendingRequest,
) -> Result<(), MagnetiteError> {
    let txid = inflight.insert(payload.make_request_state(now));
    let wire_req = payload.make_wire_request(txid);
    write_request_to_buffer(wbuf, &wire_req)?;
    Ok(())
}

impl PieceStorageEngineDumb for RemoteMagnetite {
    fn get_piece_dumb(
        &self,
        get_piece_req: &GetPieceRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>> {
        let mut self_cloned: Self = self.clone();
        let gp: GetPieceRequest = *get_piece_req;

        async move {
            let (responder, resp_rx) = oneshot::channel();

            if let Err(err) = self_cloned
                .sender
                .send(MagnetiteRequest::Piece { gp, responder })
                .await
            {
                event!(
                    Level::WARN,
                    "failed to complete request in time, rx dropped: {}",
                    err
                );
            }

            match resp_rx.await {
                Ok(Ok(v)) => {
                    counter!("remotemagnetite.upstream_fetched_bytes", v.len() as u64);
                    Ok(v)
                }
                Ok(Err(err)) => Err(err),
                Err(..) => Err(MagnetiteError::CompletionLost),
            }
        }
        .boxed()
    }
}

#[derive(PartialEq, Eq)]
pub enum MagnetiteRequestKind {
    Piece,
}

enum MagnetiteRequest {
    Piece {
        gp: GetPieceRequest,
        responder: oneshot::Sender<Result<Bytes, MagnetiteError>>,
    },
}
