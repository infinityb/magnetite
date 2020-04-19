use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use futures::future::FutureExt;
use rand::{thread_rng, Rng};
use slab::Slab;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};
use tokio::task;
use tracing::{event, Level};

use crate::model::{MagnetiteError, ProtocolViolation, TorrentID};
use crate::storage::{
    utils as storage_utils, GetPieceRequest, PieceStorageEngine, PieceStorageEngineDumb,
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
const LATENCY_TARGET_MAX_MILLISECONDS: u32 = 800;
const CHUNK_SIZE: u32 = 32 * 1024; // 26ms @ 10Mbps

#[test]
fn xx() {
    let gp = GetPieceRequest {
        content_key: TorrentID::zero(),
        piece_sha: TorrentID::zero(),
        piece_length: 16777216,
        total_length: 287396254600,
        piece_index: 17130,
    };

    for (idx, x) in PiecePendingRequestFactory::new(0, &gp).enumerate() {
        println!("[{}] = {:?}", idx, x);
    }
}

#[derive(Debug)]
struct PiecePendingRequestFactory {
    piece_slab_index: usize,
    content_key: TorrentID,
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
            piece_length: piece_length,
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
    fn make_request_state(&self, now: &Instant) -> RequestState {
        let PendingRequest::ChunkRequest(ref p) = self;
        RequestState {
            piece_slab_index: p.piece_slab_index,
            req_submit_time: *now,
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
            txid: txid,
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
    content_key: TorrentID,
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
            piece_length: piece_length,
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

    drop(request_state);
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

async fn run_connected(
    mut socket: TcpStream,
    incoming_request_queue: &mut mpsc::Receiver<MagnetiteRequest>,
) -> Result<bool, MagnetiteError> {
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

    loop {
        if let Some(average) = latency.average() {
            if average < LATENCY_TARGET_MIN_MILLISECONDS {
                let prev_window_size = current_window_size;
                current_window_size += 1;
                if WINDOW_SIZE_HARD_LIMIT < current_window_size {
                    current_window_size = WINDOW_SIZE_HARD_LIMIT;
                }
                if prev_window_size != current_window_size {
                    event!(
                        Level::INFO,
                        "increased window size to {}",
                        current_window_size
                    );
                }
            }
            if LATENCY_TARGET_MAX_MILLISECONDS < average {
                let prev_window_size = current_window_size;
                current_window_size = current_window_size / 2;
                if current_window_size < 2 {
                    current_window_size = 2;
                }
                if prev_window_size != current_window_size {
                    event!(Level::INFO, "halved window size to {}", current_window_size);
                }
            }
        }

        let take_count;
        if inflight.len() < current_window_size {
            take_count = current_window_size - inflight.len();
        } else {
            take_count = 0;
        }

        // opportunistically fill split_request_queue
        while split_request_queue.len() < take_count && request_queue_open {
            match incoming_request_queue.try_recv() {
                Ok(MagnetiteRequest::Piece { gp, responder }) => {
                    let (_, fetch_length) = storage_utils::compute_offset_length(
                        gp.piece_index,
                        gp.piece_length,
                        gp.total_length,
                    );

                    let ps = PieceState::new(fetch_length, responder);
                    let pf = PiecePendingRequestFactory::new(piece_state.insert(ps), &gp);
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
                enqueue_request_partial(&now, &mut wbuf, &mut inflight, ri)?;
            } else {
                break;
            }
        }

        let to_write = wbuf.split().freeze();
        if to_write.len() > 0 {
            socket.write_all(&to_write[..]).await?;
        }
        drop(to_write);

        // now: either slab is full or split_request_queue is empty.  If slab is full, we shouldn't
        // take any more requests, and we must complete a response read.
        while inflight.len() == current_window_size {
            socket.read_buf(&mut rbuf).await?;
            while let Some(resp) = read_response_from_buffer(&mut rbuf)? {
                resolve_response(resp, &mut inflight, &mut piece_state, &mut latency)?;
            }
        }

        if !request_queue_open && inflight.is_empty() {
            if split_request_queue.is_empty() {
                break;
            }
        }

        // event!(Level::ERROR, "request_queue_open={:?} inflight.len()={:?} split_request_queue.len()={:?}",
        //     request_queue_open,
        //     inflight.len(),
        //     split_request_queue.len());

        if request_queue_open || !inflight.is_empty() {
            ::tokio::select! {
                req = incoming_request_queue.recv(), if request_queue_open => {
                    if let Some(v) = req {
                        let MagnetiteRequest::Piece { gp, responder } = v;
                        let (_, fetch_length) = storage_utils::compute_offset_length(
                        gp.piece_index, gp.piece_length, gp.total_length);

                        let ps = PieceState::new(fetch_length, responder);
                        let pf = PiecePendingRequestFactory::new(piece_state.insert(ps), &gp);
                        split_request_queue.extend(pf);
                    } else {
                        request_queue_open = false;
                    }
                },
                read_res = socket.read_buf(&mut rbuf), if !inflight.is_empty() => {
                    read_res?;
                    while let Some(resp) = read_response_from_buffer(&mut rbuf)? {
                        resolve_response(resp, &mut inflight, &mut piece_state, &mut latency)?;
                    }
                }
            }
        }

        // match (!inflight.is_empty(), request_queue_open) {
        //     (false, false) => {
        //         // nothing to read, nothing to queue, and can't get more work.  We've exhausted
        //         // all of the work we'll ever get.  Iteration stops.
        //         if split_request_queue.len() == 0 {
        //             return Ok(());
        //         }
        //     }
        //     (false, true) => {
        //         // nothing to read (we don't have any requests out), can get more work.
        //         if let Some(v) = incoming_request_queue.recv().await {
        //             let MagnetiteRequest::Piece { gp, responder } = v;
        //             let ps = PieceState::new(gp.piece_length, responder);
        //             let pf = PiecePendingRequestFactory::new(piece_state.insert(ps), &gp);
        //             split_request_queue.extend(pf);
        //         } else {
        //             request_queue_open = false;
        //         }
        //     }
        //     (true, false) => {
        //         // something to read, can't get new work.
        //         socket.read_buf(&mut rbuf).await?;
        //         while let Some(resp) = read_response_from_buffer(&mut rbuf)? {
        //             resolve_response(resp, &mut inflight, &mut piece_state, &mut latency)?;
        //         }
        //     }
        //    (true, true) => {
        //    }
        // }
    }

    Ok(request_queue_open)
}

impl RemoteMagnetite {
    pub fn connected(addr: &str) -> RemoteMagnetite {
        let addr: String = addr.to_string();
        let (sender, mut incoming_request_queue) = mpsc::channel(16);

        task::spawn(async move {
            const FAILURE_DELAY_MILLISECONDS_MAX: u64 = 60_000;
            const FAILURE_DELAY_MILLISECONDS_START: u64 = 100;
            let mut failure_delay_milliseconds: u64 = FAILURE_DELAY_MILLISECONDS_START;

            loop {
                let socket = match TcpStream::connect(&addr).await {
                    Ok(v) => v,
                    Err(err) => {
                        event!(Level::ERROR, "upstream connection failure: {}", err);

                        failure_delay_milliseconds += failure_delay_milliseconds;
                        if FAILURE_DELAY_MILLISECONDS_MAX < failure_delay_milliseconds {
                            failure_delay_milliseconds = FAILURE_DELAY_MILLISECONDS_MAX;
                        }

                        tokio::time::delay_for(tokio::time::Duration::from_millis(
                            failure_delay_milliseconds,
                        ))
                        .await;

                        continue;
                    }
                };

                failure_delay_milliseconds = FAILURE_DELAY_MILLISECONDS_START;

                match run_connected(socket, &mut incoming_request_queue).await {
                    Ok(false) => break,
                    Ok(true) => (),
                    Err(err) => {
                        event!(Level::ERROR, "upstream connection aborted: {}", err);
                        continue;
                    }
                }
            }
        });

        RemoteMagnetite { sender }
    }
}

pub async fn start_server<S>(mut socket: TcpStream, engine: S) -> Result<(), MagnetiteError>
where
    S: PieceStorageEngine,
{
    use std::collections::hash_map::HashMap;

    let mut wbuf = BytesMut::with_capacity(BUF_SIZE_RESPONSE);
    let mut rbuf = BytesMut::with_capacity(BUF_SIZE_REQUEST);

    loop {
        socket.read_buf(&mut rbuf).await?;

        let mut fast_cache: HashMap<(TorrentID, u32), Bytes> = Default::default();

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
                            let piece = engine.get_piece(&p.content_key, p.piece_index).await?;

                            fast_cache.insert(cache_key, piece.clone());

                            piece
                        }
                    };

                    if piece.len() < piece_offset as usize {
                        return Err(ProtocolViolation.into());
                    }

                    let piece_rest = &piece[piece_offset..];
                    if piece_rest.len() < fetch_length {
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

                    while wbuf.len() > 0 {
                        socket.write_buf(&mut wbuf).await?;
                    }
                }
            }
        }
    }
}

fn enqueue_request_partial(
    now: &Instant,
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
        let gp: GetPieceRequest = get_piece_req.clone();

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
                Ok(v) => v,
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
