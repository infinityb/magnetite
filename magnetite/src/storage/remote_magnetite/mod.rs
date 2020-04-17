use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use futures::future::FutureExt;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use slab::Slab;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task;
use tracing::{event, Level};

use crate::model::{MagnetiteError, TorrentID};
use crate::storage::{GetPieceRequest, PieceStorageEngine, PieceStorageEngineDumb};
use crate::utils::BytesCow;

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
const LATENCY_TARGET_MS: u32 = 300;
const CHUNK_SIZE: u32 = 32 * 1024; // 26ms @ 10Mbps

struct PiecePendingRequestFactory {
    piece_slab_index: usize,
    content_key: TorrentID,
    piece_index: u32,
    piece_length: u32,
    piece_requested_length: u32,
}

impl PiecePendingRequestFactory {
    pub fn new(piece_slab_index: usize, gp: &GetPieceRequest) -> Self {
        PiecePendingRequestFactory {
            piece_slab_index,
            content_key: gp.content_key,
            piece_index: gp.piece_index,
            piece_length: gp.piece_length,
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

        Some(PendingRequest::ChunkRequest(PendingChunkRequest {
            piece_slab_index: self.piece_slab_index,
            content_key: self.content_key,
            piece_index: self.piece_index,
            piece_offset: request_offset,
            fetch_length: remaining,
            cookie_data: cookie,
        }))
    }
}

impl PendingRequest {
    fn make_request_state(&self, now: &Instant) -> RequestState {
        let PendingRequest::ChunkRequest(ref p) = self;
        RequestState {
            piece_slab_index: p.piece_slab_index,
            req_submit_time: *now,
            req_kind: MagnetiteRequestKind::Piece,
            piece_index: p.piece_index,
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

enum PendingRequest {
    ChunkRequest(PendingChunkRequest),
}

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
    piece_index: u32,
    txid_cookie: u64,
    piece_offset: u32,
    fetch_length: u32,
}

struct PieceState {
    piece_length: u32,
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
        unimplemented!();
    }
}

#[derive(Clone)]
pub struct RemoteMagnetite {
    tcp: Arc<Mutex<TcpStream>>,
}

struct LatencyStats {
    last_latency_ms: u32,
    // acc: u32,
    // history: VecDeque<u32>,
}

impl LatencyStats {
    pub fn add(&mut self, d: Duration) {
        self.last_latency_ms = d.as_millis() as u32;
        // d.as_micro
        // unimplemented!();
    }

    fn average(&self) -> u32 {
        self.last_latency_ms
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

    let request_state = inflight
        .get(txid_value)
        .ok_or(MagnetiteError::ProtocolViolation)?;
    if request_state.txid_cookie != txid_cookie {
        return Err(MagnetiteError::ProtocolViolation);
    }
    if request_state.req_kind != resp.kind() {
        return Err(MagnetiteError::ProtocolViolation);
    }

    drop(request_state);
    let request_state = inflight.remove(txid_value);

    latency_stats.add(request_state.req_submit_time.elapsed());

    let piece_state = pieces.get_mut(request_state.piece_slab_index).unwrap();
    if piece_state.merge_response(&request_state, &resp)? {
        let PieceState {
            data, responder, ..
        } = pieces.remove(request_state.piece_slab_index);
        responder.send(Ok(data.freeze()));
    }

    Ok(())
}

async fn run_connected(
    mut socket: TcpStream,
    incoming_request_queue: mpsc::Receiver<MagnetiteRequest>,
) -> Result<(), MagnetiteError> {
    let mut current_window_size: usize = WINDOW_SIZE_START;
    let mut inflight: Slab<RequestState> = Default::default();
    // this shouldn't really exceed one or two elements, I think.
    let mut piece_state: Slab<PieceState> = Default::default();

    let mut wbuf = BytesMut::with_capacity(BUF_SIZE_REQUEST);
    let mut rbuf = BytesMut::with_capacity(BUF_SIZE_RESPONSE);
    let mut latency = LatencyStats { last_latency_ms: 0 };
    // get a piece request, convert it into chunk requests,
    // send remaining_window_size chunk requests, wait until a chunk returns
    // before sending one more.  Between sends, we can check if the piece future
    // is cancelled.
    //
    let mut split_request_queue: VecDeque<PendingRequest> = VecDeque::new();
    let mut request_queue_open = true;

    loop {
        // recompute window size.
        unimplemented!();

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
                    let ps = PieceState::new(gp.piece_length, responder);
                    let pf = PiecePendingRequestFactory::new(piece_state.insert(ps), &gp);
                    split_request_queue.extend(pf);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Closed) => {
                    request_queue_open = false;
                }
            };
        }

        for i in 0..take_count {
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

        match (!inflight.is_empty(), request_queue_open) {
            (false, false) => {
                // nothing to read, nothing to queue, and can't get more work.  We've exhausted
                // all of the work we'll ever get.  Iteration stops.
                if split_request_queue.len() == 0 {
                    return Ok(());
                }
            }
            (false, true) => {
                // nothing to read (we don't have any requests out), can get more work.
                if let Some(v) = incoming_request_queue.recv().await {
                    let MagnetiteRequest::Piece { gp, responder } = v;
                    let ps = PieceState::new(gp.piece_length, responder);
                    let pf = PiecePendingRequestFactory::new(piece_state.insert(ps), &gp);
                    split_request_queue.extend(pf);
                } else {
                    request_queue_open = false;
                }
            }
            (true, false) => {
                // something to read, can't get new work.
                socket.read_buf(&mut rbuf).await?;
                while let Some(resp) = read_response_from_buffer(&mut rbuf)? {
                    resolve_response(resp, &mut inflight, &mut piece_state, &mut latency)?;
                }
            }
            (true, true) => {
                ::tokio::select! {
                    req = incoming_request_queue.recv() => {
                        if let Some(v) = req {
                            let MagnetiteRequest::Piece { gp, responder } = v;
                            let ps = PieceState::new(gp.piece_length, responder);
                            let pf = PiecePendingRequestFactory::new(piece_state.insert(ps), &gp);
                            split_request_queue.extend(pf);
                        } else {
                            request_queue_open = false;
                        }
                    }
                    read_res = socket.read_buf(&mut rbuf) => {
                        read_res?;
                        while let Some(resp) = read_response_from_buffer(&mut rbuf)? {
                            resolve_response(resp, &mut inflight, &mut piece_state, &mut latency)?;
                        }
                    }
                }
            }
        }
    }
}

fn start_worker(mut socket: TcpStream, incoming_request_queue: mpsc::Receiver<MagnetiteRequest>) {
    task::spawn(run_connected(socket, incoming_request_queue));
}


// #[derive(Debug, PartialEq, Eq)]
// pub struct MagnetiteWireRequest {
//     pub txid: u64,
//     pub payload: MagnetiteWireRequestPayload,
// }

// #[derive(Debug, PartialEq, Eq)]
// pub enum MagnetiteWireRequestPayload {
//     Piece(MagnetiteWirePieceRequest),
// }

// #[derive(Debug, PartialEq, Eq)]
// pub struct MagnetiteWirePieceRequest {
//     pub content_key: TorrentID,
//     pub piece_index: u32,
//     pub piece_offset: u32,
//     pub fetch_length: u32,
// }

// #[derive(Debug, PartialEq, Eq)]
// pub struct MagnetiteWireResponse {
//     pub txid: u64,
//     pub payload: MagnetiteWireResponsePayload,
// }

// #[derive(Debug, PartialEq, Eq)]
// pub enum MagnetiteWireResponsePayload {
//     Piece(MagnetiteWirePieceResponse),
// }

// #[derive(Debug, PartialEq, Eq)]
// pub struct MagnetiteWirePieceResponse {
//     pub data: Vec<u8>,
// }

// impl MagnetiteWireRequestPayload {
//     pub fn kind(&self) -> MagnetiteRequestKind {
//         match self {
//             MagnetiteWireRequestPayload::Piece(..) => MagnetiteRequestKind::Piece,
//         }
//     }
// }

// impl MagnetiteWireResponse {
//     pub fn kind(&self) -> MagnetiteRequestKind {
//         match self.payload {
//             MagnetiteWireResponsePayload::Piece(..) => MagnetiteRequestKind::Piece,
//         }
//     }
// }

pub async fn start_server<S>(mut socket: TcpStream, engine: S) -> Result<(), MagnetiteError>
    where S: PieceStorageEngine
{    
    let mut wbuf = BytesMut::with_capacity(BUF_SIZE_RESPONSE);
    let mut rbuf = BytesMut::with_capacity(BUF_SIZE_REQUEST);

    loop {
        socket.read_buf(&mut rbuf).await?;

        while let Some(req) = read_request_from_buffer(&mut rbuf)? {
            let req: MagnetiteWireRequest = req;
            
            match req.payload {
                MagnetiteWireRequestPayload::Piece(ref p) => {
                    let piece_offset = p.piece_offset;
                    let fetch_length = p.fetch_length;

                    let piece = engine.get_piece(&p.content_key, p.piece_index).await?;
                    if piece.len() < piece_offset as usize {
                        return Err(MagnetiteError::ProtocolViolation);
                    }

                    let piece_rest = &piece[piece_offset as usize..];
                    if piece_rest.len() < fetch_length as usize {
                        return Err(MagnetiteError::ProtocolViolation);
                    }

                    let piece_rest = &piece_rest[..fetch_length as usize];
                    
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
        let self_cloned: Self = self.clone();

        async move {
            let mut tcp = self_cloned.tcp.lock().await;

            //
            unreachable!();
        }
        .boxed()
    }
}

#[derive(PartialEq, Eq)]
enum MagnetiteRequestKind {
    Piece,
}

enum MagnetiteRequest {
    Piece {
        gp: GetPieceRequest,
        responder: oneshot::Sender<Result<Bytes, MagnetiteError>>,
    },
}
