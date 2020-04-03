xuse std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::collections::HashMap;
use std::task::Poll;


use tokio::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc, oneshot};
use futures::future::Future;
use futures::stream::{self, Stream, StreamExt};
use slab::Slab;
use tracing::{event, Level};
use bytes::BytesMut;

use crate::model::TorrentID;

const TXID_RANDOM_MASK: u32 = 0xFFFF_FF00;

const TRACKER_MTU: i32 = 1280;
const ACTION_CONNECT: i32 = 0;
const ACTION_ANNOUNCE: i32 = 1;
const ACTION_SCRAPE: i32 = 2;
const ACTION_ERROR: i32 = 3;

#[derive(Debug)]
pub struct Expired;

impl fmt::Display for Expired {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Expired")
    }
}

impl std::error::Error for Expired {}

// --

struct TrackerConnectionState {
    cached_socket_addr: Option<SocketAddr>,
    // if we don't have a connection_id, we will store pending requests here.
    pending_requests: Vec<Request>,
    next_allowed_send: Instant,
    last_message_sent: Instant,
    last_successful_response: Instant,
    unresponded_messages: i32,
    connection_id: i32,
}

struct Trackers {
    socket: tokio::net::UdpSocket,
    lockables: Arc<TrackersLockables>,
}

struct TrackerManager {
    connection_ids: HashMap<(TorrentID, String), Arc<Mutex<TrackerConnectionState>>>,
    buf: BytesMut,
    outstanding: Slab<RequestState>,
}

pub struct Config {
    bind_address: SocketAddr,
}

pub async fn start_tracker_subsystem(config: Config) -> impl Future<Output=Result<(), failure::Error>> {
    use tokio::net::UdpSocket;
    use tokio::time::timeout;
    use futures::stream::SelectAll;
    
    let mut connection_stats: HashMap<(TorrentID, String), Arc<Mutex<TrackerConnectionState>>> = Default::default();
    
    let (mut response_queue_tx, mut response_queue_rx) = tokio::sync::mpsc::channel(10);
    let socket = UdpSocket::bind(&config.bind_address).await?;

    let read_half = Arc::new(socket);
    let write_half = read_half.clone();

    enum Work {
        Request {
            request: Request,
        },
        Response {
            value: Response,
            source_addr: SocketAddr,
        },
        Expiration {
            txid: u32,
            generation: u64,
        },
        Terminating {
            sensible: bool,
        },
    }
    
    let response_queue_rx = futures::stream::poll_fn(move |_| -> Poll<Option<Work>> {
        let mut msg_buf = [0; 4 * 1024];
        loop {    
            let (length, source_addr) = match read_half.poll_recv_from(&mut msg_buf[..]) {
                Poll::Ready(Ok(tup)) => tup,
                Poll::Ready(Err(err)) => {
                    event!(Level::ERROR, "error rxing on tracker socket: {}", err);
                    return Poll::Ready(None);
                },
                Poll::NotReady => return Poll::NotReady,
            };
            match parse_response(&msg_buf[..length]) {
                Ok(value) => Poll::Ready(Some(Work::Response {
                    value,
                    source_addr,
                })),
                Err(..) => {
                    // maybe log invalid packets later?
                    continue;
                }
            };
        }
    });

    // work task
    async {
        // this can probably use a smaller, Copy key, and then we can store
        // that key in the pending request, if we need it.  I think we don't though.
        let mut connection_ids: HashMap<(TorrentID, String), TrackerConnectionState> = Default::default();
        let mut outstanding: Slab<RequestState> = Slab::with_capacity(256);
        let mut incoming: SelectAll<Box<Stream<Item=Work>>> = Default::default();

        incoming.push(
            response_queue_rx.
                chain(stream::once(async {
                    Work::Terminating { sensible: false }
                })).boxed());

        incoming.push(
            request_queue_rx.
                chain(stream::once(async {
                    Work::Terminating { sensible: true }
                })).boxed());

        let mut msg_buf = [0; 4096];
        while let Some(msg) = incoming.next().await {
            match msg {
                Work::Request { request } => {
                    if outstanding.len() == 256 {
                        // send saturation response or queue.
                    }

                    let entry = outstanding.vacant_entry();
                    let txid_random = thread_rng().gen();
                    let txid = entry.key() as u32 | (txid_random & TXID_RANDOM_MASK);

                    incoming.push(stream::once(async {
                        tokio::time::delay_for(Duration::new(15, 0)).await;
                        Work::Expiration { txid, generation }
                    }).boxed());

                    // write_request out 

                    entry.insert(RequestState {
                        send_time: Instant::now(),
                        transaction_id: txid,
                        value: req,
                    });

                    //
                },
                Work::Response { value, source_addr } => {
                    if let Some(req) = outstanding.get(value.txid & ~TXID_RANDOM_MASK) {
                        if req.txid != value.txid {
                            // txid_index is active but doesn't match full
                            // txid - discard.
                            continue;
                        }
                        if req.source_addr != source_addr {
                            continue;
                        }

                        drop(req);

                        // update tracker liveness

                        let state = outstanding.remove(txid);
                        state.resolver.send(Ok(Response {
                            //
                        }));
                    }
                },
                Work::Expiration { txid, generation } => {
                    if let Some(req) = outstanding.get(value.txid & ~TXID_RANDOM_MASK) {
                        if req.txid != value.txid {
                            // txid_index is active but doesn't match full
                            // txid - discard.
                            continue;
                        }

                        const MAX_FAILURE_COUNT: i8 = 3;

                        if req.failure_count >= MAX_FAILURE_COUNT {
                            drop(req);

                            let state = outstanding.remove(txid);
                            state.resolver.send(Err(Expired.into()));
                        } else {
                            req.failure_count += 1;
                            req.send_time = Instant::now();

                            // write_request out again;
                        }
                    }
                },
                Work::Terminating { sensible } => {
                    if !sensible {
                        event!(Level::ERROR, "socket closed without shutdown request");
                    }
                    break;
                }
            }
        }
    }
}


enum InternalRequest {
    Connect,
    Announce(AnnounceRequest),
    Scrape(Vec<TorrentID>),
}

enum InternalResponse {
    Connect {
        connection_id: i64,
    },
    Announce {
        info_hash: TorrentID,
        req_interval_duration_seconds: i32,
        leechers: i32,
        seeders: i32,
        peers: Vec<AnnouncePeer>,
    },
    Scrape {
        results: Vec<(TorrentID, ScrapeItem)>,
    },
    Error(failure::Error),
}

trait Request {
    type Response: TryFrom<InternalResponse, Error=failure::Error>;

    type Fut: Future<Output=Result<Self::Response, failure::Error>>;

    //
}

// --

impl Request for ConnectRequest {
    type Response = ConnectResponse;
}

impl TryFrom<InternalResponse> for ConnectResponse {
    type Error = failure::Error;

    fn try_from(value: InternalResponse) -> Result<Self, Self::Error> {
        match value {
            InternalResponse::Connect { connection_id } => {
                unimplemented!();
            },
            InternalResponse::Error(err) => return Err(err),
            _ => return Err(failure::format_err("internal error {}:{}", file!(), line!()));
        }
    }
}

// -- 

impl Request for AnnounceRequest {
    type Response = AnnounceResponse;
}

impl TryFrom<InternalResponse> for AnnounceResponse {
    type Error = failure::Error;

    fn try_from(value: InternalResponse) -> Result<Self, Self::Error> {
        match value {
            // results: Vec<(TorrentID, ScrapeItem)>,
            InternalResponse::Scrape { results } => {
                unimplemented!();
            },
            InternalResponse::Error(err) => return Err(err),
            _ => return Err(failure::format_err("internal error {}:{}", file!(), line!()));
        }
    }
}

// --

impl Request for ScrapeRequest {
    type Response = ScrapeResponse;
}

impl TryFrom<InternalResponse> for ConnectResponse {
    type Error = failure::Error;

    fn try_from(value: InternalResponse) -> Result<Self, Self::Error> {
        match value {
            // results: Vec<(TorrentID, ScrapeItem)>,
            InternalResponse::Announce {
                info_hash: TorrentID,
                req_interval_duration_seconds: i32,
                leechers: i32,
                seeders: i32,
                peers: Vec<AnnouncePeer>,
            } => {
                unimplemented!();
            },
            InternalResponse::Error(err) => return Err(err),
            _ => return Err(failure::format_err("internal error {}:{}", file!(), line!()));
        }
    }
}

// --

pub struct RequestState {
    response_source_addr: SocketAddr,
    failure_count: i8,
    send_time: Instant,
    value: Request,
    resolver: oneshot::Sender<InternalResponse>,
}

pub enum RequestContextValue {
    Connect,
    Announce(TorrentID),
    Scrape(Vec<TorrentID>),
}

pub struct PendingResponse {
    request: Request,
}

pub struct ConnectRequest {
    pub prefix: RequestPrefix,
}

pub struct ConnectResponse {
    pub prefix: ResponsePrefix,
    pub connection_id: i64,
}

pub struct AnnounceRequest {
    pub prefix: RequestPrefix,
    pub info_hash: TorrentID,
    pub peer_id: TorrentID,
    pub downloaded: i64,
    pub left: i64,
    pub uploaded: i64,
    pub event: i32,
    pub ip: u32,
    pub key: u32,
    pub num_want: i32,
    pub port: u16,
    pub extensions: u16,
}

pub struct AnnounceResponse {
    pub prefix: ResponsePrefix,
    pub req_interval_duration_seconds: i32,
    pub leechers: i32,
    pub seeders: i32,
    pub peers: Vec<AnnouncePeer>,
}

pub struct AnnouncePeer {
    pub ip: u32,
    pub port: u16,
}

pub struct ScrapeRequest {
    pub prefix: RequestPrefix,
    pub info_hash_list: Vec<TorrentID>,
}

pub struct ScrapeResponse {
    pub prefix: ResponsePrefix,
    pub scrape_items: Vec<ScrapeItem>,
}

pub struct ScrapeItem {
    pub complete: i32,
    pub downloaded: i32,
    pub incomplete: i32,
}

pub struct Error {
    pub prefix: ResponsePrefix,
    pub error: Vec<u8>,
}

pub struct RequestPrefix {
    pub connection_id: i64,
    pub action: i32,
    pub transaction_id: u32,
}

pub struct ResponsePrefix {
    pub action: i32,
    pub transaction_id: u32,
}

