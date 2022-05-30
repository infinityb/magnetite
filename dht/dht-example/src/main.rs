use std::collections::{HashSet, VecDeque};
use std::hash::{BuildHasher, Hash, Hasher};
use std::io;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{App, Arg};
use futures::channel::oneshot;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use smallvec::SmallVec;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tracing::{event, Level};
use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::FmtSubscriber;

use dht::tracker::{AnnounceCtx, TrackerSearch};
use dht::wire::{
    DhtErrorResponse, DhtMessage, DhtMessageData, DhtMessageQuery, DhtMessageQueryAnnouncePeer,
    DhtMessageQueryFindNode, DhtMessageQueryGetPeers,
    DhtMessageResponse, DhtMessageResponseData,
};
use dht::{
    ThinNode,
    BucketManager, NodeEnvironment, TransactionCompletion, BUCKET_SIZE,
};
use magnetite_common::TorrentId;

fn handle_query_ping(bm: &BucketManager, message: &DhtMessage) -> DhtMessage {
    DhtMessage {
        transaction: message.transaction.clone(),
        data: DhtMessageData::Response(DhtMessageResponse {
            data: DhtMessageResponseData {
                id: bm.self_peer_id,
                nodes: Default::default(),
                nodes6: Default::default(),
                values: vec![],
                token: vec![],
            },
        }),
    }
}

fn handle_query_find_node(
    bm: &BucketManager,
    message: &DhtMessage,
    m_qfn: &DhtMessageQueryFindNode,
    environment: &NodeEnvironment,
) -> DhtMessage {
    let closest = bm.find_close_nodes(&m_qfn.target, BUCKET_SIZE, environment);
    let want_v4 = true;
    let want_v6 = false;

    let mut nodes: Vec<(TorrentId, SocketAddrV4)> = Vec::new();
    let mut nodes6: Vec<SocketAddrV6> = Vec::new();
    for node in closest {
        match node.peer_addr {
            SocketAddr::V4(v4) => {
                if want_v4 {
                    nodes.push((node.peer_id, v4))
                }
            }
            SocketAddr::V6(v6) => {
                if want_v6 {
                    nodes6.push(v6)
                }
            }
        }
    }

    DhtMessage {
        transaction: message.transaction.clone(),
        data: DhtMessageData::Response(DhtMessageResponse {
            data: DhtMessageResponseData {
                id: bm.self_peer_id,
                nodes,
                nodes6,
                values: vec![],
                token: vec![],
            },
        }),
    }
}

fn handle_query_get_peers(
    bm: &BucketManager,
    message: &DhtMessage,
    m_gp: &DhtMessageQueryGetPeers,
    environment: &NodeEnvironment,
    client_addr: &SocketAddr,
) -> DhtMessage {
    let want_v4 = true;
    let want_v6 = false;

    let mut peers = SmallVec::<[&SocketAddr; 32]>::new();
    let mut nodes: Vec<(TorrentId, SocketAddrV4)> = Vec::new();
    let mut nodes6: Vec<SocketAddrV6> = Vec::new();

    let mut hasher = bm.get_peers_search.build_hasher();
    Hash::hash(client_addr, &mut hasher);

    bm.tracker.search_announce(
        &TrackerSearch {
            now: environment.now,
            info_hash: m_gp.info_hash,
            cookie: hasher.finish(),
        },
        &mut peers,
    );

    if peers.is_empty() {
        for node in bm.find_close_nodes(&m_gp.info_hash, BUCKET_SIZE, &environment) {
            match node.peer_addr {
                SocketAddr::V4(v4) => {
                    if want_v4 {
                        nodes.push((node.peer_id, v4));
                    }
                }
                SocketAddr::V6(v6) => {
                    if want_v6 {
                        nodes6.push(v6)
                    }
                }
            }
        }
    }

    let token = bm.generate_token(client_addr);
    DhtMessage {
        transaction: message.transaction.clone(),
        data: DhtMessageData::Response(DhtMessageResponse {
            data: DhtMessageResponseData {
                id: bm.self_peer_id,
                token,
                nodes,
                nodes6,
                values: peers.into_iter().cloned().collect(),
            },
        }),
    }
}

fn handle_query_announce_peer(
    bm: &mut BucketManager,
    message: &DhtMessage,
    m_ap: &DhtMessageQueryAnnouncePeer,
    environment: &NodeEnvironment,
    client_addr: &SocketAddr,
) -> DhtMessage {
    if !bm.check_token(&m_ap.token, client_addr) {
        return DhtMessage {
            transaction: message.transaction.clone(),
            data: DhtMessageData::Error(DhtErrorResponse {
                error: (203, "Bad token".to_string()),
            }),
        };
    }

    bm.tracker.insert_announce(
        &m_ap.info_hash,
        client_addr,
        &AnnounceCtx {
            now: environment.now,
        },
    );

    DhtMessage {
        transaction: message.transaction.clone(),
        data: DhtMessageData::Response(DhtMessageResponse {
            data: DhtMessageResponseData {
                id: bm.self_peer_id,
                nodes: Default::default(),
                nodes6: Default::default(),
                token: vec![],
                values: vec![],
            },
        }),
    }
}

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_NAME: &str = env!("CARGO_PKG_NAME");

fn dht_query_apply_txid(
    bm: &mut BucketManager,
    bma: &Arc<Mutex<BucketManager>>,
    message: &mut DhtMessage,
    to: SocketAddr,
    now: &Instant,
) -> oneshot::Receiver<Box<TransactionCompletion>> {
    let txslot = bm.acquire_transaction_slot();
    let txid = txslot.key();

    let future = txslot.assign(message, to, now);
    drop(bm);

    let bm_tmp = Arc::clone(&bma);
    tokio::spawn(async move {
        let bm = bm_tmp;

        tokio::time::sleep(Duration::new(3, 0)).await;
        let mut bm_locked = bm.lock().await;
        bm_locked.clean_expired_transaction(txid);
        drop(bm_locked);
    });

    future
}

#[derive(Clone)]
struct HyperHandler {
    bm: Arc<Mutex<BucketManager>>,
    so: Arc<UdpSocket>,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut my_subscriber_builder =
        FmtSubscriber::builder().with_span_events(FmtSpan::FULL | FmtSpan::NEW | FmtSpan::CLOSE);

    let app = App::new(CARGO_PKG_NAME)
        .version(CARGO_PKG_VERSION)
        .author("Stacey Ell <software@e.staceyell.com>")
        .arg(
            Arg::with_name("bind-address")
                .long("bind-address")
                .help("Address to bind the server to")
                .default_value("[::1]:3000"),
        )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        );

    let matches = app.get_matches();
    let verbosity = matches.occurrences_of("v");
    let should_print_test_logging = 4 < verbosity;

    my_subscriber_builder = my_subscriber_builder.with_max_level(match verbosity {
        0 => TracingLevelFilter::ERROR,
        1 => TracingLevelFilter::WARN,
        2 => TracingLevelFilter::INFO,
        3 => TracingLevelFilter::DEBUG,
        _ => TracingLevelFilter::TRACE,
    });

    tracing::subscriber::set_global_default(my_subscriber_builder.finish())
        .expect("setting tracing default failed");

    if should_print_test_logging {
        print_test_logging();
    }

    let bind_address: SocketAddr = matches
        .value_of("bind-address")
        .expect("bind-address must be known")
        .to_socket_addrs()
        .expect("invalid bind-address arg")
        .next()
        .expect("invalid bind-address arg: didn't resolve to anything");

    let sock = Arc::new(UdpSocket::bind(&bind_address).await?);
    let mut starter_tasks: Vec<tokio::task::JoinHandle<Result<(), failure::Error>>> = Vec::new();
    let self_id = TorrentId(*b"\xe8E)\xd0j\xc4V\x9e\x03Yu[t\xbb\x85\x12{Z\xc4o");
    let bm = Arc::new(Mutex::new(BucketManager::new(self_id)));

    let bm_tmp = Arc::clone(&bm);
    let sock_tmp = Arc::clone(&sock);
    starter_tasks.push(tokio::spawn(async move {
        let sock = sock_tmp;
        let bm = bm_tmp;
        let mut buf = [0; 1400];
        loop {
            event!(Level::TRACE, "iteration, waiting on rx");
            let (len, addr) = sock.recv_from(&mut buf).await?;
            let rx_bytes = &buf[..len];
            event!(
                Level::TRACE,
                "[from {}] got message bytes: {:?}",
                addr,
                &BinStr(rx_bytes)
            );

            let decoded;
            match bencode::from_bytes::<DhtMessage>(rx_bytes) {
                Ok(v) => decoded = v,
                Err(err) => {
                    event!(Level::TRACE, "    \x1b[31mdecoded(fail)\x1b[0m: {}", err);
                    continue;
                }
            }

            let mut bm_locked = bm.lock().await;
            let is_reply = bm_locked.handle_incoming_packet(&decoded, addr);
            let environment = NodeEnvironment {
                now: Instant::now(),
                is_reply,
            };

            let mut peer_id = None;
            let mut response = None;
            match decoded.data {
                DhtMessageData::Query(DhtMessageQuery::Ping { id }) => {
                    peer_id = Some(id);
                    response = Some(handle_query_ping(&bm_locked, &decoded));
                }
                DhtMessageData::Query(DhtMessageQuery::FindNode(ref find_node)) => {
                    peer_id = Some(find_node.id);
                    response = Some(handle_query_find_node(
                        &bm_locked,
                        &decoded,
                        find_node,
                        &environment,
                    ));
                }
                DhtMessageData::Query(DhtMessageQuery::GetPeers(ref gp)) => {
                    peer_id = Some(gp.id);
                    response = Some(handle_query_get_peers(
                        &bm_locked,
                        &decoded,
                        gp,
                        &environment,
                        &addr,
                    ));
                }
                DhtMessageData::Query(DhtMessageQuery::AnnouncePeer(ref ap)) => {
                    peer_id = Some(ap.id);
                    response = Some(handle_query_announce_peer(
                        &mut bm_locked,
                        &decoded,
                        ap,
                        &environment,
                        &addr,
                    ));
                }
                DhtMessageData::Query(DhtMessageQuery::SampleInfohashes(ref si)) => {
                    peer_id = Some(si.id);
                }
                DhtMessageData::Query(DhtMessageQuery::Vote(ref vote)) => {
                    peer_id = Some(vote.id);
                }
                DhtMessageData::Response(response) => {
                    peer_id = Some(response.data.id);
                }
                _ => (),
            }

            if let Some(pid) = peer_id {
                bm_locked.node_seen(&ThinNode { id: pid, saddr: addr, }, &environment);
            }

            drop(bm_locked);

            if let Some(ref resp) = response {
                event!(Level::TRACE, "    response: {:?}", resp);
                let resp_bytes = bencode::to_bytes(resp).unwrap();
                event!(Level::TRACE, "    as-bytes: {:?}", BinStr(&resp_bytes));
                sock.send_to(&resp_bytes, addr).await?;
                event!(Level::TRACE, "    sent.");
            }
        }
    }));

    let bm_tmp = Arc::clone(&bm);
    let sock_tmp = Arc::clone(&sock);
    starter_tasks.push(tokio::spawn(async move {
        let context = HyperHandler {
            bm: bm_tmp,
            so: sock_tmp,
        };

        use std::convert::Infallible;
        use std::net::SocketAddr;
        use hyper::{Body, Request, Response, Server};
        use hyper::service::{make_service_fn, service_fn};
        use hyper::server::conn::AddrStream;

        async fn handle(
            context: HyperHandler,
            addr: SocketAddr,
            req: Request<Body>
        ) -> Result<Response<Body>, Infallible> {

            Ok(Response::new(Body::from("Hello World")))
        }

        let make_service = make_service_fn(move |conn: &AddrStream| {
            let context = context.clone();
            let addr = conn.remote_addr();
            let service = service_fn(move |req| {
                handle(context.clone(), addr, req)
            });
            async move { Ok::<_, Infallible>(service) }
        });

        let addr = SocketAddr::from(([127, 0, 0, 1], 3001));
        let server = Server::bind(&addr).serve(make_service);
        server.await?;
        Ok(())
    }));


    let bm_tmp = Arc::clone(&bm);
    let sock_tmp = Arc::clone(&sock);
    starter_tasks.push(tokio::spawn(async move {
        let sock = sock_tmp;
        let mut visited_peers: HashSet<SocketAddr> = Default::default();
        let target_id = self_id ^ TorrentId(*b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xaa\xaa\xaa");
        event!(Level::TRACE, target=?target_id, "start bootstrapper");
        
        let bm = bm_tmp;

        let target: SocketAddr = "172.105.96.16:3019".parse().unwrap();
        let mut msg = DhtMessage {
            transaction: Vec::new(),
            data: DhtMessageData::Query(DhtMessageQuery::FindNode(DhtMessageQueryFindNode {
                id: self_id,
                target: target_id,
                want: Default::default(),
            })),
        };

        let now = Instant::now();

        let mut bm_locked = bm.lock().await;
        let future = dht_query_apply_txid(&mut bm_locked, &bm, &mut msg, target, &now);
        drop(bm_locked);

        let resp_bytes = bencode::to_bytes(&msg).unwrap();
        event!(Level::TRACE, what=?BinStr(&resp_bytes), to=?target, "sending");
        sock.send_to(&resp_bytes, target).await?;

        let completed = future.await?;
        event!(Level::TRACE, completed=?completed, "got response");
        tokio::time::sleep(Duration::new(1, 0)).await;

        let mut targets: VecDeque<(TorrentId, SocketAddr)> = Default::default();
        if let Ok(mr) = completed.response {
            for node in &mr.data.nodes {
                let saddr = SocketAddr::V4(node.1);
                if !visited_peers.contains(&saddr) {
                    visited_peers.insert(saddr);
                    targets.push_back((node.0, saddr));
                }
            }
        } else {
            return Ok(()); // for now
        }

        let mut inflight: FuturesUnordered<_> = futures::stream::FuturesUnordered::new();

        while !targets.is_empty() || !inflight.is_empty() {
            let bm_locked = bm.lock().await;
            println!("{:#?}", bm_locked.buckets);
            drop(bm_locked);

            const CONCURRENCY_LIMIT: usize = 4;
            if CONCURRENCY_LIMIT <= inflight.len() {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            if CONCURRENCY_LIMIT <= inflight.len() || targets.is_empty() {
                let completed: Box<TransactionCompletion> = inflight.next().await.unwrap()?;
                event!(Level::TRACE, completed=?completed, "got response");

                if let Ok(mr) = completed.response {
                    for node in &mr.data.nodes {
                        let saddr = SocketAddr::V4(node.1);
                        if !visited_peers.contains(&saddr) {
                            visited_peers.insert(saddr);
                            targets.push_back((node.0, saddr));
                        }
                    }
                }
            }

            if let Some((_tid, saddr)) = targets.pop_front() {
                let mut msg = DhtMessage {
                    transaction: Vec::new(),
                    data: DhtMessageData::Query(DhtMessageQuery::FindNode(
                        DhtMessageQueryFindNode {
                            id: self_id,
                            target: target_id,
                            want: Default::default(),
                        },
                    )),
                };

                let now = Instant::now();
                
                let mut bm_locked = bm.lock().await;
                let future = dht_query_apply_txid(&mut bm_locked, &bm, &mut msg, saddr, &now);
                drop(bm_locked);
                inflight.push(future);

                let resp_bytes = bencode::to_bytes(&msg).unwrap();
                event!(
                    Level::TRACE,
                    "sending {:?} to {}",
                    BinStr(&resp_bytes),
                    saddr
                );
                sock.send_to(&resp_bytes, saddr).await?;
            }
        }

        Ok(())
    }));

    // let bm_tmp = Arc::clone(&bm);
    // let sock_tmp = Arc::clone(&sock);
    // starter_tasks.push(tokio::spawn(async move {
    //     let sock = sock_tmp;
    //     let mut visited_peers: HashSet<SocketAddr> = Default::default();
    //     let target_id = self_id ^ TorrentId(*b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xaa\xaa\xaa");
    //     event!(Level::TRACE, target=?target_id, "start spider");
        
    //     let bm = bm_tmp;

    //     let target: SocketAddr = "172.105.96.16:3019".parse().unwrap();
    //     let mut msg = DhtMessage {
    //         transaction: Vec::new(),
    //         data: DhtMessageData::Query(DhtMessageQuery::FindNode(DhtMessageQueryFindNode {
    //             id: self_id,
    //             target: target_id,
    //             want: Default::default(),
    //         })),
    //     };

    //     let now = Instant::now();

    //     let mut bm_locked = bm.lock().await;
    //     let future = dht_query_apply_txid(&mut bm_locked, &bm, &mut msg, target, &now);
    //     drop(bm_locked);

    //     let resp_bytes = bencode::to_bytes(&msg).unwrap();
    //     event!(Level::TRACE, what=?BinStr(&resp_bytes), to=?target, "sending");
    //     sock.send_to(&resp_bytes, target).await?;

    //     let completed = future.await?;
    //     event!(Level::TRACE, completed=?completed, "got response");
    //     tokio::time::sleep(Duration::new(1, 0)).await;

    //     let mut targets: VecDeque<(TorrentId, SocketAddr)> = Default::default();
    //     if let Ok(mr) = completed.response {
    //         for node in &mr.data.nodes {
    //             let saddr = SocketAddr::V4(node.1);
    //             if !visited_peers.contains(&saddr) {
    //                 visited_peers.insert(saddr);
    //                 targets.push_back((node.0, saddr));
    //             }
    //         }
    //     } else {
    //         return Ok(()); // for now
    //     }

    //     let mut inflight: FuturesUnordered<_> = futures::stream::FuturesUnordered::new();

    //     while !targets.is_empty() || !inflight.is_empty() {
    //         const CONCURRENCY_LIMIT: usize = 256;
    //         if CONCURRENCY_LIMIT <= inflight.len() {
    //             tokio::time::sleep(Duration::from_millis(50)).await;
    //         }
    //         if CONCURRENCY_LIMIT <= inflight.len() || targets.is_empty() {
    //             let completed: Box<TransactionCompletion> = inflight.next().await.unwrap()?;
    //             event!(Level::TRACE, completed=?completed, "got response");

    //             if let Ok(mr) = completed.response {
    //                 for node in &mr.data.nodes {
    //                     let saddr = SocketAddr::V4(node.1);
    //                     if !visited_peers.contains(&saddr) {
    //                         visited_peers.insert(saddr);
    //                         targets.push_back((node.0, saddr));
    //                     }
    //                 }
    //             }
    //         }

    //         if let Some((_tid, saddr)) = targets.pop_front() {
    //             let mut msg = DhtMessage {
    //                 transaction: Vec::new(),
    //                 data: DhtMessageData::Query(DhtMessageQuery::FindNode(
    //                     DhtMessageQueryFindNode {
    //                         id: self_id,
    //                         target: target_id,
    //                         want: Default::default(),
    //                     },
    //                 )),
    //             };

    //             let now = Instant::now();
                
    //             let mut bm_locked = bm.lock().await;
    //             let future = dht_query_apply_txid(&mut bm_locked, &bm, &mut msg, target, &now);
    //             drop(bm_locked);
    //             inflight.push(future);

    //             let resp_bytes = bencode::to_bytes(&msg).unwrap();
    //             event!(
    //                 Level::TRACE,
    //                 "sending {:?} to {}",
    //                 BinStr(&resp_bytes),
    //                 saddr
    //             );
    //             sock.send_to(&resp_bytes, saddr).await?;
    //         }
    //     }

    //     Ok(())
    // }));

    let mut futures: FuturesUnordered<tokio::task::JoinHandle<Result<(), failure::Error>>> =
        starter_tasks.into_iter().collect();

    while !futures.is_empty() {
        futures.next().await.unwrap().unwrap().unwrap();
    }

    Ok(())
}

pub struct BinStr<'a>(pub &'a [u8]);

impl std::fmt::Debug for BinStr<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "b\"")?;
        for &b in self.0 {
            match b {
                b'\0' => write!(f, "\\0")?,
                b'\n' => write!(f, "\\n")?,
                b'\r' => write!(f, "\\r")?,
                b'\t' => write!(f, "\\t")?,
                b'\\' => write!(f, "\\\\")?,
                b'"' => write!(f, "\\\"")?,
                _ if 0x20 <= b && b < 0x7F => write!(f, "{}", b as char)?,
                _ => write!(f, "\\x{:02x}", b)?,
            }
        }
        write!(f, "\"")?;
        Ok(())
    }
}

pub struct BinStrBuf(pub Vec<u8>);

impl std::fmt::Debug for BinStrBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let bin_str = BinStr(&self.0);
        write!(f, "{:?}.to_vec()", bin_str)
    }
}

pub fn hex<'a>(scratch: &'a mut [u8], input: &[u8]) -> Option<&'a str> {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

    if scratch.len() < input.len() * 2 {
        return None;
    }

    let mut sciter = scratch.iter_mut();
    for by in input {
        *sciter.next().unwrap() = HEX_CHARS[usize::from(*by >> 4)];
        *sciter.next().unwrap() = HEX_CHARS[usize::from(*by & 0xF)];
    }
    drop(sciter);

    Some(std::str::from_utf8(&scratch[..input.len() * 2]).unwrap())
}

#[allow(clippy::cognitive_complexity)] // macro bug around event!()
fn print_test_logging() {
    event!(Level::TRACE, "logger initialized - trace check");
    event!(Level::DEBUG, "logger initialized - debug check");
    event!(Level::INFO, "logger initialized - info check");
    event!(Level::WARN, "logger initialized - warn check");
    event!(Level::ERROR, "logger initialized - error check");
}
