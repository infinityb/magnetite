use std::any::Any;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::{HashSet, VecDeque};
use std::fs::File;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6, ToSocketAddrs};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{App, Arg};
use futures::channel::oneshot;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};
use rand::prelude::SliceRandom;
use rand::thread_rng;
use rand::{RngCore, SeedableRng};
use smallvec::SmallVec;
use tokio::net::{self, UdpSocket};
use tokio::sync::{mpsc, Mutex};
use tokio::{task, time};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};
use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::FmtSubscriber;

use bin_str::BinStr;
use dht::wire::{
    DhtErrorResponse, DhtMessage, DhtMessageData, DhtMessageQuery, DhtMessageQueryAnnouncePeer,
    DhtMessageQueryFindNode, DhtMessageQueryGetPeers, DhtMessageQueryPing, DhtMessageResponse,
    DhtMessageResponseData, DhtNodeSave,
};
use dht::{
    BucketFormatter, BucketManager, GeneralEnvironment, RecursionState, RequestEnvironment,
    ThinNode, TransactionCompletion, BUCKET_SIZE,
};
use magnetite_common::TorrentId;
use magnetite_tracker_lib::{AnnounceCtx, TrackerSearch};

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_NAME: &str = env!("CARGO_PKG_NAME");

mod search;

struct DedupVecDeque<T> {
    queue: VecDeque<T>,
    inset: HashSet<T>,
}

impl<T> Default for DedupVecDeque<T> {
    fn default() -> DedupVecDeque<T> {
        DedupVecDeque {
            queue: VecDeque::new(),
            inset: HashSet::new(),
        }
    }
}

impl<T: Hash + Eq + Clone> DedupVecDeque<T> {
    fn pop_front(&mut self) -> Option<T> {
        let v = self.queue.pop_front()?;
        self.inset.remove(&v);
        Some(v)
    }

    fn push_back(&mut self, v: T) {
        if self.inset.contains(&v) {
            return;
        }
        self.queue.push_back(v.clone());
        self.inset.insert(v);
    }
}

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
    env: &GeneralEnvironment,
) -> DhtMessage {
    let closest = bm.find_close_nodes(&m_qfn.target, BUCKET_SIZE, env);
    let want_v4 = true;
    let want_v6 = false;

    let mut nodes: Vec<(TorrentId, SocketAddrV4)> = Vec::new();
    let mut nodes6: Vec<SocketAddrV6> = Vec::new();
    for node in closest {
        match node.thin.saddr {
            SocketAddr::V4(v4) => {
                if want_v4 {
                    nodes.push((node.thin.id, v4))
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
    env: &GeneralEnvironment,
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
            now: env.now,
            info_hash: m_gp.info_hash,
            cookie: hasher.finish(),
        },
        &mut peers,
    );

    if peers.is_empty() {
        for node in bm.find_close_nodes(&m_gp.info_hash, BUCKET_SIZE, env) {
            match node.thin.saddr {
                SocketAddr::V4(v4) => {
                    if want_v4 {
                        nodes.push((node.thin.id, v4));
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
    env: &GeneralEnvironment,
    client_addr: &SocketAddr,
) -> DhtMessage {
    let chktoken = bm.check_token(&m_ap.token, client_addr);
    event!(Level::INFO,
        kind="announce-peer",
        from=?client_addr,
        announce=?m_ap,
        token_ok=?chktoken,
        "announce",
    );
    if !chktoken {
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
        &AnnounceCtx { now: env.now },
        unimplemented!("none or some?"),
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

fn dht_query_apply_txid(
    bm: &mut BucketManager,
    bma: &Rc<RefCell<BucketManager>>,
    message: &mut DhtMessage,
    to: SocketAddr,
    now: &Instant,
    queried_peer_id: Option<TorrentId>,
) -> oneshot::Receiver<Box<TransactionCompletion>> {
    let txslot = bm.acquire_transaction_slot();
    let txid = txslot.key();

    let future = txslot.assign(message, to, now, queried_peer_id);
    drop(bm);

    let bm_tmp = Rc::clone(&bma);
    task::spawn_local(async move {
        let bm = bm_tmp;

        tokio::time::sleep(Duration::new(3, 0)).await;
        let mut bm_locked = bm.borrow_mut();
        let genv = GeneralEnvironment {
            now: Instant::now(),
        };
        bm_locked.clean_expired_transaction(txid, &genv);
        drop(bm_locked);
    });

    future
}

#[derive(Clone)]
pub struct DhtContext {
    bm: Rc<RefCell<BucketManager>>,
    so: Rc<UdpSocket>,
}

fn should_bootstrap(bm: &BucketManager) -> bool {
    for bucket in &bm.buckets {
        if bucket.nodes.len() < 8 {
            return true;
        }
    }
    false
}

fn should_bootstrap_hardest(bm: &BucketManager) -> bool {
    for bucket in &bm.buckets {
        if bucket.nodes.len() != 0 {
            return false;
        }
    }
    true
}

async fn send_to_node(so: &UdpSocket, to: SocketAddr, msg: &DhtMessage) -> io::Result<usize> {
    let resp_bytes = bencode::to_bytes(&msg).unwrap();
    event!(Level::TRACE, what=?msg, what_bin=?BinStr(&resp_bytes), to=?to, "sending");
    let res = so.send_to(&resp_bytes, to).await;
    if let Err(ref err) = res {
        event!(Level::ERROR, to=?to, "failed to send: {}", err);
    }
    res
}

// trait DhtSystemCommand: Into<DhtSystemRequestData> {
//     type Ok: Send;

//     fn extract(boxed: Box<dyn Any + Send>) -> Self::Ok;
// }

struct DhtSystemRequest {
    data: DhtSystemRequestData,
    response: oneshot::Sender<Box<dyn Any + Send>>,
}

enum DhtSystemRequestData {
    Search(DhtSystemRequestSearch),
}

struct DhtSystemRequestSearch {
    target: TorrentId,
    search_kind: search::SearchKind,
}

impl Into<DhtSystemRequestData> for DhtSystemRequestSearch {
    fn into(self) -> DhtSystemRequestData {
        DhtSystemRequestData::Search(self)
    }
}

// impl DhtSystemCommand for DhtSystemRequestSearch {
//     type Ok = mpsc::Receiver<SearchEvent>;

//     fn extract(boxed: Box<dyn Any + Send>) -> Self::Ok {
//         boxed.downcast::<Self::Ok>().expect("command responded with bad response")
//     }
// }

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
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

    let (tx, rx) = mpsc::channel::<DhtSystemRequestData>(64);
    task::LocalSet::new()
        .run_until(async {
            let sock = Rc::new(UdpSocket::bind(&bind_address).await?);
            let mut starter_tasks: Vec<tokio::task::JoinHandle<Result<(), failure::Error>>> =
                Vec::new();
            let self_id = TorrentId(*b"\x18E)\xd0j\xc4V\x9e\x03Yu[t\xbb\x85\x12{Z\xc4o");
            let bm = Rc::new(RefCell::new(BucketManager::new(self_id)));
            let bm_tmp = Rc::clone(&bm);
            let sock_tmp = Rc::clone(&sock);

            let context = DhtContext {
                bm: Rc::clone(&bm),
                so: Rc::clone(&sock),
            };
            starter_tasks.push(task::spawn_local(async move {
                let mut rx = ReceiverStream::new(rx);
                while let Some(v) = rx.next().await {
                    
                }
                Ok(())
            }));

            let context = DhtContext {
                bm: Rc::clone(&bm),
                so: Rc::clone(&sock),
            };
            starter_tasks.push(task::spawn_local(response_engine(context)));

            let context = DhtContext {
                bm: Rc::clone(&bm),
                so: Rc::clone(&sock),
            };
            starter_tasks.push(task::spawn_local(file_saver(context)));

            let context = DhtContext {
                bm: Rc::clone(&bm),
                so: Rc::clone(&sock),
            };
            starter_tasks.push(task::spawn_local(maintenance(context)));

            let mut futures: FuturesUnordered<tokio::task::JoinHandle<Result<(), failure::Error>>> =
                starter_tasks.into_iter().collect();

            while !futures.is_empty() {
                StreamExt::next(&mut futures)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
            }

            Result::<_, failure::Error>::Ok(())
        })
        .await?;

    Result::<_, failure::Error>::Ok(())
}

#[allow(clippy::cognitive_complexity)] // macro bug around event!()
fn print_test_logging() {
    event!(Level::TRACE, "logger initialized - trace check");
    event!(Level::DEBUG, "logger initialized - debug check");
    event!(Level::INFO, "logger initialized - info check");
    event!(Level::WARN, "logger initialized - warn check");
    event!(Level::ERROR, "logger initialized - error check");
}

async fn file_saver(context: DhtContext) -> Result<(), failure::Error> {
    let mut now = Instant::now();
    let mut next_run = now + Duration::from_secs(60);

    let timer = tokio::time::sleep_until(next_run.into());
    tokio::pin!(timer);

    loop {
        timer.as_mut().await;
        now = Instant::now();
        next_run = now + Duration::from_secs(60);
        timer.as_mut().reset(next_run.into());

        let mut nodes = Vec::new();
        let bm_locked = context.bm.borrow_mut();
        for b in &bm_locked.buckets {
            for node in &b.nodes {
                if let SocketAddr::V4(v4) = node.thin.saddr {
                    nodes.push((node.thin.id, v4));
                }
            }
        }

        let bytes = bencode::to_bytes(&DhtNodeSave { nodes })?;
        let mut ff = File::options()
            .write(true)
            .create(true)
            .open("dht-state.ben")?;

        ff.write_all(&bytes[..])?;
        drop(ff);

        let formatted = format!(
            "self-id: {}\n{}",
            bm_locked.self_peer_id.hex(),
            bm_locked.format_buckets()
        );

        println!("{}", formatted);

        drop(bm_locked);
    }
}

async fn response_engine(context: DhtContext) -> Result<(), failure::Error> {
    let mut buf = [0; 1400];
    loop {
        let (len, addr) = context.so.recv_from(&mut buf).await?;
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

        event!(Level::TRACE, from=?addr, message=?decoded, "rx-message");

        let genv = GeneralEnvironment {
            now: Instant::now(),
        };
        let mut bm_locked = context.bm.borrow_mut();
        let is_reply = bm_locked.handle_incoming_packet(&decoded, addr, &genv);
        let env = RequestEnvironment {
            gen: genv,
            is_reply,
        };

        let mut peer_id = None;
        let mut response = None;
        match decoded.data {
            DhtMessageData::Query(ref qq) => {
                let mut ff = File::options()
                    .write(true)
                    .create(true)
                    .open("query.log")
                    .unwrap();

                write!(&mut ff, "[from {}] {:?}", addr, qq).unwrap();
            }
            _ => (),
        }
        match decoded.data {
            DhtMessageData::Query(DhtMessageQuery::Ping(ref ping)) => {
                peer_id = Some(ping.id);
                response = Some(handle_query_ping(&bm_locked, &decoded));
            }
            DhtMessageData::Query(DhtMessageQuery::FindNode(ref find_node)) => {
                peer_id = Some(find_node.id);
                response = Some(handle_query_find_node(
                    &bm_locked, &decoded, find_node, &env.gen,
                ));
            }
            DhtMessageData::Query(DhtMessageQuery::GetPeers(ref gp)) => {
                peer_id = Some(gp.id);
                response = Some(handle_query_get_peers(
                    &bm_locked, &decoded, gp, &env.gen, &addr,
                ));
            }
            DhtMessageData::Query(DhtMessageQuery::AnnouncePeer(ref ap)) => {
                peer_id = Some(ap.id);
                response = Some(handle_query_announce_peer(
                    &mut bm_locked,
                    &decoded,
                    ap,
                    &env.gen,
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
            bm_locked.node_seen(
                &ThinNode {
                    id: pid,
                    saddr: addr,
                },
                &env,
            );
        }

        drop(bm_locked);

        if let Some(ref resp) = response {
            send_to_node(&context.so, addr, resp).await;
        }
    }
}

struct RateLimit {
    last_sent: Instant,
    recent_sent_queries: u64,
}

fn rate_limit_check_and_incr(
    tree: &mut BTreeMap<SocketAddr, RateLimit>,
    addr: SocketAddr,
    nenv: &RequestEnvironment,
) -> bool {
    let v = tree.entry(addr).or_insert(RateLimit {
        last_sent: nenv.gen.now,
        recent_sent_queries: 0,
    });

    // event!(Level::INFO,
    //     now=?nenv.gen.now.elapsed(),
    //     last_sent=?v.last_sent.elapsed(),
    //     recent_sent_queries=?v.recent_sent_queries,
    //     should_rt=?(nenv.gen.now < (v.last_sent + Duration::from_secs(90))),
    //     "checking-rate-limit");
    if (v.last_sent + Duration::from_secs(90)) < nenv.gen.now {
        v.recent_sent_queries = 1;
        v.last_sent = nenv.gen.now;
        true
    } else {
        if v.recent_sent_queries < 3 {
            v.recent_sent_queries += 1;
            true
        } else {
            false
        }
    }
}

async fn bootstrap(context: DhtContext) -> Result<(), failure::Error> {
    // FIXME: duplicate state between maintenance and bootstrap:
    let mut recent_peers_queried: BTreeMap<SocketAddr, RateLimit> = Default::default();

    let bm_locked = context.bm.borrow_mut();
    let self_peer_id = bm_locked.self_peer_id;
    drop(bm_locked);

    let mut target_id = self_peer_id;
    let mut rng = rand::rngs::StdRng::from_entropy();
    rng.fill_bytes(&mut target_id.as_mut_bytes()[12..]);

    let mut msg: DhtMessage = Into::into(DhtMessageQueryFindNode {
        id: self_peer_id,
        target: target_id,
        want: SmallVec::new(),
    });
    let mut addresses = Vec::new();
    addresses.extend(net::lookup_host("router.utorrent.com:6881").await?);
    addresses.extend(net::lookup_host("router.bittorrent.com:6881").await?);
    addresses.extend(net::lookup_host("dht.transmissionbt.com:6881").await?);

    let mut nenv = RequestEnvironment {
        gen: GeneralEnvironment {
            now: Instant::now() - Duration::from_secs(900),
        },
        is_reply: false,
    };
    let mut to_send_msgs = Vec::new();
    let mut bm_locked = context.bm.borrow_mut();
    for addr in addresses {
        if rate_limit_check_and_incr(&mut recent_peers_queried, addr, &nenv) {
            let mut node_msg = msg.clone();
            dht_query_apply_txid(
                &mut bm_locked,
                &context.bm,
                &mut node_msg,
                addr,
                &nenv.gen.now,
                None,
            );

            to_send_msgs.push((addr, node_msg));
        }
    }
    drop(bm_locked);

    for msg in &to_send_msgs {
        send_to_node(&context.so, msg.0, &msg.1).await;
    }
    Ok(())
}

async fn maintenance(context: DhtContext) -> Result<(), failure::Error> {
    let mut rng = rand::rngs::StdRng::from_entropy();
    let mut recent_peers_queried: BTreeMap<SocketAddr, RateLimit> = Default::default();

    let bm_locked = context.bm.borrow_mut();
    let self_peer_id = bm_locked.self_peer_id;
    drop(bm_locked);

    let mut target_id = self_peer_id;
    // slightly randomized target
    rng.fill_bytes(&mut target_id.as_mut_bytes()[12..]);

    let mut targets: DedupVecDeque<(TorrentId, SocketAddr)> = Default::default();
    let mut find_worst_bucket = false;
    let mut next_request = Instant::now();
    let timer = tokio::time::sleep_until(next_request.into());
    tokio::pin!(timer);

    if let Ok(mut ff) = File::options().read(true).open("dht-state.ben") {
        let mut bytes = Vec::new();
        ff.read_to_end(&mut bytes)?;
        drop(ff);

        let decoded: DhtNodeSave = bencode::from_bytes(&bytes[..])?;
        let nenv = RequestEnvironment {
            gen: GeneralEnvironment {
                now: Instant::now() - Duration::from_secs(900),
            },
            is_reply: false,
        };

        let mut bm_locked = context.bm.borrow_mut();
        for node in &decoded.nodes {
            let saddr = SocketAddr::V4(node.1);
            bm_locked.node_seen(&ThinNode { id: node.0, saddr }, &nenv);
        }
        drop(bm_locked);
    }

    let mut bm_locked = context.bm.borrow_mut();
    if should_bootstrap_hardest(&bm_locked) {
        drop(bm_locked);
        event!(
            Level::INFO,
            file = file!(),
            line = line!(),
            "maintenance-blocker enter"
        );
        bootstrap(context.clone()).await?;
        event!(
            Level::INFO,
            file = file!(),
            line = line!(),
            "maintenance-blocker exit"
        );
    } else {
        drop(bm_locked);
    }

    event!(Level::TRACE, target=?target_id, "start bootstrapper/maintenance");

    let mut bm_locked = context.bm.borrow_mut();
    let formatted = format!(
        "self-id: {}\n{}",
        bm_locked.self_peer_id.hex(),
        bm_locked.format_buckets()
    );
    drop(bm_locked);

    println!("{}", formatted);

    loop {
        event!(
            Level::INFO,
            file = file!(),
            line = line!(),
            "maintenance-blocker enter"
        );
        timer.as_mut().await;
        event!(
            Level::INFO,
            file = file!(),
            line = line!(),
            "maintenance-blocker exit"
        );
        let mut nenv = RequestEnvironment {
            gen: GeneralEnvironment {
                now: Instant::now(),
            },
            is_reply: false,
        };
        let bm_locked = context.bm.borrow();
        if should_bootstrap(&bm_locked) {
            drop(bm_locked);
            next_request = nenv.gen.now + Duration::from_secs(1);
            timer.as_mut().reset(next_request.into());
        } else {
            drop(bm_locked);
            next_request = nenv.gen.now + Duration::from_secs(6);
            timer.as_mut().reset(next_request.into());
        }

        nenv.gen.now = Instant::now();

        let bucket;
        let mut want_nodes = false;

        let mut bm_locked = context.bm.borrow_mut();
        if find_worst_bucket {
            find_worst_bucket = false;
            want_nodes = false;
            bucket = bm_locked.find_mut_worst_bucket(&nenv.gen);
            eprintln!(
                "worst_bucket: {}",
                BucketFormatter {
                    bb: bucket,
                    genv: &nenv.gen,
                }
            );
            // event!(Level::INFO, worst_bucket=, "maintenance-find-bucket");
        } else {
            find_worst_bucket = true;
            bucket = bm_locked.find_mut_oldest_bucket();
            eprintln!(
                "oldest_bucket: {}",
                BucketFormatter {
                    bb: bucket,
                    genv: &nenv.gen,
                }
            );
            // event!(Level::INFO, oldest_bucket=BucketFormatter { bb: bucket }, "maintenance-find-bucket");
        }
        want_nodes |= !bucket.is_saturated(&nenv.gen);
        want_nodes |= bucket.prefix.contains(&self_peer_id);
        let bucket_is_saturated = bucket.is_saturated(&nenv.gen);
        let search_target = bucket.prefix.rand_within(&mut rng);

        if let Some(node) = bucket.find_mut_node_for_maintenance_ping(&nenv.gen) {
            let mut msg = if want_nodes {
                Into::into(DhtMessageQueryFindNode {
                    id: self_peer_id,
                    target: target_id,
                    want: SmallVec::new(),
                })
            } else {
                Into::into(DhtMessageQueryPing { id: self_peer_id })
            };

            node.apply_activity_receive_request(&nenv.gen);
            let node_thin = node.thin;

            drop(node);
            drop(bucket);
            let future = dht_query_apply_txid(
                &mut bm_locked,
                &context.bm,
                &mut msg,
                node_thin.saddr,
                &nenv.gen.now,
                Some(node_thin.id),
            );
            drop(bm_locked);
            event!(
                Level::INFO,
                file = file!(),
                line = line!(),
                "maintenance-blocker enter"
            );
            send_to_node(&context.so, node_thin.saddr, &msg).await;
            event!(
                Level::INFO,
                file = file!(),
                line = line!(),
                "maintenance-blocker exit"
            );
            let completed = future.await?;

            if let Ok(mr) = completed.response {
                for node in &mr.data.nodes {
                    let saddr = SocketAddr::V4(node.1);
                    targets.push_back((node.0, saddr));
                }
            }
        } else {
            drop(bucket);
            drop(bm_locked);
        }

        if !bucket_is_saturated {
            event!(
                Level::INFO,
                file = file!(),
                line = line!(),
                "maintenance-blocker enter"
            );
            event!(Level::DEBUG, target=?search_target, "initiating search....");
            if let Ok(s) = search::start_search(
                context.clone(),
                search_target,
                search::SearchKind::FindNode,
                64,
            ) {
                task::spawn_local(async move {
                    let mut s = tokio_stream::wrappers::ReceiverStream::new(s);
                    while let Some(v) = s.next().await {
                        tokio::time::sleep(Duration::from_millis(450)).await;
                    }
                });
                event!(Level::DEBUG, target=?search_target, "finished.");
            }
            event!(
                Level::INFO,
                file = file!(),
                line = line!(),
                "maintenance-blocker exit"
            );
        }

        while let Some((tid, saddr)) = targets.pop_front() {
            let mut msg = Into::into(DhtMessageQueryFindNode {
                id: self_peer_id,
                target: target_id,
                want: Default::default(),
            });

            nenv.gen.now = Instant::now();
            event!(
                Level::INFO,
                file = file!(),
                line = line!(),
                "maintenance-blocker enter"
            );
            if rate_limit_check_and_incr(&mut recent_peers_queried, saddr, &nenv) {
                tokio::time::sleep(Duration::from_millis(100)).await;

                nenv.gen.now = Instant::now();
                let mut bm_locked = context.bm.borrow_mut();
                dht_query_apply_txid(
                    &mut bm_locked,
                    &context.bm,
                    &mut msg,
                    saddr,
                    &nenv.gen.now,
                    Some(tid),
                );
                drop(bm_locked);
                send_to_node(&context.so, saddr, &msg).await;
            }
            event!(
                Level::INFO,
                file = file!(),
                line = line!(),
                "maintenance-blocker exit"
            );
        }
    }
}
