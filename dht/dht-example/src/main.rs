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
    BucketFormatter, BucketInfoFormatter, BucketManager2, GeneralEnvironment, RecursionState, RequestEnvironment, ThinNode, TransactionCompletion, BUCKET_SIZE
};
use magnetite_common::TorrentId;
use magnetite_tracker_lib::{AnnounceCtx, TrackerSearch};

type BucketManager = BucketManager2;

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_NAME: &str = env!("CARGO_PKG_NAME");

mod search;
// mod debug_server;

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
    mut bm: std::cell::RefMut<'_, BucketManager>,
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

        tokio::time::sleep(Duration::new(13, 0)).await;
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
            let mut starter_tasks: Vec<tokio::task::JoinHandle<Result<(), anyhow::Error>>> =
                Vec::new();
            let self_id = TorrentId(*b"\x18E)\xd0j\xc4V\x9e\x03Yu[t\xbb\x85\x12{Z\xc4o");
            let bm = Rc::new(RefCell::new(BucketManager::new(&self_id)));
            let bm_tmp = Rc::clone(&bm);
            let sock_tmp = Rc::clone(&sock);

            // let context = DhtContext {
            //     bm: Rc::clone(&bm),
            //     so: Rc::clone(&sock),
            // };
            starter_tasks.push(task::spawn_local(async move {
                let mut rx = ReceiverStream::new(rx);
                while let Some(v) = rx.next().await {
                    //
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
            starter_tasks.push(task::spawn_local(file_sync(context)));

            let context = DhtContext {
                bm: Rc::clone(&bm),
                so: Rc::clone(&sock),
            };
            starter_tasks.push(task::spawn_local(maintenance(context)));

            let mut futures: FuturesUnordered<tokio::task::JoinHandle<anyhow::Result<()>>> =
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

async fn file_sync(context: DhtContext) -> anyhow::Result<()> {
    let mut recent_peers_queried: BTreeMap<SocketAddr, RateLimit> = Default::default();
    let mut rng = rand::rngs::StdRng::from_entropy();
    let bm_locked = context.bm.borrow_mut();
    let self_peer_id = bm_locked.self_peer_id;
    drop(bm_locked);

    let mut target_id = self_peer_id;
    // slightly randomized target
    rng.fill_bytes(&mut target_id.as_mut_bytes()[12..]);

    let mut nodes_to_ping_as_bootstrap = Vec::new();
    if let Ok(mut ff) = File::options().read(true).open("dht-state.ben") {
        let mut bytes = Vec::new();
        ff.read_to_end(&mut bytes)?;
        drop(ff);

        let gen = GeneralEnvironment {
            now: Instant::now() - Duration::from_secs(900),
        };
        let decoded: DhtNodeSave = bencode::from_bytes(&bytes[..])?;
        let mut bm_locked = context.bm.borrow_mut();
        for node in &decoded.nodes {
            let nenv = RequestEnvironment {
                gen,
                is_reply: false,
                addr: SocketAddr::V4(node.1),
            };
            let saddr = SocketAddr::V4(node.1);
            bm_locked.node_seen(&ThinNode { id: node.0, saddr }, &nenv);
        }
        nodes_to_ping_as_bootstrap.extend(decoded.nodes.iter().map(|(x, y)| (*y, *x)));
        drop(bm_locked);
    } else {
        let bm_locked = context.bm.borrow_mut();
        let self_peer_id = bm_locked.self_peer_id;
        drop(bm_locked);

        let mut target_id = self_peer_id;
        let mut rng = rand::rngs::StdRng::from_entropy();
        rng.fill_bytes(&mut target_id.as_mut_bytes()[12..]);

        let msg: DhtMessage = Into::into(DhtMessageQueryFindNode {
            id: self_peer_id,
            target: target_id,
            want: SmallVec::new(),
        });
        let mut addresses = Vec::new();
        addresses.extend(net::lookup_host("router.bittorrent.com:6881").await?);
        addresses.extend(net::lookup_host("dht.transmissionbt.com:6881").await?);

        let mut to_send_msgs = Vec::new();
        let gen = GeneralEnvironment {
            now: Instant::now() - Duration::from_secs(900),
        };
        for addr in addresses {
            let nenv = RequestEnvironment {
                gen,
                is_reply: false,
                addr,
            };
            let bm_locked = context.bm.borrow_mut();
            if rate_limit_check_and_incr(&mut recent_peers_queried, addr, &nenv) {
                let mut node_msg = msg.clone();
                let _ = dht_query_apply_txid(
                    bm_locked,
                    &context.bm,
                    &mut node_msg,
                    addr,
                    &nenv.gen.now,
                    None,
                );
                to_send_msgs.push((addr, node_msg));
            }
        }

        for msg in &to_send_msgs {
            if let Err(e) = send_to_node(&context.so, msg.0, &msg.1).await {
                event!(Level::WARN, "error sending {}", e);
            }
        }
    }

    let msg: DhtMessage = Into::into(DhtMessageQueryFindNode {
        id: self_peer_id,
        target: target_id,
        want: SmallVec::new(),
    });

    let max_inflight = 64;
    let mut running = futures::stream::FuturesUnordered::new();
    for (addr, node) in &nodes_to_ping_as_bootstrap {
        use futures::prelude::stream::StreamExt;
        while max_inflight <= running.len() {
            running.next().await;
        }
        let now = Instant::now();
        let addr = SocketAddr::V4(*addr);
        let mut node_msg = msg.clone();
        let bm_locked = context.bm.borrow_mut();
        running.push(dht_query_apply_txid(
            bm_locked,
            &context.bm,
            &mut node_msg,
            addr,
            &now,
            Some(*node),
        ));
        if let Err(e) = send_to_node(&context.so, addr, &node_msg).await {
            event!(Level::WARN, "error sending {}", e);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    while !running.is_empty() {
        running.next().await;
    }

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
        for (_, node) in &bm_locked.nodes {
            if let SocketAddr::V4(v4) = node.thin.saddr {
                nodes.push((node.thin.id, v4));
            }
        }

        let bytes = bencode::to_bytes(&DhtNodeSave { nodes })?;
        let mut ff = File::options()
            .write(true)
            .create(true)
            .open("dht-state.ben.tmp")?;

        ff.write_all(&bytes[..])?;
        drop(ff);

        std::fs::rename("dht-state.ben.tmp", "dht-state.ben")?;

        let formatted = format!(
            "self-id: {}\n{}",
            bm_locked.self_peer_id.hex(),
            bm_locked.format_buckets()
        );

        println!("{}", formatted);

        drop(bm_locked);
    }
}

async fn response_engine(context: DhtContext) -> anyhow::Result<()> {
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
        event!(Level::INFO, is_reply=%is_reply, "rx-message");

        let env = RequestEnvironment {
            gen: genv,
            is_reply,
            addr,
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
            if is_reply {
                if let Some(node) = bm_locked.nodes.get_mut(&pid) {
                    node.apply_activity_receive_response(&env.gen);
                }
            }
        }

        drop(bm_locked);

        if let Some(ref resp) = response {
            if let Err(e) = send_to_node(&context.so, addr, resp).await {
                event!(Level::WARN, "error sending {}", e);
            }
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

fn reapply_bucketting(context: DhtContext) -> anyhow::Result<()> {
    use rand::seq::SliceRandom;

    let gen = GeneralEnvironment {
        now: Instant::now() - Duration::from_secs(900),
    };

    let mut bm_locked = context.bm.borrow_mut();
    let mut need_reprocessing = true;
    while need_reprocessing {
        need_reprocessing = false;
        let BucketManager { ref mut buckets, ref mut nodes, self_peer_id, .. } = *bm_locked;
        for (_base, bi) in buckets {
            let mut good_nodes_unassigned = 0;
            let mut good_nodes_assigned = 0;
            for (_nid, node) in nodes.range_mut(bi.prefix.to_range()) {
                if node.quality(&gen).is_good() {
                    if node.in_bucket {
                        good_nodes_assigned += 1;
                    } else {
                        good_nodes_unassigned += 1;
                    }
                }
            }
            event!(
                Level::DEBUG,
                good_nodes_assigned=good_nodes_assigned,
                good_nodes_unassigned=good_nodes_unassigned,
                "collected-initial-node-stats"
            );

            assert!(good_nodes_assigned <= BUCKET_SIZE, "cannot have more than 8 good nodes in a bucket");
            let remaining_good_slots = if good_nodes_assigned <= BUCKET_SIZE {
                BUCKET_SIZE - good_nodes_assigned
            } else {
                0
            };

            let mut max_demotes = std::cmp::min(remaining_good_slots, std::cmp::max(BUCKET_SIZE, good_nodes_unassigned));
            let mut node_candidates = Vec::new();
            for (_nid, node) in nodes.range_mut(bi.prefix.to_range()) {
                // demote as many as we are promoting later in the loop iteration.
                if !node.quality(&gen).is_good() && node.in_bucket {
                    node_candidates.push(node);
                }
            }
            node_candidates.shuffle(&mut thread_rng());
            node_candidates.truncate(max_demotes);
            event!(Level::DEBUG, demote_count=node_candidates.len(), "demote");
            for n in node_candidates.into_iter() {
                n.in_bucket = false;
                max_demotes -= 1;
            }

            let max_promotes = remaining_good_slots;
            let mut node_candidates = Vec::new();
            for (_nid, node) in nodes.range_mut(bi.prefix.to_range()) {
                if !node.in_bucket && node.quality(&gen).is_good() {
                    node_candidates.push(node);
                }
            }
            node_candidates.shuffle(&mut thread_rng());
            node_candidates.truncate(max_promotes);
            event!(Level::DEBUG, promote_count=node_candidates.len(), "promote");
            for n in node_candidates.into_iter() {
                n.in_bucket = true;
            }

            let mut good_nodes_assigned = 0;
            for (_nid, node) in nodes.range_mut(bi.prefix.to_range()) {
                if node.quality(&gen).is_good() {
                    if node.in_bucket {
                        good_nodes_assigned += 1;
                    }
                }
            }
            assert!(good_nodes_assigned <= BUCKET_SIZE, "cannot have more than 8 good nodes in a bucket");

            event!(Level::DEBUG,
                good_nodes_assigned=good_nodes_assigned,
                is_bucket_saturated=BUCKET_SIZE <= good_nodes_assigned,
                is_self_bucket=bi.prefix.contains(&self_peer_id),
                prefix=%bi.prefix,
                self_peer_id=?self_peer_id,
                "presplit");

            if BUCKET_SIZE <= good_nodes_assigned && bi.prefix.contains(&self_peer_id) {
                // we will split so reset reprocessing flag.
                event!(Level::DEBUG, "need_reprocessing");
                need_reprocessing = true;
            }
        }

        if need_reprocessing {
            event!(Level::INFO, before_buckets=bm_locked.buckets.len(), "need_reprocessing");
            bm_locked.split_bucket(&self_peer_id)?;
            event!(Level::INFO, after_buckets=bm_locked.buckets.len(), "need_reprocessing");
        }
    }

    let node_expiration = gen.now + Duration::new(7200, 0);
    let mut expiring_nodes = true;
    let mut last_nid = TorrentId::zero();
    while expiring_nodes {
        let mut remove_nid = None;
        for (nid, node) in bm_locked.nodes.range(last_nid..) {
            if node_expiration < node.last_touch_time {
                remove_nid = Some(*nid);
                break;
            }
        }
        expiring_nodes = remove_nid.is_some();
        if let Some(n) = remove_nid {
            last_nid = n;
            bm_locked.nodes.remove(&n).unwrap();
        }
    }
    Ok(())
}

async fn maintenance(context: DhtContext) -> anyhow::Result<()> {
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
    let mut next_request = Instant::now() + Duration::from_secs(15);
    let timer = tokio::time::sleep_until(next_request.into());
    tokio::pin!(timer);

    let bm_locked = context.bm.borrow_mut();
    let formatted = format!(
        "self-id: {}\n{}",
        bm_locked.self_peer_id.hex(),
        bm_locked.format_buckets()
    );
    drop(bm_locked);

    println!("{}", formatted);

    loop {
        reapply_bucketting(context.clone())?;
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
        let mut ngen = GeneralEnvironment {
            now: Instant::now(),
        };

        next_request = ngen.now + Duration::from_secs(6);
        timer.as_mut().reset(next_request.into());

        ngen.now = Instant::now();

        let bucket_prefix;
        let mut want_nodes = false;
        let mut bm_locked = context.bm.borrow_mut();
        if find_worst_bucket {
            find_worst_bucket = false;
            want_nodes = false;
            let bucket = bm_locked.find_worst_bucket(&ngen);
            bucket_prefix = bucket.prefix;
            eprintln!(
                "worst_bucket: {}",
                BucketInfoFormatter {
                    bb: bucket,
                    nodes: bm_locked.nodes_for_bucket_containing_id(&bucket_prefix.base),
                    genv: &ngen,
                }
            );
            // event!(Level::INFO, worst_bucket=, "maintenance-find-bucket");
        } else {
            find_worst_bucket = true;
            let bucket = bm_locked.find_oldest_bucket();
            bucket_prefix = bucket.prefix;
            eprintln!(
                "oldest_bucket: {}",
                BucketInfoFormatter {
                    bb: bucket,
                    nodes: bm_locked.nodes_for_bucket_containing_id(&bucket_prefix.base),
                    genv: &ngen,
                }
            );
            // event!(Level::INFO, oldest_bucket=BucketFormatter { bb: bucket }, "maintenance-find-bucket");
        }
        want_nodes |= !bm_locked.adding_to_bucket_creates_split(&bucket_prefix.base, &ngen);
        want_nodes |= bucket_prefix.contains(&self_peer_id);
        let search_target = bucket_prefix.rand_within(&mut rng);

        if let Some(node) = bm_locked.find_mut_node_for_maintenance_ping(&ngen) {
            let mut msg = if want_nodes {
                Into::into(DhtMessageQueryFindNode {
                    id: self_peer_id,
                    target: search_target,
                    want: SmallVec::new(),
                })
            } else {
                Into::into(DhtMessageQueryPing { id: self_peer_id })
            };
            node.apply_activity_receive_request(&ngen);
            let node_thin = node.thin;
            let future = dht_query_apply_txid(
                bm_locked,
                &context.bm,
                &mut msg,
                node_thin.saddr,
                &ngen.now,
                Some(node_thin.id),
            );
            event!(
                Level::INFO,
                file = file!(),
                line = line!(),
                "maintenance-blocker enter"
            );
            if let Err(e) = send_to_node(&context.so, node_thin.saddr, &msg).await {
                event!(Level::WARN, "error sending {}", e);
            }
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
            drop(bm_locked);
        }

        while let Some((tid, saddr)) = targets.pop_front() {
            let mut msg = Into::into(DhtMessageQueryFindNode {
                id: self_peer_id,
                target: search_target,
                want: Default::default(),
            });

            ngen.now = Instant::now();
            event!(
                Level::INFO,
                file = file!(),
                line = line!(),
                "maintenance-blocker enter"
            );
            let mut nenv = RequestEnvironment {
                gen: ngen,
                is_reply: false,
                addr: saddr,
            };
            if rate_limit_check_and_incr(&mut recent_peers_queried, saddr, &nenv) {
                tokio::time::sleep(Duration::from_millis(100)).await;
                nenv.gen.now = Instant::now();
                let bm_locked = context.bm.borrow_mut();
                let _ = dht_query_apply_txid(
                    bm_locked,
                    &context.bm,
                    &mut msg,
                    saddr,
                    &ngen.now,
                    Some(tid),
                );
                if let Err(e) = send_to_node(&context.so, saddr, &msg).await {
                    event!(Level::WARN, "error sending {}", e);
                }
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
