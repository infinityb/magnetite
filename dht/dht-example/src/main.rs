use std::io;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::time::{Duration, Instant};
use std::hash::{Hasher, BuildHasher};
use std::net::ToSocketAddrs;

use chrono::prelude::Utc;
use clap::{App, Arg};
use tokio::net::UdpSocket;
use tracing::Instrument;
use tracing::{event, Level};
use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::FmtSubscriber;

use dht::wire::{
    DhtErrorResponse, DhtMessageQueryAnnouncePeer,
    DhtMessage, DhtMessageData, DhtMessageQuery, DhtMessageQueryFindNode, DhtMessageQueryGetPeers,
    DhtMessageResponse, DhtMessageResponseData, DhtMessageResponseFindNode,
    DhtMessageResponseGetPeers, DhtMessageResponseGetPeersData,
};
use dht::{BucketManager, ConfirmLevel, Node, NodeEnvironment, BUCKET_SIZE, TRANSACTION_SIZE_BYTES};
use magnetite_common::TorrentId;

struct TrackerEntry {
    peer_addr: SocketAddr,
    expiration: Instant,
}

fn handle_query_ping(bm: &BucketManager, message: &DhtMessage) -> DhtMessage {
    DhtMessage {
        transaction: message.transaction.clone(),
        data: DhtMessageData::Response(DhtMessageResponse {
            response: DhtMessageResponseData::GeneralId {
                id: bm.self_peer_id,
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
    let mut closest = bm.find_close_nodes(&m_qfn.target, BUCKET_SIZE, environment);

    let mut found_node_id = false;
    if let Some(last) = closest.last() {
        found_node_id = last.peer_id == m_qfn.target;
    }
    if found_node_id {
        let target_node = closest.pop().unwrap();
        closest.clear();
        closest.push(target_node);
    }

    let want_v4 = true;
    let want_v6 = false;

    let mut nodes: Vec<SocketAddrV4> = Vec::new();
    let mut nodes6: Vec<SocketAddrV6> = Vec::new();
    for node in closest {
        match node.peer_addr {
            SocketAddr::V4(v4) => {
                if want_v4 {
                    nodes.push(v4)
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
            response: DhtMessageResponseData::FindNode(DhtMessageResponseFindNode {
                id: bm.self_peer_id,
                nodes,
                nodes6,
            }),
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

    let mut peers = Vec::new();
    let mut nodes: Vec<SocketAddrV4> = Vec::new();
    let mut nodes6: Vec<SocketAddrV6> = Vec::new();

    let data;
    if peers.is_empty() {
        for node in bm.find_close_nodes(&m_gp.info_hash, BUCKET_SIZE, &environment) {
            match node.peer_addr {
                SocketAddr::V4(v4) => {
                    if want_v4 {
                        nodes.push(v4)
                    }
                }
                SocketAddr::V6(v6) => {
                    if want_v6 {
                        nodes6.push(v6)
                    }
                }
            }
        }

        data = DhtMessageResponseGetPeersData::CloseNodes { nodes, nodes6 };
    } else {
        data = DhtMessageResponseGetPeersData::Peers { values: peers };
    }

    let token = bm.generate_token(client_addr);
    DhtMessage {
        transaction: message.transaction.clone(),
        data: DhtMessageData::Response(DhtMessageResponse {
            response: DhtMessageResponseData::GetPeers(DhtMessageResponseGetPeers {
                id: bm.self_peer_id,
                token,
                data,
            }),
        }),
    }
}

fn handle_query_announce_peer(
    bm: &BucketManager,
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

    // FIXME: insert into tracker

    DhtMessage {
        transaction: message.transaction.clone(),
        data: DhtMessageData::Response(DhtMessageResponse {
            response: DhtMessageResponseData::GeneralId {
                id: bm.self_peer_id,
            },
        }),
    }
}

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_NAME: &str = env!("CARGO_PKG_NAME");

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


    let sock = UdpSocket::bind(&bind_address).await?;
    let mut buf = [0; 1024];

    let self_id = TorrentId(
        *b"\x88\xc8\xa2\xf2\x0a\x09\x0d\x7f\x80\xe5\xcd\xc7\x5c\x5c\x94\x2c\xc5\x2a\x61\x26",
    );
    let mut bm = BucketManager::new(self_id);
    let mut next_dump_state = Instant::now() + Duration::new(3, 0);

    // let mut recursion_state: Arc<Mutex<
    // let mut pending_tx: Arc<Mutex<HashMap<TransactionKey, Box<TransactionValue>>>> = Default::default();
    
    // #[derive(PartialEq, Eq, Hash)]
    // struct TransactionKey {
    //     pub transaction_id: [u8; TRANSACTION_SIZE_BYTES],
    //     pub peer_addr: SocketAddr,
    // }

    // struct TransactionValue {
    //     pub X: (),
    // }

    //
    // let next_work_at = [
    //     &next_dump_state,
    // ].iter().min().unwrap()
    // // let mut tracker: BTreeMap<TorrentId, Vec<TrackerEntry>> = BTreeMap::new();

    loop {
        bm.tick(&NodeEnvironment {
            now: Instant::now(),
            is_reply: false,
        });

        for b in &bm.buckets {
            //
        }

        let (len, addr) = sock.recv_from(&mut buf).await?;
        let rx_bytes = &buf[..len];

        event!(Level::TRACE, "{:?} bytes received from {:?}:", len, addr);
        event!(Level::TRACE, "    rust-bytes: {:?}", BinStr(rx_bytes));

        let v = match bencode::from_bytes::<dht::wire::DhtMessage>(rx_bytes) {
            Ok(v) => v,
            Err(err) => {
                event!(Level::TRACE, "    \x1b[31mdecoded(fail)\x1b[0m: {}", err);
                continue;
            }
        };
        event!(Level::TRACE, "    \x1b[32mdecoded(ok)\x1b[0m: {:?}", v);

        let environment = NodeEnvironment {
            now: Instant::now(),
            is_reply: false,
        };

        let mut peer_id = None;
        let mut response = None;
        match v.data {
            DhtMessageData::Query(DhtMessageQuery::Ping { id }) => {
                peer_id = Some(id);
                response = Some(handle_query_ping(&bm, &v));
            }
            DhtMessageData::Query(DhtMessageQuery::FindNode(ref find_node)) => {
                peer_id = Some(find_node.id);
                response = Some(handle_query_find_node(&bm, &v, find_node, &environment));
            }
            DhtMessageData::Query(DhtMessageQuery::GetPeers(ref gp)) => {
                peer_id = Some(gp.id);
                response = Some(handle_query_get_peers(&bm, &v, gp, &environment, &addr));
            }
            DhtMessageData::Query(DhtMessageQuery::AnnouncePeer(ref ap)) => {
                peer_id = Some(ap.id);
                response = Some(handle_query_announce_peer(&bm, &v, ap, &environment, &addr));
            }
            DhtMessageData::Query(DhtMessageQuery::SampleInfohashes(ref si)) => {
                peer_id = Some(si.id);
            }
            DhtMessageData::Query(DhtMessageQuery::Vote(ref vote)) => {
                peer_id = Some(vote.id);
            }
            _ => (),
        }

        if let Some(ref resp) = response {
            event!(Level::TRACE, "    response: {:?}", resp);
            let resp_bytes = bencode::to_bytes(resp).unwrap();
            event!(Level::TRACE, "    as-bytes: {:?}", BinStr(&resp_bytes));
            sock.send_to(&resp_bytes, addr).await?;
            event!(Level::TRACE, "    sent.");
        }

        // let cur_sys_time = Utc::now();
        // let now_sys_time = SystemTime::now();
        if let Some(pid) = peer_id {
            bm.add_node(
                Node {
                    peer_id: pid,
                    peer_addr: addr,
                    last_message_time: environment.now,
                    last_correct_reply_time: environment.now,
                    last_request_sent_time: environment.now,
                    pinged: 0,
                },
                ConfirmLevel::GotMessage,
                &environment,
            );
        }

        if next_dump_state < environment.now {
            next_dump_state = environment.now + Duration::new(30, 0);

            let mut node_count = 0;
            for b in bm.buckets.iter() {
                node_count += b.nodes.len();
            }
            event!(Level::INFO, "State (node_count={}):", node_count);
            for b in bm.buckets.iter() {
                let age = environment.now.duration_since(b.last_touched_time);

                event!(Level::INFO, 
                    "    bucket {}/{} (len={}, last_touched_time_age={:?})",
                    b.prefix.base.hex(),
                    b.prefix.prefix_len,
                    b.nodes.len(),
                    age
                );

                for n in &b.nodes {
                    event!(Level::INFO, 
                        "        record {} {:22} quality={:?}",
                        n.peer_id.hex(),
                        n.peer_addr,
                        n.quality(&environment),
                    );
                }
            }
        }
    }
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
