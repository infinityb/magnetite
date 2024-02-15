use std::borrow::Cow;
use std::convert::Infallible;
use std::fmt::{self, Write};
use std::sync::Arc;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::time::{Instant, Duration};
use std::str::FromStr;

use clap::{App, Arg, arg, command, value_parser, ArgAction};
use hyper::server::Server;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response};
use hyper::{StatusCode, Method};
use percent_encoding::PercentDecode;
use serde::{Serialize, Deserialize};
use smallvec::SmallVec;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tracing::{event, Level};
use tracing::{instrument, error, info_span, info, warn, Instrument};
use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
use tracing_subscriber::FmtSubscriber;
use tracing_subscriber::prelude::*;

use bin_str::BinStr;
use magnetite_common::TorrentId;
use magnetite_tracker_lib::{Tracker, AnnounceCtx, TrackerSearch};

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_NAME: &str = env!("CARGO_PKG_NAME");

#[derive(Debug)]
enum Route {
    Debugging(RouteDebugging),
    Announce(RouteAnnounce),
    Scrape(RouteScrape),
}

#[derive(Debug)]
enum RouteDebugging {
    Sleep,
    Dump,
}

#[derive(Debug)]
struct RouteAnnounce {
    peer_address: SocketAddr,
    info_hash: TorrentId,
    peer_id: Option<TorrentId>,
    event: RouteAnnounceEvent,
    port: u16,
    // uploaded: i64,
    // downloaded: i64,left: i64,
    // numwant: i64,
    // key: String,
    // compact: bool,
    // supportcrypto: bool,
}

#[derive(Debug)]
struct RouteScrape {
    peer_address: SocketAddr,
    info_hash: TorrentId,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum RouteAnnounceEvent {
    Started,
    Completed,
    Update,
    Stopped,
}


#[derive(Clone, Debug)]
struct SharedState {
    state: Arc<Mutex<Tracker>>,
}

#[instrument(skip(tracker, req))]
async fn handle_debugging(
    tracker: Arc<Mutex<Tracker>>,
    req: Request<Body>,
    route: RouteDebugging,
) -> Result<Response<Body>, failure::Error> {
    match route {
        RouteDebugging::Sleep => {
            tokio::time::sleep(Duration::from_millis(500)).await;

            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain")
                .body("ZzZzzZzz".into())
                .expect("unable to build response"));
        },
        RouteDebugging::Dump => {
            return handle_debugging_dump(tracker).await;
        }
    }
}

async fn handle_debugging_dump(tracker: Arc<Mutex<Tracker>>) -> Result<Response<Body>, failure::Error> {
    let locked = tracker.lock().await;
    let v = locked.get_torrent_peers_display(&Instant::now());
    drop(locked);


    return Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain")
        .body(v.into())
        .expect("unable to build response"));
}


#[instrument(skip(tracker, req))]
async fn handle_announce(
    tracker: Arc<Mutex<Tracker>>,
    req: Request<Body>,
    route: RouteAnnounce,
) -> Result<Response<Body>, failure::Error> {
    let now = Instant::now();

    event!(Level::INFO,
        headers = ?req.headers(),
        uri = ?req.uri(),
        route = ?route,
        "magnetite-tracker-request-announce");


    let body = hyper::body::to_bytes(req.into_body())
        .instrument(info_span!("hyper-read-body"))
        .await?;

    event!(Level::INFO,
        body = ?BinStr(&body[..]),
        "magnetite-tracker-request-body");

    #[derive(Serialize, Deserialize)]
    struct AnnounceResponseCompact {
        complete: i64,
        downloaded: i64,
        incomplete: i64,
        interval: i64,
        #[serde(rename = "min interval")]
        min_interval: i64,
        #[serde(with = "serde_vec_socket_addr_v4_http")]
        peers: Vec<SocketAddrV4>,
    }

    let mut tracker_locked = tracker.lock().await;
    let mut peer_address = route.peer_address.clone();
    peer_address.set_port(route.port);

    let annctx = AnnounceCtx { now: Instant::now() };
    if route.event == RouteAnnounceEvent::Stopped {
        tracker_locked.remove_announce(
            &route.info_hash,
            &peer_address,
            &annctx,
            route.peer_id,
        );
    } else {
        tracker_locked.insert_announce(
            &route.info_hash,
            &peer_address,
            &annctx,
            route.peer_id,
        );
    }

    let mut peers = SmallVec::<[&SocketAddr; 16]>::new();
    tracker_locked.search_announce(&TrackerSearch {
        now: now,
        info_hash: route.info_hash,
        cookie: 0,
    }, &mut peers);
    
    let peers_copied = peers.into_iter().filter_map(|x| {
        match x {
            SocketAddr::V4(ref v4) => Some(SocketAddrV4::clone(v4)),
            _ => None,
        }
    }).collect::<Vec<SocketAddrV4>>();

    drop(tracker_locked);

    let bencout = bencode::to_bytes(&AnnounceResponseCompact {
        complete: 0,
        downloaded: 0,
        incomplete: peers_copied.len() as i64,
        interval: 1800,
        min_interval: 1800,
        peers: peers_copied,
    }).unwrap();

    event!(Level::INFO,
        body = ?BinStr(&bencout[..]),
        "magnetite-tracker-request-potential-response");

    // HTTP/1.1 200 OK
    // Server: openresty
    // Date: Sun, 14 Aug 2022 16:55:19 GMT
    // Content-Type: text/plain
    // Connection: close
    // 
    // d8:completei281e10:downloadedi865e10:incompletei0e8:intervali1800e12:min intervali1800e5:peers0:e

    return Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain")
        .body(bencout.into())
        .expect("unable to build response"));
}

#[instrument(skip(tracker, req))]
async fn handle_scrape(
    tracker: Arc<Mutex<Tracker>>,
    req: Request<Body>,
    route: RouteScrape,
) -> Result<Response<Body>, failure::Error> {
    event!(Level::INFO,
        headers = ?req.headers(),
        uri = ?req.uri(),
        route = ?route,
        "magnetite-tracker-request-scrape");

    let body_res = hyper::body::to_bytes(req.into_body())
        .instrument(info_span!("hyper-read-body"))
        .await;

    let body = match body_res {
        Ok(v) => v,
        Err(err) => {
            warn!("error reading body");

            return Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("bad body".into())
                .expect("unable to build response"));
        }
    };

    event!(Level::INFO,
        body = ?BinStr(&body[..]),
        "magnetite-tracker-request-body");

    // stub
    return Ok(Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body("Scrape not implemented".into())
        .expect("unable to build response"));
}

#[instrument(skip(tracker, req))]
async fn handle_root(
    tracker: Arc<Mutex<Tracker>>,
    peer_address: SocketAddr,
    req: Request<Body>,
) -> Result<Response<Body>, failure::Error> {
    event!(Level::INFO,
        peer_address = ?peer_address,
        headers = ?req.headers(),
        uri = ?req.uri(),
        "magnetite-tracker-request-announce");

    let mut known_route = None;
    if "/debugging/sleep" == req.uri().path() && req.method() == Method::GET {
        known_route = Some(Route::Debugging(RouteDebugging::Sleep));
    }
    if "/debugging/dump" == req.uri().path() && req.method() == Method::GET {
        known_route = Some(Route::Debugging(RouteDebugging::Dump));
    }
    if "/scrape" == req.uri().path() && req.method() == Method::GET {
        let mut info_hash = None;

        if let Some(pdec) = find_query_param_in_request(&req, "info_hash") {
            let buf: Cow<'_, [u8]> = pdec.into();
            info_hash = TorrentId::from_slice(&buf).ok();
        }

        known_route = Some(Route::Scrape(RouteScrape {
            peer_address,
            info_hash: info_hash.unwrap(),
        }));
    }
    if "/announce" == req.uri().path() && req.method() == Method::GET {
        let mut info_hash = None;
        let mut peer_id = None;
        let mut event = None;
        let mut port = 0;

        if let Some(pdec) = find_query_param_in_request(&req, "info_hash") {
            let buf: Cow<'_, [u8]> = pdec.into();
            info_hash = TorrentId::from_slice(&buf).ok();
        }
        if let Some(pdec) = find_query_param_in_request(&req, "peer_id") {
            let buf: Cow<'_, [u8]> = pdec.into();
            peer_id = TorrentId::from_slice(&buf).ok();
        }
        if let Some(pdec) = find_query_param_in_request(&req, "event") {
            let value = pdec.decode_utf8_lossy();
            event = match &value[..] {
                "" => Some(RouteAnnounceEvent::Update),
                "started" => Some(RouteAnnounceEvent::Started),
                "completed" => Some(RouteAnnounceEvent::Completed),
                "stopped" => Some(RouteAnnounceEvent::Stopped),
                _ => {
                    eprintln!("unknown event {:?}", value);
                    None
                }
            }
        } else {
            event = Some(RouteAnnounceEvent::Update);
        }
        if let Some(pdec) = find_query_param_in_request(&req, "port") {
            let value = pdec.decode_utf8_lossy();
            port = value.parse().ok().unwrap_or(0_u16);
        }

        if info_hash.is_some() && peer_id.is_some() && event.is_some() {
            known_route = Some(Route::Announce(RouteAnnounce {
                peer_address,
                info_hash: info_hash.unwrap(),
                // XXX: required?
                peer_id: Some(peer_id.unwrap()),
                event: event.unwrap(),
                port,
            }));
        }
    }

    if known_route.is_none() {
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("No route found".into())
            .expect("unable to build response"));
    }
    let known_route = known_route.unwrap();

    match known_route {
        Route::Announce(ann) => handle_announce(tracker, req, ann).await,
        Route::Scrape(scr) => handle_scrape(tracker, req, scr).await,
        Route::Debugging(debugging) => handle_debugging(tracker, req, debugging).await,
    }
}

fn unpack_ascii_query<'a>(query: &'a str) -> Option<(&'a str, PercentDecode<'a>)> {
    let mut parts = query.splitn(2, '=');
    let name = parts.next()?;
    let value = parts.next()?;
    Some((name, percent_encoding::percent_decode_str(value)))
}

fn find_query_param_in_request<'a>(req: &'a Request<Body>, qparam: &str) -> Option<PercentDecode<'a>> {
    if let Some(query) = req.uri().query() {
        for query_str in query.split("&") {
            if let Some((name, value)) = unpack_ascii_query(query_str) {
                if name == qparam {
                    return Some(value);
                }
            }
        }
    }
    None
}

fn decode_percents_bin<'a>(string: &'a str) -> std::borrow::Cow<'a, [u8]> {
    percent_encoding::percent_decode_str(string).into()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    let tracer = opentelemetry_jaeger::new_agent_pipeline().install_simple()?;

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("TRACE"))
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()
        .expect("Failed to register tracer with registry");
    let app = App::new(CARGO_PKG_NAME)
        .version(CARGO_PKG_VERSION)
        .author("Stacey Ell <software@e.staceyell.com>")
        .about("Basic bittorrent tracker implementation for testing")
        .arg(
            Arg::new("verbose")
                .short('v')
                .action(clap::ArgAction::Count)
                .help("Sets the level of verbosity"),
        )
        .arg(
            Arg::new("allow-all")
                .long("allow-all")
                .action(clap::ArgAction::SetTrue)
                .help("Allow any infohash to be announced")
        )
        .arg(
            Arg::new("bind-address")
                .long("bind-address")
                .value_name("SOCKET_ADDR")
                .help("The IP and TCP port to bind to")
                .default_value("[::1]:3000")
                .value_parser(<SocketAddr as FromStr>::from_str),
        );

    let matches = app.get_matches();
    let allow_all = matches.get_flag("allow-all");
    let verbosity = matches.get_count("verbose");
    let should_print_test_logging = 4 < verbosity;

    let mut my_subscriber_builder = FmtSubscriber::builder();
    my_subscriber_builder = my_subscriber_builder.with_max_level(match verbosity {
        0 => TracingLevelFilter::ERROR,
        1 => TracingLevelFilter::WARN,
        2 => TracingLevelFilter::INFO,
        3 => TracingLevelFilter::DEBUG,
        _ => TracingLevelFilter::TRACE,
    });

    // tracing::subscriber::set_global_default(my_subscriber_builder.finish())
    //     .expect("setting tracing default failed");

    if should_print_test_logging {
        print_test_logging();
    }

    if !allow_all {
        eprintln!("--allow-all is currently required since we don't have a mechanism to specify an allow-list");
        std::process::exit(1);
    }

    let rt = Runtime::new()?;
    rt.block_on(async {
        let tracker = Arc::new(Mutex::new(Tracker::new()));
        let addr: SocketAddr = matches.get_one::<SocketAddr>("bind-address").unwrap().clone();
        let make_svc = make_service_fn(move |conn: &hyper::server::conn::AddrStream| {
            let tracker = Arc::clone(&tracker);
            let raddr = conn.remote_addr();
            async move {
                let tracker = Arc::clone(&tracker);
                Ok::<_, Infallible>(service_fn(move |v| {
                    let tracker = Arc::clone(&tracker);
                    async move {
                        match handle_root(tracker, raddr, v).await {
                            Ok(v) => Result::<_, Infallible>::Ok(v),
                            Err(err) => {
                                error!("handler error: {:?}", err);
                                Ok(service_down_response())
                            }
                        }
                    }
                }))
            }.instrument(info_span!("spawn-service", raddr=%raddr))
        });
        let server = Server::bind(&addr).serve(make_svc);

        if let Err(e) = server.await {
            eprintln!("server error: {}", e);
        }
    });
    Ok(())
}

#[allow(clippy::cognitive_complexity)] // macro bug around event!()
fn print_test_logging() {
    event!(Level::TRACE, "logger initialized - trace check");
    event!(Level::DEBUG, "logger initialized - debug check");
    event!(Level::INFO, "logger initialized - info check");
    event!(Level::WARN, "logger initialized - warn check");
    event!(Level::ERROR, "logger initialized - error check");
}


mod serde_vec_socket_addr_v4_http {
    use magnetite_common::TorrentId;
    use std::net::SocketAddrV4;

    use serde::{Deserializer, Serializer};

    fn serialize_sock_addr_v4(into: &mut [u8; 6], v4: &SocketAddrV4) {
        let port = v4.port();
        let octets = v4.ip().octets();
        into[0..4].copy_from_slice(&octets[..]);
        into[4] = (port >> 8) as u8;
        into[5] = (port & 0xFF) as u8;
    }

    fn deserialize_sock_addr_v4(from: &[u8; 6]) -> SocketAddrV4 {
        let mut ip_octets = [0; 4];
        ip_octets.copy_from_slice(&from[..4]);
        let port = (u16::from(from[4]) << 8) + u16::from(from[5]);
        SocketAddrV4::new(ip_octets.into(), port)
    }

    struct AddressListVisitorV4;

    impl<'de> serde::de::Visitor<'de> for AddressListVisitorV4 {
        type Value = Vec<SocketAddrV4>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("an byte-aray where the length is a multiple of 6")
        }

        fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value.len() % 6 != 0 {
                return Err(E::custom(format!("bad length: {}", value.len())));
            }

            let mut out = Vec::new();
            for ch in value.chunks(26) {
                let mut buf: [u8; 6] = [0; 6];
                buf.copy_from_slice(&ch[..]);
                out.push(deserialize_sock_addr_v4(&buf));
            }

            Ok(out)
        }
    }

    pub fn serialize<S>(
        addrs: &Vec<SocketAddrV4>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut out = Vec::with_capacity(addrs.len() * 6);
        for a in addrs {
            let mut buf = [0; 6];
            serialize_sock_addr_v4(&mut buf, a);
            out.extend(&buf[..]);
        }
        serializer.serialize_bytes(&out)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<SocketAddrV4>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(AddressListVisitorV4)
    }
}

fn service_down_response() -> Response<Body> {
    let builder = hyper::Response::builder().status(StatusCode::INTERNAL_SERVER_ERROR);
    let response = builder.body("Service Unavailable".into()).unwrap();
    response
}
