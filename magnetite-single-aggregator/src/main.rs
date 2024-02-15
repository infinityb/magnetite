use std::fs::File;
use std::io::Read;
use std::fmt;

use tonic::{Response, Request, Status};
use tonic::transport::Server;
use failure::ResultExt;
use svix_ksuid::{Ksuid, KsuidLike};
use tracing::{span, event, Level};
use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
use tracing_subscriber::FmtSubscriber;
use clap::{Arg, ArgAction, value_parser};
use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
// use hyper::client::Client;

use magnetite_common::TorrentId;
use magnetite_model::{TorrentMetaWrapped, BitField};
use magnetite_single_api::proto::magnetite_server::{Magnetite, MagnetiteServer};
use magnetite_single_api::proto::torrent_session_start_item;
use magnetite_single_api::proto;
use magnetite_single_api::tokio::sync::Mutex;
use magnetite_single_api::tokio::time::{sleep, Duration};
use magnetite_single_api::tokio_stream::wrappers::ReceiverStream;
use magnetite_single_api::TorrentSessionStartItem;
use magnetite_single_api::{futures, tokio};

#[derive(Debug)]
// XXX: duplicated
pub struct UnknownInfoHash(TorrentId);

impl fmt::Display for UnknownInfoHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UnknownInfoHash({:?})", self.0)
    }
}

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_NAME: &str = env!("CARGO_PKG_NAME");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let mut my_subscriber_builder = FmtSubscriber::builder()
        // .with_ansi(false)
        .json()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL);
        //.pretty();

    let app = clap::Command::new(CARGO_PKG_NAME)
        .version(CARGO_PKG_VERSION)
        .author("Stacey Ell <stacey.ell@gmail.com>")
        .about("Demonstration Torrent Seeder")
        // .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(
            Arg::new("v")
                .short('v')
                .action(ArgAction::Count)
                .value_parser(value_parser!(u8).range(..5))
                .help("Sets the level of verbosity"),
        );

    let matches = app.get_matches();
    let verbosity = matches.get_count("v");
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


    let addr = "[::1]:10000".parse().unwrap();
    let svc = MagnetiteServer::new(MagnetiteService {
        self_peer_id: "3bd2d8c746fe2d2de11c5329cdb6878ac2964d4f".parse().unwrap(),
    });
    Server::builder().add_service(
        svc.max_encoding_message_size(16 * 1024 * 1024)
           .max_decoding_message_size(16 * 1024 * 1024)
    ).serve(addr).await?;
    Ok(())
}

#[derive(PartialEq, Eq, Hash)]
enum TorrentServiceState {
    Offline,
    Active,
    Stopping,
}

struct TorrentService {
    state_changed: Arc<Notify>,
    state_active: TorrentServiceState,
    data: Box<[u8]>,
}

#[derive(Debug, Clone)]
struct MagnetiteService {
    self_peer_id: TorrentId,
    torrents: Arc<Mutex<HashMap<TorrentId, Arc<Mutex<TorrentService>>>>>,
}

#[tonic::async_trait]
impl Magnetite for MagnetiteService {
    type StartSessionStream = ReceiverStream<Result<proto::TorrentSessionStartItem, Status>>;

    async fn start_session(&self, request: Request<proto::TorrentSessionStartRequest>)
        -> Result<Response<Self::StartSessionStream>, Status>
    {
        let req_body = request.get_ref();
        let info_hash: TorrentId = req_body.info_hash.parse()
            .map_err(|e| Status::invalid_argument("badly formed info_hash"))?;
        let torrent = load_torrent_meta_file(&info_hash).await
            .map_err(|e| {
                // XXX: bad error mapping
                eprintln!("failed to start {}: {}", req_body.info_hash, e);
                Status::unknown(format!("XXX: {e}"))
            })?;

        let session_id = Ksuid::new(None, None);
        event!(Level::INFO,
            session_id = %session_id,
            info_hash = %req_body.info_hash,
            torrent_info_name = %torrent.meta.info.name,
            request_metadata = ?request.metadata(),
            "start_session",
        );

        let (mut tx, rx) = tokio::sync::mpsc::channel(1);
        let this: MagnetiteService = self.clone();
        tokio::spawn(async move {
            use proto::torrent_session_start_item::Item;

            let bitfield = BitField::all(torrent.meta.info.pieces.len() as u32);   
            let bfvec = bitfield.data.iter().cloned().collect();
            tx.send(Ok(proto::TorrentSessionStartItemInit {
                session_id: session_id.to_string(),
                replace_bitfield: bfvec,
                peer_id: this.self_peer_id.hex().to_string(),
            }.into())).await?;

            let mut offset = 0;
            loop {
                offset += 1;
                tx.send(Ok(TorrentSessionStartItem {
                    item: Item::SetSeedPermitBytes(offset * 1024 * 1024 * 1024),
                }.into())).await?;

                let sleeper = sleep(Duration::from_secs(1800));
                tokio::pin!(sleeper);
                tokio::select! {
                    _ = &mut sleeper => {
                        continue;
                    }
                    _ = tx.closed() => {
                        break;
                    }
                }
            }

            eprintln!("broken pope -X-X-X-X");
            Result::<(), failure::Error>::Ok(())
        });

        let mut resp = Response::new(ReceiverStream::new(rx));
        Ok(resp)
    }

    async fn notify_finalize_piece(&self, request: Request<proto::FinalizePieceRequest>)
        -> Result<Response<proto::FinalizePieceResponse>, Status>
    {
        Err(Status::unimplemented("TODO"))
    }

    async fn update_session_statistics(&self, request: Request<proto::SessionStatisticsUpdateRequest>)
        -> Result<Response<proto::SessionStatisticsUpdateResponse>, Status>
    {
        Err(Status::unimplemented("TODO"))
    }

    async fn add_torrent(&self, request: Request<proto::AddTorrentRequest>)
        -> Result<Response<proto::AddTorrentResponse>, Status>
    {
        let req_body = request.get_ref();
        let info_hash: TorrentId = req_body.info_hash.parse()
            .map_err(|e| Status::invalid_argument("badly formed info_hash"))?;
        let torrent = TorrentMetaWrapped::from_bytes(&req_body.torrent_data)
            .map_err(|e| Status::invalid_argument("badly formed torrent_data"))?;

        let mut locked = self.torrents.lock().await;
        locked.insert(torrent.info_hash, TorrentService{
            state_active: false,
            data: req_body.torrent_data
        });
        Ok(Response::new(proto::AddTorrentResponse {}))
    }

    async fn change_torrent_status(&self, request: Request<proto::ChangeTorrentStatusRequest>)
        -> Result<Response<proto::ChangeTorrentStatusResponse>, Status>
    {
        let req_body = request.get_ref();
        let info_hash: TorrentId = req_body.info_hash.parse()
            .map_err(|e| Status::invalid_argument("badly formed info_hash"))?;

        let mut locked = self.torrents.lock().await;
        let service = locked.get_mut(&info_hash).ok_or_else(|| Status::invalid_argument("info_hash not known"))?;
        let mut do_start_task = false;
        if req_body.set_active {
            match service.state_active {
                TorrentServiceState::Stopping => {
                    return Err(Status::failed_precondition("currently stopping torrent, cannot start"));
                }
                TorrentServiceState::Active => {
                    return Ok(Response::new(proto::AddTorrentResponse {}));
                }
                TorrentServiceState::Stopped => {
                    do_start_task = true;
                }
            }
        } else {
            if service.state_active == TorrentServiceState::Active {
                service.state_active = TorrentServiceState::Stopping;
                service.state_changed.notify();
            }
            if service.state_active == TorrentServiceState::Stopping {
                return Ok(Response::new(proto::AddTorrentResponse {}));
            }
        }
        if do_start_task {
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = notify2.notified() => {
                            //
                        }
                    }
                }
            });
        }
    }
}

// "/announce?info_hash=<PHEX>&peer_id=<PHEX>&event=started"
//         event!(Level::WARN,
//             announce = %torrent.meta.announce,
//             "add_torrent.meta");

//         let url = format!("{}?info_hash={}&peer_id={}&event=started&port={}",
//             torrent.meta.announce,
//             percent_encode(info_hash.as_bytes(), NON_ALPHANUMERIC),
//             percent_encode(self.self_peer_id.as_bytes(), NON_ALPHANUMERIC),
//             51409,
//         );

//         event!(Level::WARN,
//             announce_url = url,
//             "add_torrent.meta2");

//         let bytes = reqwest::get(&url)
//             .await.map_err(|e| Status::invalid_argument("announce error"))?
//             .bytes()
//             .await.map_err(|e| Status::invalid_argument("announce error"))?;


//         let interval = 120;
//         tokio::spawn(async move {
//             // let client = Client::builder()
//             //     .pool_idle_timeout(Duration::from_secs(30))
//             //     .build_http();

//             // client.get(Uri::from_static("http://httpbin.org/ip"));

//             loop {
//                 sleep(Duration::from_secs(interval)).await;
//             }
//         });

//         Ok(Response::new(proto::AddTorrentResponse {}))
//     }
// }

async fn open_torrent_meta_file(info_hash: &TorrentId) -> Result<File, failure::Error> {
    let span = span!(Level::INFO, "open_torrent_meta_file");
    let _guard = span.enter();

    let filename = format!("/storage/users/sell/ceph-transmission-backup/torrent-hive/torrents/{}.torrent", info_hash.hex());
    if let Ok(file) = File::open(&filename) {
        return Ok(file);
    }

    let filename = format!("/mnt/media/torrents/{}.torrent", info_hash.hex());
    let file = File::open(&filename).with_context(|_| {
        format!("opening {:?}", filename)
    })?;

    event!(Level::WARN,
        info_hash = %info_hash.hex(),
        "open_torrent_meta_file.fallback");

    Ok(file)
}

async fn load_torrent_meta_file(info_hash: &TorrentId) -> Result<TorrentMetaWrapped, failure::Error> {
    let mut file = open_torrent_meta_file(info_hash).await.with_context(|_| {
        UnknownInfoHash(*info_hash)
    })?;
    
    let mut by = Vec::new();
    file.read_to_end(&mut by)?;

    let torrent = TorrentMetaWrapped::from_bytes(&by)?;
    event!(Level::ERROR,
        name = torrent.meta.info.name,
        piece_count = torrent.meta.info.pieces.len(),
        "load_torrent_meta");

    if !torrent.meta.info.files.is_empty() {
        failure::bail!("multi-torrent not yet supported");
    }

    Ok(torrent)
}


#[allow(clippy::cognitive_complexity)] // macro bug around event!()
fn print_test_logging() {
    event!(Level::TRACE, "logger initialized - trace check");
    event!(Level::DEBUG, "logger initialized - debug check");
    event!(Level::INFO, "logger initialized - info check");
    event!(Level::WARN, "logger initialized - warn check");
    event!(Level::ERROR, "logger initialized - error check");
}

