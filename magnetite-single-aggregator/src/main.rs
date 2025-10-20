use std::io::Read;
use std::fmt;
use std::sync::Arc;
use std::collections::HashMap;

use tonic::{Response, Request, Status};
use tonic::transport::Server;
use svix_ksuid::{Ksuid, KsuidLike};
use tracing::{event, Level};
use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
use tracing_subscriber::FmtSubscriber;
use clap::{Arg, ArgAction, value_parser};
use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
use tokio_postgres::tls::NoTls;
// use hyper::client::Client;

use serde_json;

use magnetite_common::TorrentId;
use magnetite_model::{TorrentMetaWrapped, BitField};
use magnetite_single_api::proto::magnetite_server::{Magnetite, MagnetiteServer};
use magnetite_single_api::proto;
use magnetite_single_api::tokio::sync::{Mutex, Notify};
use magnetite_single_api::tokio::time::{sleep, Duration};
use magnetite_single_api::tokio_stream::wrappers::ReceiverStream;
use magnetite_single_api::TorrentSessionStartItem;
use magnetite_single_api::{futures, tokio};

refinery::embed_migrations!("migrations");

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

    println!("{}", crate::migrations::V1__initialise::migration());

    let mut my_subscriber_builder = FmtSubscriber::builder()
        // .with_ansi(false)
        .json()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL);
        //.pretty();

    let app = clap::Command::new(CARGO_PKG_NAME)
        .version(CARGO_PKG_VERSION)
        .author("Stacey Ell <stacey.ell@gmail.com>")
        .about("Magnetite Daemon")
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

    let mag = MagnetiteService {
        pgdsn: "host=localhost user=sell dbname=magnetite".to_string().into_boxed_str().into(),
        self_peer_id: "3bd2d8c746fe2d2de11c5329cdb6878ac2964d4f".parse().unwrap(),
    };

    let (mut client, connection) = tokio_postgres::connect(&mag.pgdsn, NoTls).await?;
    let spawn_completion = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let report = migrations::runner().run_async(&mut client).await?;
    event!(Level::INFO, "migrations run: {:?}", report);

    drop(client);
    spawn_completion.await?;
    eprintln!("migration complete - starting app.");

    let addr = "[::1]:10000".parse().unwrap();
    let svc = MagnetiteServer::new(mag);
    Server::builder().add_service(
        svc.max_encoding_message_size(16 * 1024 * 1024)
           .max_decoding_message_size(16 * 1024 * 1024)
    ).serve(addr).await?;
    Ok(())
}

#[derive(PartialEq, Eq, Hash, Debug)]
enum TorrentServiceState {
    Offline,
    Active,
    Stopping,
}


#[derive(Debug)]
struct TorrentServiceStatics {
    state_changed: Arc<Notify>,
    data_path: String,
    data: Box<[u8]>,
}

#[derive(Debug)]
struct TorrentService {
    statics: Arc<TorrentServiceStatics>,
    state_active: TorrentServiceState,
}

#[derive(Debug, Clone)]
struct MagnetiteService {
    self_peer_id: TorrentId,
    pgdsn: Arc<str>,
    // torrents: Arc<Mutex<HashMap<TorrentId, Arc<Mutex<TorrentService>>>>>,
}

#[derive(Debug)]
struct AddTorrentRequestParsed {
    info_hash: TorrentId,
    torrent_data: Box<[u8]>,
    torrent_meta: TorrentMetaWrapped,
    file_path: String,
}

pub trait TryFromProto<T>: Sized {
    fn try_from_proto(value: &T) -> Result<Self, Status>;
}

pub trait TryIntoProto: Sized {
    type Proto;

    fn try_into_proto(&self) -> Result<Self::Proto, Status>;
}

impl TryFromProto<proto::AddTorrentRequest> for AddTorrentRequestParsed {
    fn try_from_proto(value: &proto::AddTorrentRequest) -> Result<Self, Status> {
        let info_hash: TorrentId = value.info_hash.parse()
            .map_err(|e| Status::invalid_argument(format!("badly formed info_hash: {}", e)))?;

        let torrent_meta = TorrentMetaWrapped::from_bytes(&value.torrent_data)
            .map_err(|e| Status::invalid_argument(format!("badly formed torrent_data: {}", e)))?;

        if torrent_meta.info_hash != info_hash {
            return Err(Status::invalid_argument("info_hash mismatch"));
        }

        Ok(AddTorrentRequestParsed {
            info_hash: info_hash,
            torrent_data: value.torrent_data.clone().into_boxed_slice(),
            torrent_meta: torrent_meta,
            file_path: value.file_path.clone(),
        })
    }
}

#[derive(Debug)]
struct ChangeTorrentStatusRequestParsed {
    info_hash: TorrentId,
    set_active: bool,
}

impl TryFromProto<proto::ChangeTorrentStatusRequest> for ChangeTorrentStatusRequestParsed {
    fn try_from_proto(value: &proto::ChangeTorrentStatusRequest) -> Result<Self, Status> {
        let info_hash: TorrentId = value.info_hash.parse()
            .map_err(|e| Status::invalid_argument(format!("badly formed info_hash: {}", e)))?;

        Ok(ChangeTorrentStatusRequestParsed {
            info_hash: info_hash,
            set_active: value.set_active,
        })
    }
}

struct ChangeTorrentStatusResponseParsed {}

impl TryIntoProto for ChangeTorrentStatusResponseParsed {
    type Proto = proto::ChangeTorrentStatusResponse;

    fn try_into_proto(&self) -> Result<Self::Proto, Status> {
        Ok(proto::ChangeTorrentStatusResponse {})
    }
}

struct AddTorrentResponseParsed {}

impl TryIntoProto for AddTorrentResponseParsed {
    type Proto = proto::AddTorrentResponse;

    fn try_into_proto(&self) -> Result<Self::Proto, Status> {
        Ok(proto::AddTorrentResponse {})
    }
}


pub async fn get_database_handle<T>(
    config: &str,
    tls: T,
) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<Result<(), Status>>), Status>
where
    T: tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket>,
    <T as tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket>>::Stream: std::marker::Send + 'static
{
    let (mut client, connection) = tokio_postgres::connect(&config, tls)
        .await
        .map_err(|e| {
            Status::unavailable(format!("{}", e))
        })?;

    let connection_fut = tokio::spawn(async move {
        connection.await.map_err(|e| {
            Status::unavailable(format!("{}", e))
        })
    });

    Ok((client, connection_fut))
}

#[derive(Debug)]
struct StatusError(Status);

impl std::error::Error for StatusError {}

impl fmt::Display for StatusError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

async fn database_add_torrent(
    client: &mut tokio_postgres::Client,
    request: Request<proto::AddTorrentRequest>,
    parsed: &AddTorrentRequestParsed,
) -> Result<AddTorrentResponseParsed, failure::Error> {
    let maybe_rec = client.query_opt("
        SELECT id FROM torrent
        WHERE info_hash = $1
    ", &[
        &request.get_ref().info_hash
    ]).await?;

    if maybe_rec.is_some() {
        return Err(StatusError(Status::already_exists("info_hash already added")).into());
    }

    let empty = serde_json::json!({});
    let info_data = TorrentMetaWrapped::get_info_bytes(&request.get_ref().torrent_data[..])?;
    let maybe_rec = client.query_opt("
        INSERT INTO torrent
        (info_hash, info_data, annotations, state)
        VALUES
        ($1, $2, $3, $4)
    ", &[
        &request.get_ref().info_hash,
        &info_data,
        &empty,
        &"",
    ]).await?;

    // let mut locked = self.torrents.lock().await;
    // locked.insert(torrent.info_hash, Arc::new(Mutex::new(TorrentService {
    //     statics: Arc::new(TorrentServiceStatics {
    //         state_changed: Arc::new(Notify::new()),
    //         data_path: req_body.file_path.clone(),
    //         data: req_body.torrent_data.clone().into_boxed_slice(),
    //     }),
    //     state_active: TorrentServiceState::Offline,
    // })));

   
    Ok(AddTorrentResponseParsed {})
}

#[tonic::async_trait]
impl Magnetite for MagnetiteService {
    type StartSessionStream = ReceiverStream<Result<proto::TorrentSessionStartItem, Status>>;

    async fn start_session(&self, request: Request<proto::TorrentSessionStartRequest>)
        -> Result<Response<Self::StartSessionStream>, Status>
    {
        //

        Err(Status::unimplemented("unimplemented"))
        // let req_body = request.get_ref();
        // let info_hash: TorrentId = req_body.info_hash.parse()
        //     .map_err(|e| Status::invalid_argument(format!("badly formed info_hash: {}", e)))?;

        // let torrent_data;
        // let torrent_statics;
        // let torrent;
        // let torrent_service;
        // {
        //     let mut locked = self.torrents.lock().await;
        //     torrent_service = locked.get_mut(&info_hash)
        //         .ok_or_else(|| Status::invalid_argument("info_hash not known"))
        //         .map(|e| Arc::clone(e))
        //         ?;
        //     drop(locked);

        //     let service = torrent_service.lock().await;
        //     torrent_statics = Arc::clone(&service.statics);
        //     torrent_data = service.statics.data.clone();
        //     torrent = TorrentMetaWrapped::from_bytes(&service.statics.data)
        //          .map_err(|e| Status::internal(format!("stored torrent invalid!: {}", e)))?;
        // }

        // let session_id = Ksuid::new(None, None);
        // event!(Level::INFO,
        //     session_id = %session_id,
        //     info_hash = %req_body.info_hash,
        //     torrent_info_name = %torrent.meta.info.name,
        //     request_metadata = ?request.metadata(),
        //     "start_session",
        // );

        // let (tx, rx) = tokio::sync::mpsc::channel(1);
        // let this: MagnetiteService = self.clone();
        // tokio::spawn(async move {
        //     use proto::torrent_session_start_item::Item;

        //     let bitfield = BitField::all(torrent.meta.info.pieces.len() as u32);   
        //     let bfvec = bitfield.data.iter().cloned().collect();
        //     tx.send(Ok(proto::TorrentSessionStartItemInit {
        //         session_id: session_id.to_string(),
        //         replace_bitfield: bfvec,
        //         peer_id: this.self_peer_id.hex().to_string(),
        //         torrent_data: torrent_data.clone().into(),
        //         file_path: torrent_statics.data_path.clone(),
        //     }.into())).await?;

        //     let mut offset = 0;
        //     loop {
        //         offset += 1;
        //         tx.send(Ok(TorrentSessionStartItem {
        //             item: Item::SetSeedPermitBytes(offset * 1024 * 1024 * 1024),
        //         }.into())).await?;

        //         let sleeper = sleep(Duration::from_secs(1800));
        //         tokio::pin!(sleeper);
        //         tokio::select! {
        //             _ = &mut sleeper, if !sleeper.is_elapsed() => {
        //                 continue;
        //             }
        //             _ = tx.closed() => {
        //                 break;
        //             }
        //         }
        //     }

        //     eprintln!("broken pope -X-X-X-X");
        //     Result::<(), failure::Error>::Ok(())
        // });

        // let resp = Response::new(ReceiverStream::new(rx));
        // Ok(resp)
    }

    async fn notify_finalize_piece(&self, _request: Request<proto::FinalizePieceRequest>)
        -> Result<Response<proto::FinalizePieceResponse>, Status>
    {
        Err(Status::unimplemented("TODO"))
    }

    async fn update_session_statistics(&self, _request: Request<proto::SessionStatisticsUpdateRequest>)
        -> Result<Response<proto::SessionStatisticsUpdateResponse>, Status>
    {
        Err(Status::unimplemented("TODO"))
    }

    async fn add_torrent(&self, request: Request<proto::AddTorrentRequest>)
        -> Result<Response<proto::AddTorrentResponse>, Status>
    {
        let (mut client, connection) = get_database_handle(&self.pgdsn, NoTls).await?;
        let parsed = TryFromProto::try_from_proto(request.get_ref())?;
        let out = database_add_torrent(&mut client, request, &parsed)
            .await
            .map_err(|e| {
                Status::unavailable(format!("database operation error: {}", e))
            })?;
        connection.await.map_err(|e| {
            Status::unavailable(format!("database connection error: {}", e))
        })??;
        Ok(Response::new(out.try_into_proto()?))
    }

    async fn change_torrent_status(&self, request: Request<proto::ChangeTorrentStatusRequest>)
        -> Result<Response<proto::ChangeTorrentStatusResponse>, Status>
    {
        let (mut client, connection) = get_database_handle(&self.pgdsn, NoTls).await?;
        let parsed: ChangeTorrentStatusRequestParsed = TryFromProto::try_from_proto(request.get_ref())?;
        
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // need to spawn management task.. todo..

        let _rows_affected = client.execute(
            "UPDATE torrent SET active = $2 WHERE info_hash = $1",
            &[&parsed.info_hash.hex().to_string(), &parsed.set_active]
        ).await
            .map_err(|e| Status::internal(format!("Database error: {}", e)))?;

        let out = ChangeTorrentStatusResponseParsed {};
        Ok(Response::new(out.try_into_proto()?))
    }
}

#[allow(clippy::cognitive_complexity)] // macro bug around event!()
fn print_test_logging() {
    event!(Level::TRACE, "logger initialized - trace check");
    event!(Level::DEBUG, "logger initialized - debug check");
    event!(Level::INFO, "logger initialized - info check");
    event!(Level::WARN, "logger initialized - warn check");
    event!(Level::ERROR, "logger initialized - error check");
}

