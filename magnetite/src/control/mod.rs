use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::StreamExt;
use metrics::{counter, timing};
use tokio::sync::{broadcast, mpsc};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{event, Level};

use magnetite_common::TorrentId;

use crate::model::TorrentMetaWrapped;

pub mod messages;
pub mod api {
    tonic::include_proto!("magnetite.api");
}

use self::api::{
    add_torrent_request::TorrentFile,
    torrent_host_server::{TorrentHost, TorrentHostServer},
    AddTorrentRequest, AddTorrentResponse, ListTorrentsRequest, ListTorrentsResponse,
    RemoveTorrentRequest, RemoveTorrentResponse, TorrentEntry,
};
use self::messages::{BusAddTorrent, BusListTorrents, BusRemoveTorrent};

struct Control {
    sender: broadcast::Sender<crate::BusMessage>,
}

// on magnetite startup, we can send each system our event-bus and the sender of
// an mpsc channel.  The mpsc can be Sender<()> - we only care about closure.
// each system will close the channel once it's initialized and has subscribed
// to the event bus.  We'll wait for all systems to initialize before sending
// any data over the event bus.  This will be painful - there's a lot of refactoring
// to do here.

#[tonic::async_trait]
impl TorrentHost for Control {
    async fn add_torrent(
        &self,
        request: tonic::Request<AddTorrentRequest>,
    ) -> Result<tonic::Response<AddTorrentResponse>, tonic::Status> {
        counter!("magnetite_control.add_torrent_requests", 1);

        let req = request.get_ref();

        let parse_start = Instant::now();
        let torrent = match req.torrent_file {
            Some(TorrentFile::Bytes(ref data)) => TorrentMetaWrapped::from_bytes(data)
                .map_err(|e| tonic::Status::invalid_argument(format!("{}", e)))?,
            Some(TorrentFile::Uri(..)) => {
                return Err(tonic::Status::unimplemented("pending"));
            }
            None => return Err(tonic::Status::invalid_argument("torrent_file is mandatory")),
        };
        timing!("control.add_torrent_parse_time", parse_start.elapsed());

        let info_hash = torrent.info_hash;
        let torrent = Arc::new(torrent);

        let (tx, mut rx) = mpsc::channel(8);
        let bus_req: Box<Any + Send + Sync> = Box::new(BusAddTorrent {
            torrent: torrent,
            response: tx,
        });

        let resolve_start = Instant::now();
        self.sender.send(bus_req.into()).map_err(|e| {
            // XXX/FIXME: re-assess the FailedPrecondition/Aborted/Unavailable
            // when we're closer to our desired architecture.  Currently, this
            // seems correct to me - it will happen if none of the systems are
            // online, either by slow startup (?) or pending shutdown (?).
            tonic::Status::aborted("no service")
        })?;

        while let Some(v) = rx.next().await {}

        timing!("control.add_torrent_resolve_time", resolve_start.elapsed());

        Ok(tonic::Response::new(AddTorrentResponse {
            info_hash: info_hash.into(),
        }))
    }

    async fn remove_torrent(
        &self,
        request: tonic::Request<RemoveTorrentRequest>,
    ) -> Result<tonic::Response<RemoveTorrentResponse>, tonic::Status> {
        counter!("magnetite_control.remove_torrent_requests", 1);

        let (tx, mut rx) = mpsc::channel(8);
        let bus_req: Box<Any + Send + Sync> = Box::new(BusRemoveTorrent { response: tx });

        let resolve_start = Instant::now();
        self.sender.send(bus_req.into()).map_err(|e| {
            // XXX/FIXME: re-assess the FailedPrecondition/Aborted/Unavailable
            // when we're closer to our desired architecture.  Currently, this
            // seems correct to me - it will happen if none of the systems are
            // online, either by slow startup (?) or pending shutdown (?).
            tonic::Status::aborted("no service")
        })?;

        while let Some(v) = rx.next().await {}

        timing!(
            "control.remove_torrent_resolve_time",
            resolve_start.elapsed()
        );

        Ok(tonic::Response::new(RemoveTorrentResponse { /* empty */ }))
    }

    async fn list_torrents(
        &self,
        request: tonic::Request<ListTorrentsRequest>,
    ) -> Result<tonic::Response<ListTorrentsResponse>, tonic::Status> {
        counter!("control.list_torrents_requests", 1);

        let (tx, mut rx) = mpsc::channel(8);
        let bus_req: Box<Any + Send + Sync> = Box::new(BusListTorrents { response: tx });

        let resolve_start = Instant::now();
        self.sender.send(bus_req.into()).map_err(|e| {
            // XXX/FIXME: re-assess the FailedPrecondition/Aborted/Unavailable
            // when we're closer to our desired architecture.  Currently, this
            // seems correct to me - it will happen if none of the systems are
            // online, either by slow startup (?) or pending shutdown (?).
            tonic::Status::aborted("no service")
        })?;

        let mut entries = Vec::new();
        while let Some(v) = rx.next().await {
            entries.extend(v.into_iter());
        }

        timing!(
            "control.list_torrents_resolve_time",
            resolve_start.elapsed()
        );

        Ok(tonic::Response::new(ListTorrentsResponse { entries }))
    }
}

pub async fn start_control_service(
    ebus: broadcast::Sender<crate::BusMessage>,
    bind_addr: &str,
) -> Result<(), failure::Error> {
    let mut mock_ebus_rx = ebus.subscribe();
    tokio::task::spawn(async move {
        loop {
            let req = match mock_ebus_rx.recv().await {
                Ok(v) => v,
                Err(broadcast::RecvError::Closed) => break,
                Err(broadcast::RecvError::Lagged(v)) => {
                    event!(Level::WARN, "mock ebus responder lagged {}", v);
                    continue;
                }
            };

            if let Some(blt) = req.downcast_ref::<BusListTorrents>() {
                let mut blt: BusListTorrents = blt.clone();
                drop(req);

                tokio::task::spawn(async move {
                    let _ = blt
                        .response
                        .send(vec![TorrentEntry {
                            info_hash: TorrentId::from([
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                            ])
                            .into(),
                            name: "collection #0".to_string(),
                            size_bytes: 100000,
                        }])
                        .await;
                });
            }
        }
    });

    let mut mock_ebus_rx = ebus.subscribe();
    tokio::task::spawn(async move {
        loop {
            let req = match mock_ebus_rx.recv().await {
                Ok(v) => v,
                Err(broadcast::RecvError::Closed) => break,
                Err(broadcast::RecvError::Lagged(v)) => {
                    event!(Level::WARN, "mock ebus responder lagged {}", v);
                    continue;
                }
            };
            if let Some(blt) = req.downcast_ref::<BusListTorrents>() {
                let mut blt: BusListTorrents = blt.clone();
                drop(req);

                tokio::time::delay_for(Duration::from_millis(600)).await;

                tokio::task::spawn(async move {
                    let _ = blt
                        .response
                        .send(vec![TorrentEntry {
                            info_hash: TorrentId::from([
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
                            ])
                            .into(),
                            name: "collection #1".to_string(),
                            size_bytes: 900000,
                        }])
                        .await;
                });
            }
        }
    });

    event!(Level::INFO, "binding control socket {}", bind_addr);

    let server = Control { sender: ebus };
    let addr: SocketAddr = bind_addr.parse()?;

    let serve = Server::builder()
        .add_service(api::torrent_host_server::TorrentHostServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
