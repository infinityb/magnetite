use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use futures::future::FutureExt;
use metrics::{counter, timing};
use tokio::sync::{broadcast, mpsc};
use tonic::transport::Server;
use tracing::{event, Level};
use futures::stream::StreamExt;

use magnetite_common::TorrentId;

use crate::model::TorrentMetaWrapped;
use crate::CommonInit;

pub mod messages;
pub mod api {
    tonic::include_proto!("magnetite.api");
}

use self::api::{
    add_torrent_request::TorrentFile, torrent_host_server::TorrentHost, AddTorrentRequest,
    AddTorrentResponse, ListTorrentsRequest, ListTorrentsResponse, RemoveTorrentRequest,
    RemoveTorrentResponse, TorrentEntry,AddPeerRequest,
    AddPeerResponse,
    DisconnectPeerResponse,
    SubscribeStatusUpdatesRequest,
    SubscribeStatusUpdateResponse,
    DisconnectPeerRequest,
};
use self::messages::{BusPeerAdded, BusPeerDisconnect, BusSessionPeerBitfieldUpdate, BusAddTorrent, BusListTorrents, BusRemoveTorrent};

struct Control {
    sender: broadcast::Sender<crate::BusMessage>,
}

// on magnetite startup, we can send each system our event-bus and the sender of
// an mpsc channel.  The mpsc can be Sender<()> - we only care about closure.
// each system will close the channel once it's initialized and has subscribed
// to the event bus.  We'll wait for all systems to initialize before sending
// any data over the event bus.  This will be painful - there's a lot of refactoring
// to do here.
//
#[tonic::async_trait]
impl TorrentHost for Control {
    type SubscribeStatusUpdatesStream = mpsc::Receiver<Result<SubscribeStatusUpdateResponse, tonic::Status>>;

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
        let backing_file = req
            .backing_file
            .clone()
            .ok_or_else(|| tonic::Status::invalid_argument("backing_file is mandatory"))?;

        timing!("control.add_torrent_parse_time", parse_start.elapsed());

        let info_hash = torrent.info_hash;
        let torrent = Arc::new(torrent);

        let (tx, mut rx) = mpsc::channel(8);
        let bus_req: Box<dyn Any + Send + Sync> = Box::new(BusAddTorrent {
            torrent: torrent,
            backing_file: backing_file,
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

        let info_hash_buf = &request.get_ref().info_hash[..];
        let info_hash = TorrentId::from_slice(info_hash_buf)
            .map_err(|e| tonic::Status::invalid_argument("invalid info_hash"))?;

        let bus_req: Box<dyn Any + Send + Sync> = Box::new(BusRemoveTorrent {
            info_hash,
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

        while let Some(v) = rx.next().await {
            //
        }

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
        let bus_req: Box<dyn Any + Send + Sync> = Box::new(BusListTorrents { response: tx });

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

    async fn add_peer(
        &self,
        request: tonic::Request<AddPeerRequest>,
    ) -> Result<tonic::Response<AddPeerResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not yet implemented"))
    }

    async fn disconnect_peer(
        &self,
        request: tonic::Request<DisconnectPeerRequest>,
    ) -> Result<tonic::Response<DisconnectPeerResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not yet implemented"))
    }

    async fn subscribe_status_updates(
        &self,
        request: tonic::Request<SubscribeStatusUpdatesRequest>,
    ) -> Result<tonic::Response<Self::SubscribeStatusUpdatesStream>, tonic::Status> {
        use crate::control::api::{StatusUpdatePeerDisconnect, StatusUpdatePeerBitfieldUpdateElement, StatusUpdateNewPeer, StatusUpdatePeerBitfieldUpdate};
        use crate::control::api::subscribe_status_update_response::Event;

        let (mut tx, rx) = mpsc::channel(4);
        // let features = self.features.clone();

        let mut ebus_rx = self.sender.subscribe();
        tokio::spawn(async move {
            loop {
                let creq = match ebus_rx.recv().await {
                    Ok(creq) => creq,
                    Err(err) => {
                        let status = match err {
                            broadcast::RecvError::Closed => tonic::Status::aborted("shutting down"),
                            broadcast::RecvError::Lagged(..) => tonic::Status::resource_exhausted("lagged"),
                        };
                        if let Err(err) = tx.send(Err(status)).await {
                            event!(Level::ERROR, "send to subscriber: {}", err);
                        }
                        break;
                    }
                };

                let mut event = None;
                let mut session_id = Vec::new();
                if let Some(at) = creq.downcast_ref::<BusPeerAdded>() {
                    session_id = at.connect.session_id.as_bytes().to_vec();
                    event = Some(Event::NewPeer(StatusUpdateNewPeer {
                        socket_addr: unimplemented!(),
                        peer_id: unimplemented!(),
                        bitfield_data: unimplemented!(),
                    }));
                }
                if let Some(pbu) = creq.downcast_ref::<BusSessionPeerBitfieldUpdate>() {
                    let mut elements = Vec::new();

                    for v in &pbu.update {
                        elements.push(StatusUpdatePeerBitfieldUpdateElement {
                            piece_id: v.piece_id,
                            have: v.have_piece,
                        });
                    }

                    session_id = pbu.session_id.as_bytes().to_vec();
                    event = Some(Event::Update(StatusUpdatePeerBitfieldUpdate {
                        elements,
                    }));
                }

                if let Some(pbu) = creq.downcast_ref::<BusPeerDisconnect>() {
                    session_id = pbu.session_id.as_bytes().to_vec();
                    event = Some(Event::Disconnect(StatusUpdatePeerDisconnect {}));
                }

                if event.is_some() {
                    let resp = SubscribeStatusUpdateResponse { session_id, event };
                    if let Err(err) = tx.send(Ok(resp)).await {
                        event!(Level::ERROR, "send to subscriber: {}", err);
                        break;
                    }
                }
            }

            // tx.send(Err(tonic::Status::unimplemented("not yet implemented"))).await.unwrap();
            // https://github.com/hyperium/tonic/blob/master/examples/src/routeguide/server.rs#L45
        });

        Ok(tonic::Response::new(rx))
    }
}

pub async fn start_control_service(
    common: CommonInit,
    bind_addr: &str,
) -> Result<(), failure::Error> {
    event!(Level::INFO, "binding control socket {}", bind_addr);

    let CommonInit {
        ebus, mut term_sig, ..
    } = common;
    let server = Control { sender: ebus };

    let addr: SocketAddr = bind_addr.parse()?;

    let serve = Server::builder()
        .add_service(api::torrent_host_server::TorrentHostServer::new(server))
        .serve(addr)
        .boxed();

    use std::pin::Pin;
    let pinned_term_sig = Pin::new(&mut term_sig);
    if let Ok(serve_res) = pinned_term_sig.await_with(serve).await {
        serve_res?;
    }

    event!(Level::INFO, "shutdown control server");

    Ok(())
}
