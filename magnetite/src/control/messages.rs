use std::sync::Arc;
use std::net::SocketAddr;

use tokio::sync::mpsc;
use bytes::BytesMut;
use smallvec::SmallVec;
use ksuid::Ksuid;

use super::TorrentEntry;
use crate::control::api::{AddPeerRequest, add_torrent_request::BackingFile};
use crate::model::{MagnetiteError, TorrentMetaWrapped};
use crate::storage::{GetPieceRequestChannel, PieceStorageEngineDumb};
use crate::model::proto::Handshake;


use magnetite_common::TorrentId;

#[derive(Clone)]
pub struct BusListTorrents {
    pub response: mpsc::Sender<Vec<TorrentEntry>>,
}

// --

#[derive(Clone)]
pub struct BusAddTorrent {
    pub torrent: Arc<TorrentMetaWrapped>,
    pub backing_file: BackingFile,
    pub response: mpsc::Sender<Result<(), MagnetiteError>>,
}

// --

#[derive(Clone)]
pub struct BusRemoveTorrent {
    pub info_hash: TorrentId,
    pub response: mpsc::Sender<Result<(), MagnetiteError>>,
}

// --

#[derive(Clone)]
pub struct BusFindPieceFetcher {
    response: mpsc::Sender<Arc<dyn PieceStorageEngineDumb + Send + Sync + 'static>>,
}

impl BusFindPieceFetcher {
    pub fn pair() -> (
        BusFindPieceFetcher,
        mpsc::Receiver<Arc<dyn PieceStorageEngineDumb + Send + Sync + 'static>>,
    ) {
        let (tx, rx) = mpsc::channel(8);
        (BusFindPieceFetcher { response: tx }, rx)
    }

    pub async fn respond(
        mut self,
        fetcher: Arc<dyn PieceStorageEngineDumb + Send + Sync + 'static>,
    ) {
        let _ = self.response.send(fetcher).await;
    }
}

// --

pub struct BusAddPeer {
    peer_addr: SocketAddr,
    target_info_hash: TorrentId,
    source: BusAddPeerSource,
}

pub enum BusAddPeerSource {
    Tracker,
    UserImmediate,
    User(BusAddPeerSourceUserResponse),
    Incoming(BusAddPeerSourceIncomingState),
}

pub struct BusAddPeerSourceIncomingState {
    rbuf: BytesMut,
    handshake: Handshake,
}

pub struct BusAddPeerSourceUserResponse {
    response: mpsc::Sender<Result<PeerConnectSuccess, MagnetiteError>>,
}

pub struct PeerConnectSuccess {
    pub peer_id: TorrentId,
    pub session_id: Ksuid,
}

// --

pub struct BusPeerAdded {
    pub connect: PeerConnectSuccess
}

// --

pub struct BusPeerDisconnect {
    pub session_id: Ksuid,
}

// --

pub struct BusKillConnection {
    pub session_id: Ksuid,
    pub response: mpsc::Sender<Result<(), MagnetiteError>>,
}

// ---

pub struct BusSessionPeerBitfieldUpdate {
    pub target_info_hash: TorrentId,
    pub session_id: Ksuid,
    pub update: SmallVec<[BusSessionPeerBitfieldUpdateElement; 16]>,
}

pub struct BusSessionPeerBitfieldUpdateElement {
    pub piece_id: u32,
    // support both HAVE and UNHAVE 
    pub have_piece: bool,
}

// --