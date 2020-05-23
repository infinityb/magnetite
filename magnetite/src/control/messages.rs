use std::sync::Arc;

use tokio::sync::mpsc;

use super::TorrentEntry;
use crate::control::api::add_torrent_request::BackingFile;
use crate::model::{MagnetiteError, TorrentMetaWrapped};
use crate::storage::{PieceStorageEngineDumb, GetPieceRequestChannel};

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
    pub fn pair() -> (BusFindPieceFetcher, mpsc::Receiver<Arc<dyn PieceStorageEngineDumb + Send + Sync + 'static>>) {
        let (tx, rx) = mpsc::channel(8);
        (BusFindPieceFetcher { response: tx }, rx)
    }

    pub async fn respond(mut self, fetcher: Arc<dyn PieceStorageEngineDumb + Send + Sync + 'static>) {
        let _ = self.response.send(fetcher).await;
    }
}
