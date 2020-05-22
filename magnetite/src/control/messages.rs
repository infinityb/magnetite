use std::sync::Arc;

use tokio::sync::mpsc;

use super::TorrentEntry;
use crate::model::TorrentMetaWrapped;

#[derive(Clone)]
pub struct BusListTorrents {
    pub response: mpsc::Sender<Vec<TorrentEntry>>,
}

// --

#[derive(Clone)]
pub struct BusAddTorrent {
    pub torrent: Arc<TorrentMetaWrapped>,
    pub response: mpsc::Sender<()>,
}

// --

#[derive(Clone)]
pub struct BusRemoveTorrent {
    pub response: mpsc::Sender<()>,
}
