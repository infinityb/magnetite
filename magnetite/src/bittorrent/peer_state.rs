use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use magnetite_common::TorrentId;

use crate::model::proto::Handshake;
use crate::model::{BitField, TorrentMetaWrapped};

pub struct TrackerGroup {
    //
}

pub struct Torrent {
    pub id: TorrentId,
    pub name: String,
    pub meta: Arc<TorrentMetaWrapped>,
    pub have_bitfield: BitField,
    pub tracker_groups: Vec<TrackerGroup>,
}

#[derive(Default)]
pub struct GlobalState {
    pub session_id_seq: u64,
    pub torrents: HashMap<TorrentId, Torrent>,
    pub sessions: HashMap<u64, Session>,
    pub global_stats: Stats,
}

pub fn merge_global_payload_stats(cc: &mut GlobalState, ps: &mut PeerState) {
    cc.global_stats.recv_payload_bytes += ps.global_uncommitted_stats.recv_payload_bytes;
    cc.global_stats.sent_payload_bytes += ps.global_uncommitted_stats.sent_payload_bytes;
    ps.global_uncommitted_stats = Default::default();
}

#[derive(Copy, Clone, Default)]
pub struct Stats {
    pub sent_payload_bytes: u64,
    pub recv_payload_bytes: u64,
}

pub struct Session {
    pub id: u64,
    pub addr: SocketAddr,
    pub handshake: Handshake,
    pub target: TorrentId,
    // Arc<TorrentMetaWrapped>,
    pub state: PeerState,
    // storage_engine: PieceFileStorageEngine,
}

#[derive(Clone)]
pub struct PeerState {
    pub last_read: Instant,
    pub next_keepalive: Instant,
    pub stats: Stats,
    pub global_uncommitted_stats: Stats,
    pub peer_bitfield: BitField,
    pub choking: bool,
    pub interesting: bool,
    pub choked: bool,
    pub interested: bool,
}

impl PeerState {
    pub fn new(bf_length: u32) -> PeerState {
        let now = Instant::now();
        PeerState {
            last_read: now,
            next_keepalive: now,
            stats: Default::default(),
            global_uncommitted_stats: Default::default(),
            peer_bitfield: BitField::none(bf_length),
            choking: true,
            interesting: false,
            choked: true,
            interested: false,
        }
    }
}
