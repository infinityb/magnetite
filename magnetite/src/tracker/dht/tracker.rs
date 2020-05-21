use std::time::Instant;
use std::net::SocketAddr;

use magnetite_common::TorrentId;

struct TrackerTorrentState {
    peer_serial: u64,
    peers: BTreeMap<u64, PeerRecord>,
}

struct PeerRecord {
	last_announce: Instant,
	addr: SocketAddr
	id: TorrentId,
}

impl TrackerTorrentState {
    pub fn add_peer(&mut self, pr: PeerRecord) {
    	self.peers.insert(self.peer_serial, pr);
    	self.peer_serial += 1;
    }
}
