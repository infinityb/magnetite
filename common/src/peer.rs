use metorrent_util::Sha1;

use super::TorrentInfo;

/// Holds all the needed information to track a client.
pub struct PeerState {
    torrent_info: TorrentInfo,
    peer_id: Sha1,

    // this client is choking the peer
    am_choking: bool,
    // this client is interested in the peer
    am_interested: bool,

    // peer is choking this client
    peer_choking: bool,
    // peer is interested in this client
    peer_interested: bool,

    // the peer has these pieces
    pieces: Bitfield,

    // We are waiting for the peer to respond to these requests
    pending_requests: VecDeque<Request>,

    // The peer is waiting for us to respond to these requests.
    // If we are terminating the connection, we should cancel
    // these requests?
    peer_pending_requests: VecDeque<Request>,
}

impl PeerState {
    pub fn handle(&mut self, msg: &Message) -> Result<(), ()> {
        match *msg {
            Message::Choke => {
                self.peer_choking = true;
                Ok(())
            },
            Message::Unchoke => {
                self.peer_choking = false;
                Ok(())
            },
            Message::Interested => {
                self.peer_interested = true;
                Ok(())
            },
            Message::NotInterested => {
                self.peer_interested = false;
                Ok(())
            },
            Message::Have(piece_num) => {
                self.pieces.set(piece_num, true);
                Ok(())
            },
            Message::Request(ref req) => {
                self.peer_pending_requests.push_back(req.clone());
                Ok(())
            },

            // This is never emitted after the handshake.
            // We'll drop the client if they send this.
            Message::Bitfield(ref _bf) => Err(()),

            // Handle these elsewhere.
            Message::Piece(ref _piece) => Ok(()),
            Message::Cancel(ref _cancel) => Ok(()), // maybe.
            Message::Port(_port) => Ok(()),
        }
    }
}
