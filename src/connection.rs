use std::collections::VecDeque;
use bytes::RingBuf;

use super::bitfield::{UnmeasuredBitfield, Bitfield};
use super::message_types::Request;

// A block is downloaded by the client when the client is interested in a
// peer, and that peer is not choking the client. A block is uploaded by a
// client when the client is not choking a peer, and that peer is interested
// in the client.

pub struct TorrentInfo {
    info_hash: [u8; 20],

    // The number of pieces in the torrent
    num_pieces: u32,

    // The size of each piece
    piece_length: u32,
}

/// Holds all the needed information to track a client.
pub struct ConnectionState {
    torrent_info: TorrentInfo,
    client_id: [u8; 20],

    // Ingress peer data
    ingress_buf: RingBuf,
    // Egress peer data
    egress_buf: RingBuf,

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

impl ConnectionState {
    pub fn foo() {}
}

#[derive(Debug, Eq, PartialEq)]
enum HandshakeState {
    // No data received.
    Initial,
    // Have infohash and proto version. Waiting on peer_id.
    PeerId,
    // Have whole handshake message. Waiting on bitfield.
    // The bitfield must be first if the client is to send a bitfield, but the
    // bitfield is optional.  If we see a non-bitfield message, we just
    // initialize an empty bitfield of the correct length and move to 
    // `Complete` state.
    Bitfield,
    // Got everything.
    Complete,
}

// Pre-handshake client state.
pub struct Handshake {
    // the state of the handshake process
    state: HandshakeState,

    info_hash: [u8; 20],
    client_id: [u8; 20],

    // Ingress peer data
    ingress_buf: RingBuf,
    // Egress peer data
    egress_buf: RingBuf,
    // the peer has these pieces
    pieces: UnmeasuredBitfield,
}

impl Default for Handshake {
    fn default() -> Self {
        Handshake {
            state: HandshakeState::Initial,
            info_hash: [0; 20],
            client_id: [0; 20],
            ingress_buf: RingBuf::new(1 << 15),
            egress_buf: RingBuf::new(1 << 15),
            pieces: UnmeasuredBitfield::empty(),
        }
    }
}

impl Handshake {
    pub fn set_pieces(&mut self, bf: UnmeasuredBitfield) -> Result<(), &'static str> {
        assert_eq!(self.state, HandshakeState::Bitfield);
        self.pieces = bf;
        self.state = HandshakeState::Complete;
        Ok(())
    }

    /// If this returns an error, the connection must be terminated.
    pub fn finish(self, info: TorrentInfo) -> Result<ConnectionState, &'static str> {
        if self.state != HandshakeState::Complete {
            return Err("Connection in invalid state for ending handshake");
        }
        let pieces = try!(self.pieces.measure(info.num_pieces));
        Ok(ConnectionState {
            torrent_info: info,
            client_id: self.client_id,
            ingress_buf: self.ingress_buf,
            egress_buf: self.egress_buf,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            pieces: pieces,
            pending_requests: VecDeque::new(),
            peer_pending_requests: VecDeque::new(),
        })
    }
}