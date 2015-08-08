use std::mem;
use std::collections::VecDeque;
use std::io::{self, Cursor, Read};

use byteorder::{ByteOrder, BigEndian, ReadBytesExt};
use bytes::RingBuf;

use super::slice::Slice;
use super::bitfield::{UnmeasuredBitfield, Bitfield};
use super::message_types::Request;

static PROTO_NAME: &'static [u8] = b"BitTorrent protocol";


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
    //

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

// offset is after ``pstr''
const RESERVED_OFFSET: usize = 0;
const RESERVED_LEN: usize = 8;

// offset is after ``pstr''
const INFO_HASH_OFFSET: usize = 8;
const INFO_HASH_LEN: usize = 20;

// offset is after ``pstr''
const PEER_ID_OFFSET: usize = 28;
const PEER_ID_LEN: usize = 20;

// <pstrlen><pstr><reserved><info_hash>[<peer_id>]
pub struct HeaderBuf(Slice);

impl HeaderBuf {
    pub fn new(buf: &[u8]) -> Result<&HeaderBuf, &'static str> {
        let mut length = 1;
        if buf.len() < length{
            return Err("truncated");
        }

        length += buf[0] as usize;
        if buf.len() < length {
            return Err("truncated");
        }

        length += RESERVED_LEN + PEER_ID_LEN;
        if buf.len() < length {
            return Err("truncated");
        }

        // Maybe we have a peer_id too, take a look.
        length += PEER_ID_LEN;
        if buf.len() < length {
            // Nope, no peer_id here.
            length -= PEER_ID_LEN;
        }

        Ok(unsafe { mem::transmute(buf[..length]) })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn get_protocol(&self) -> &[u8] {
        let buf = self.as_bytes();
        let proto_len = buf[0] as usize;
        &buf[1..][..proto_len]
    }

    fn after_protocol(&self) -> &[u8] {
        let buf = self.as_bytes();
        let proto_len = buf[0] as usize;
        &buf[1..][proto_len..]
    }

    pub fn get_reserved(&self) -> &[u8] {
        let buf = self.after_protocol();
        &buf[RESERVED_OFFSET..][..RESERVED_LEN]
    }

    pub fn get_info_hash(&self) -> &[u8] {
        let buf = self.after_protocol();
        &buf[INFO_HASH_OFFSET..][..INFO_HASH_LEN]
    }

    pub fn get_peer_id(&self) -> Option<&[u8]> {
        let buf = self.after_protocol();
        let peer_slice = &buf[PEER_ID_OFFSET..];
        if peer_slice.len() > PEER_ID_LEN {
            Some(&peer_slice[..PEER_ID_LEN])
        } else {
            None
        }
    }
}

