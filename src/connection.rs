use std::mem;
use std::ops;
use std::collections::VecDeque;
use std::io::{self, Cursor, Read};

use byteorder::{ByteOrder, BigEndian, ReadBytesExt};
use mio::buf::RingBuf;

use ::mt::common::{UnmeasuredBitfield, Bitfield, TorrentInfo};
use ::mt::common::message::{Message, Request};
use ::mt::util::slice::Slice;
use ::mt::util::sha1::Sha1;

static PROTO_NAME: &'static [u8] = b"BitTorrent protocol";


// A block is downloaded by the client when the client is interested in a
// peer, and that peer is not choking the client. A block is uploaded by a
// client when the client is not choking a peer, and that peer is interested
// in the client.

pub struct ConnectionState2 {
    // Ingress peer data
    pub ingress_buf: RingBuf,
    // Egress peer data
    pub egress_buf: RingBuf,
}

/// Holds all the needed information to track a client.
pub struct ConnectionState {
    pub torrent_info: TorrentInfo,
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

impl ConnectionState {
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
    torrent_info: TorrentInfo,

    reserved: [u8; 8],
    peer_id: Sha1,

    wrote_handshake: bool,
    // Ingress peer data
    pub ingress_buf: RingBuf,
    // Egress peer data
    pub egress_buf: RingBuf,
    // the peer has these pieces
    pieces: UnmeasuredBitfield,
}

impl Default for Handshake {
    fn default() -> Self {
        Handshake {
            wrote_handshake: false,
            state: HandshakeState::Initial,
            torrent_info: TorrentInfo::zero(),
            reserved: [0; 8],
            peer_id: Sha1::new([0; 20]),
            ingress_buf: RingBuf::new(1 << 17),
            egress_buf: RingBuf::new(1 << 17),
            pieces: UnmeasuredBitfield::empty(),
        }
    }
}

impl Handshake {
    pub fn initiate(info_hash: &Sha1, client_id: &Sha1) -> Handshake {
        use std::io::Write;

        // None of these can be of invalid length, so this can't panic.
        let header = HeaderBuf::build(
            &[0; 8],
            info_hash.as_bytes(),
            client_id.as_bytes()).unwrap();

        let handshake = Handshake::default();
        handshake.egress_buf.write_all(header.as_bytes()).unwrap();
        handshake.wrote_handshake = true;
        handshake
    }

    fn try_read_header(&mut self) -> Result<(), &'static str> {
        use std::slice::bytes::copy_memory;
        use ::mio::buf::Buf;

        self.ingress_buf.mark();
        let mut discard_header = false;
        let mut state: HeaderMeasurer = Default::default();
        let mut length = 0;
        let mut finished = false;
        while let Some(byte) = self.ingress_buf.read_byte() {
            if !state.push_byte(byte) {
                finished = true;
                break;
            }
            length += 1;
        }
        self.ingress_buf.reset();
        if finished {

        }
        
        if state.is_valid() {
            let mut buf = Vec::new();

            self.ingress_buf.mark();
            while let Some(byte) = self.ingress_buf.read_byte() {
                if length <= buf.len() {
                    break;
                }
                buf.push(byte);
            }
            self.ingress_buf.reset();

            let header = try!(HeaderBuf::new(buf));
            self.state = HandshakeState::PeerId;

            if header.get_protocol() != PROTO_NAME {
                return Err("Unknown protocol version");
            }

            copy_memory(header.get_reserved(), &mut self.reserved);
            
            copy_memory(header.get_info_hash(),
                self.torrent_info.info_hash.as_bytes_mut());

            if let Some(peer_id) = header.get_peer_id() {
                copy_memory(peer_id, self.peer_id.as_bytes_mut());
                self.state = HandshakeState::Bitfield;
                discard_header = true;
            }
        }

        if discard_header {
            let mut discarded = 0;
            while let Some(byte) = self.ingress_buf.read_byte() {
                if length == discarded {
                    break;
                }
                discarded += 1;
            }
        }

        Ok(())
    }

    fn try_read_bitfield(&mut self) -> Result<(), &'static str> {
        unimplemented!();
    }

    pub fn try_read(&mut self) -> Result<(), &'static str> {
        use self::HandshakeState::*;
        match self.state {
            Initial => self.try_read_header(),
            PeerId => self.try_read_header(),
            Bitfield => self.try_read_bitfield(),
            Complete => Ok(()),
        }
    }

    pub fn get_info_hash(&self) -> Option<Sha1> {
        if self.state != HandshakeState::Initial {
            Some(self.torrent_info.info_hash)
        } else {
            None
        }
    }

    pub fn update_torrent_info(&mut self, info: TorrentInfo) {
        // Either in PeerId or Bitfield state at this time.
        assert!(self.get_info_hash().is_some());

        let pre_info_hash = self.torrent_info.info_hash;
        self.torrent_info = info;

        // Info hash can't change
        assert_eq!(self.torrent_info.info_hash, pre_info_hash);

    }

    pub fn set_pieces(&mut self, bf: UnmeasuredBitfield) -> Result<(), &'static str> {
        assert_eq!(self.state, HandshakeState::Bitfield);
        self.pieces = bf;
        self.state = HandshakeState::Complete;
        Ok(())
    }

    /// If this returns an error, the connection must be terminated.
    pub fn finish(self) -> Result<ConnectionState, &'static str> {
        use std::io::Write;

        if self.state != HandshakeState::Complete {
            return Err("Connection in invalid state for ending handshake");
        }

        // None of these can be of invalid length, so this can't panic.
        let header = HeaderBuf::build(
            &[0; 8],
            self.torrent_info.info_hash.as_bytes(),
            self.torrent_info.client_id.as_bytes()).unwrap();

        // Buffer should be pretty much empty, so this can't panic.
        if !self.wrote_handshake {
            self.egress_buf.write_all(header.as_bytes()).unwrap();
            self.wrote_handshake = true;
        }

        let pieces = try!(self.pieces.measure(self.torrent_info.num_pieces));
        Ok(ConnectionState {
            torrent_info: self.torrent_info,
            peer_id: self.peer_id,
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
