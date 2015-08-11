use std::mem;
use std::ops;

use metorrent_util::Slice;

static PROTO_NAME: &'static [u8] = b"BitTorrent protocol";

// offset is after ``pstr''
const RESERVED_OFFSET: usize = 0;
const RESERVED_LEN: usize = 8;

// offset is after ``pstr''
const INFO_HASH_OFFSET: usize = 8;
const INFO_HASH_LEN: usize = 20;

// offset is after ``pstr''
const PEER_ID_OFFSET: usize = 28;
const PEER_ID_LEN: usize = 20;

#[derive(Debug, Eq, PartialEq)]
enum HandshakeMeasurer {
    Initial,
    Protocol(u8),
    Reserved(u8),
    InfoHash(u8),
    PeerId(u8),
}

impl Default for HandshakeMeasurer {
    fn default() -> HandshakeMeasurer {
        HandshakeMeasurer::Initial
    }
}

impl HandshakeMeasurer {
    pub fn push_byte(&mut self, byte: u8) -> bool {
        use self::HandshakeMeasurer::*;
        *self = match *self {
            Initial => Protocol(byte),
            Protocol(0) => return false,
            Protocol(1) => Reserved(RESERVED_LEN as u8),
            Protocol(v) => Protocol(v - 1),
            Reserved(0) => unreachable!(),
            Reserved(1) => InfoHash(INFO_HASH_LEN as u8),
            Reserved(v) => Reserved(v - 1),
            InfoHash(0) => unreachable!(),
            InfoHash(1) => PeerId(PEER_ID_LEN as u8),
            InfoHash(v) => InfoHash(v - 1),
            PeerId(0) => return false,
            PeerId(v) => PeerId(v - 1),
        };
        true
    }

    pub fn is_valid(&self) -> bool {
        match *self {
            HandshakeMeasurer::PeerId(_) => true,
            _ => false,
        }
    }
}

pub struct HandshakeBuf { inner: Vec<u8> }

impl ops::Deref for HandshakeBuf {
    type Target = Handshake;

    fn deref<'a>(&'a self) -> &'a Handshake {
        Handshake::from_u8_slice_unchecked(&self.inner)
    }
}

impl HandshakeBuf {
    fn new_unchecked(buf: Vec<u8>) -> HandshakeBuf {
        HandshakeBuf { inner: buf }
    }

    pub fn new(mut buf: Vec<u8>) -> Result<HandshakeBuf, &'static str> {
        let header_len = try!(Handshake::new(&buf)).as_bytes().len();
        buf.truncate(header_len);
        Ok(HandshakeBuf::new_unchecked(buf))
    }

    pub fn build(reserved: &[u8], info_hash: &[u8], peer_id: &[u8]) -> Option<HandshakeBuf> {
        if reserved.len() != RESERVED_LEN {
            return None;
        }
        if info_hash.len() != INFO_HASH_LEN {
            return None;
        }
        if peer_id.len() != PEER_ID_LEN {
            return None;
        }
        let buf_len = 20 + RESERVED_LEN + INFO_HASH_LEN + PEER_ID_LEN;
        let mut buf = Vec::with_capacity(buf_len);
        buf.push(19);
        buf.extend(PROTO_NAME);
        buf.extend(reserved);
        buf.extend(info_hash);
        buf.extend(peer_id);
        Some(HandshakeBuf::new(buf).ok().expect("generated invalid header"))
    }
}

// <pstrlen><pstr><reserved><info_hash>[<peer_id>]
pub struct Handshake { inner: Slice }

impl Handshake {
    fn from_u8_slice_unchecked(buf: &[u8]) -> &Handshake {
        unsafe { mem::transmute(buf) }
    }

    pub fn new(buf: &[u8]) -> Result<&Handshake, &'static str> {
        let mut state: HandshakeMeasurer = Default::default();
        let mut length = 0;

        for &byte in buf.iter() {
            if !state.push_byte(byte) {
                break;
            }
            length += 1;          
        }
        if !state.is_valid() {
            return Err("truncated");
        }
        Ok(Handshake::from_u8_slice_unchecked(&buf[..length]))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.inner
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

    pub fn has_peer_id(&self) -> bool {
        let buf = self.after_protocol();
        let peer_slice = &buf[PEER_ID_OFFSET..];
        peer_slice.len() >= 20
    }

    pub fn get_peer_id(&self) -> Option<&[u8]> {
        let buf = self.after_protocol();
        let peer_slice = &buf[PEER_ID_OFFSET..];
        if peer_slice.len() >= PEER_ID_LEN {
            Some(&peer_slice[..PEER_ID_LEN])
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PROTO_NAME, Handshake};

    #[test]
    fn parse_header_full() {
        static HEADER: &'static [u8] = b"\x13BitTorrent protocol\x00\x00\x00\x00\x00\x10\x00\x01\x14\xbe\xab\xae\xc8W\x99\x7f]p\xc1\x94\x10`kI\x1b\xb4\xf1\x9b-lt0D00-\xea/\x9f}\xd4\xb1\xa1$\xde\xaf\xe9\xb6";
        let header = Handshake::new(HEADER).unwrap();
        assert_eq!(header.get_protocol(), PROTO_NAME);
        assert_eq!(header.get_reserved(), b"\x00\x00\x00\x00\x00\x10\x00\x01");
        assert_eq!(header.get_info_hash(),
            b"\x14\xbe\xab\xae\xc8W\x99\x7f]p\xc1\x94\x10`kI\x1b\xb4\xf1\x9b");
        assert!(header.has_peer_id());
        assert_eq!(header.get_peer_id(),
            Some(&b"-lt0D00-\xea/\x9f}\xd4\xb1\xa1$\xde\xaf\xe9\xb6"[..]));
    }

    #[test]
    fn parse_header_partial() {
        static HEADER: &'static [u8] = b"\x13BitTorrent protocol\x00\x00\x00\x00\x00\x10\x00\x01\x14\xbe\xab\xae\xc8W\x99\x7f]p\xc1\x94\x10`kI\x1b\xb4\xf1\x9b";
        let header = Handshake::new(HEADER).unwrap();
        assert_eq!(header.get_protocol(), PROTO_NAME);
        assert_eq!(header.get_reserved(), b"\x00\x00\x00\x00\x00\x10\x00\x01");
        assert_eq!(header.get_info_hash(),
            b"\x14\xbe\xab\xae\xc8W\x99\x7f]p\xc1\x94\x10`kI\x1b\xb4\xf1\x9b");
        assert!(!header.has_peer_id());
        assert_eq!(header.get_peer_id(), None);
    }

    #[test]
    fn parse_header_oversized() {
        static HEADER: &'static [u8] = b"\x13BitTorrent protocol\x00\x00\x00\x00\x00\x10\x00\x01\x14\xbe\xab\xae\xc8W\x99\x7f]p\xc1\x94\x10`kI\x1b\xb4\xf1\x9b-lt0D00-\xea/\x9f}\xd4\xb1\xa1$\xde\xaf\xe9\xb6xxx";
        let header = Handshake::new(HEADER).unwrap();
        assert_eq!(header.as_bytes().len(), 68);
    }
}

#[cfg(feature="afl")]
pub mod afl {
    use std::io::{self, Read};
    use super::{PROTO_NAME, Header};

    pub fn afl_header_buf() {
        let mut buf = Vec::new();
        io::stdin().take(1 << 20).read_to_end(&mut buf).unwrap();
        let _ = Header::new(&buf);
    }
}
