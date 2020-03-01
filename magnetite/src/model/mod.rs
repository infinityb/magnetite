use std::fmt;
use std::sync::Arc;

use rand::RngCore;
use serde::{Serialize, Deserialize};
use sha1::{Sha1, Digest};

pub mod proto;

use crate::CARGO_PKG_VERSION;

const TORRENT_ID_LENGTH: usize = 20;

#[derive(Debug)]
pub struct Truncated;

impl fmt::Display for Truncated {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Truncated")
    }
}

impl std::error::Error for Truncated {}

// --

#[derive(Debug)]
pub struct ProtocolViolation;

impl fmt::Display for ProtocolViolation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ProtocolViolation")
    }
}

impl std::error::Error for ProtocolViolation {}

// --

#[derive(Debug)]
pub struct StorageEngineCorruption;

impl fmt::Display for StorageEngineCorruption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StorageEngineCorruption")
    }
}

impl std::error::Error for StorageEngineCorruption {}


// --

#[derive(Debug)]
pub struct BadHandshake;

impl fmt::Display for BadHandshake {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BadHandshake")
    }
}

impl std::error::Error for BadHandshake {}

// --

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TorrentID {
    pub data: [u8; TORRENT_ID_LENGTH],
}

impl fmt::Debug for TorrentID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TorrentID")
            .field("data", &bencode::BinStr(&self.data))
            .finish()
    }
}

/// panics if chunks is too large.
pub fn copy_from_byte_vec_nofill<'a>(mut into: &'a mut [u8], chunks: &[&[u8]]) -> &'a mut [u8] {
    for ch in chunks {
        let (cur, rest) = into.split_at_mut(ch.len());
        cur.copy_from_slice(ch);
        into = rest;
    }
    into
}

impl TorrentID {
    pub const LENGTH: usize = TORRENT_ID_LENGTH;

    pub fn zero() -> TorrentID {
        TorrentID {
            data: [0; TORRENT_ID_LENGTH],
        }
    }

    pub fn from_slice(r: &[u8]) -> Result<TorrentID, failure::Error> {
        if r.len() != TORRENT_ID_LENGTH {
            return Err(failure::format_err!("slice length is {}, must be {}", r.len(), TORRENT_ID_LENGTH));
        }
        let mut out = TorrentID::zero();
        for (o, i) in out.data.iter_mut().zip(r.iter()) {
            *o = *i;
        }
        Ok(out)
    }

    pub fn generate_peer_id_seeded(random: &str) -> TorrentID {
        let mut hasher = Sha1::new();
        hasher.input(random.as_bytes());
        let hash_result = hasher.result();

        let mut data = [0; TORRENT_ID_LENGTH];
        data.copy_from_slice(&hash_result[..]);
        copy_from_byte_vec_nofill(&mut data, &[
            b"YM-",
            CARGO_PKG_VERSION.as_bytes(),
            b"-",
        ]);
        TorrentID { data }
    }

    pub fn generate_peer_id_rng(r: &mut dyn RngCore) -> TorrentID {
        let mut data = [0; TORRENT_ID_LENGTH];
        let rest = copy_from_byte_vec_nofill(&mut data, &[
            b"YM-",
            CARGO_PKG_VERSION.as_bytes(),
            b"-",
        ]);
        r.fill_bytes(rest);
        TorrentID { data }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TorrentMeta {
    pub announce: String,
    #[serde(rename="announce-list")]
    pub announce_list: Vec<Vec<String>>,
    pub comment: String,
    #[serde(rename="created by")]
    pub created_by: String,
    #[serde(rename="creation date")]
    pub creation_date: u64,
    pub info: TorrentMetaInfo,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TorrentMetaInfo {
    pub files: Vec<TorrentMetaInfoFile>,
    #[serde(rename="piece length")]
    pub piece_length: u64,
    #[serde(with = "serde_bytes")]
    pub pieces: Vec<u8>,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TorrentMetaInfoFile {
    pub length: u64,
    // pub path: Vec<String>,
}

#[derive(Clone)]
pub struct TorrentMetaWrapped {
    pub meta: TorrentMeta,
    pub total_length: u64,
    pub info_hash: TorrentID,
    pub piece_shas: Arc<Vec<TorrentID>>,
}


impl TorrentMetaWrapped {
    pub fn from_bytes(buffer: &[u8]) -> Result<TorrentMetaWrapped, failure::Error> {
        let unpacked: bencode::Value = bencode::from_bytes(&buffer[..])?;
        let meta: TorrentMeta = bencode::from_bytes(&buffer[..])?;
        
        let info_hash;
        if let bencode::Value::Dict(ref d) = unpacked {
            let mut hasher = Sha1::new();
            let info = bencode::to_bytes(d.get(&b"info"[..]).unwrap())?;
            hasher.input(&info[..]);
            let hash_result = hasher.result();
            info_hash = TorrentID::from_slice(&hash_result[..])?;
        } else {
            return Err(failure::format_err!("invalid torrent: missing info"));
        }

        let mut total_length = 0;
        for file in &meta.info.files {
            total_length += file.length;
        }

        let mut piece_shas = Vec::new();
        for c in meta.info.pieces.chunks(20) {
            let mut tid = TorrentID::zero();
            tid.data.copy_from_slice(c);
            piece_shas.push(tid);
        }

        Ok(TorrentMetaWrapped {
            meta,
            total_length,
            info_hash,
            piece_shas: Arc::new(piece_shas),
        })
    }
}

impl fmt::Debug for TorrentMetaInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let size = self.pieces.len();
        f.debug_struct("TorrentMetaInfo")
            .field("files", &self.files)
            .field("piece_length", &self.piece_length)
            .field("pieces", &size)
            .field("name", &self.name)
            .finish()
    }
}

struct PeerState {
    // A block is downloaded by the client when the client is interested in a
    // peer, and that peer is not choking the client.
    am_choked: bool,
    am_interested: bool,
    // A block is uploaded by a client when the client is not choking a peer,
    // and that peer is interested in the client. 
    peer_choked: bool,
    peer_interested: bool,
}

impl PeerState {
    pub fn new() -> PeerState {
        PeerState {
            am_choked: true,
            am_interested: false,
            peer_choked: true,
            peer_interested: false,
        }
    }
}

#[derive(Clone)]
pub struct BitField {
    pub bit_length: u32,
    pub data: Box<[u8]>,
}

impl BitField {
    pub fn as_raw_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn all(bit_length: u32) -> BitField {
        let mut byte_length = (bit_length / 8) as usize;
        let overflow_bits = (bit_length % 8) as u8;
        let fill_byte_length = byte_length;
        if overflow_bits > 0 {
            byte_length += 1;
        }
        let mut v = Vec::with_capacity(byte_length);
        for _ in 0..fill_byte_length {
            v.push(b'\xFF');
        }
        let mut last_byte = 0x00;
        for bf in 0..overflow_bits {
            last_byte |= 1 << (7 - bf);
        }
        if overflow_bits > 0 {
            v.push(last_byte);
        }
        BitField {
            bit_length: bit_length,
            data: v.into_boxed_slice(),
        }
    }

    pub fn none(bit_length: u32) -> BitField {
        let mut byte_length = (bit_length / 8) as usize;
        if bit_length % 8 > 0 {
            byte_length += 1;
        }
        BitField {
            bit_length: bit_length,
            data: vec![0; byte_length].into_boxed_slice(),
        }
    }

    pub fn set(&mut self, index: u32, value: bool) {
        assert!(index < self.bit_length);
        let byte_index = (index / 8) as usize;
        let bit_index = (index % 8) as u8;
        self.data[byte_index] |= 1 << bit_index;
    }
}