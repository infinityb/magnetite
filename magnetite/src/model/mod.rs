use std::fmt;
use std::io;
use std::ops::{BitAnd, BitXor};
use std::path::PathBuf;

use failure::Fail;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

use magnetite_common::{TorrentId, TorrentMeta, TorrentMetaInfo, TorrentMetaInfoFile, TorrentMetaWrapped, TorrentMetaInfo};

use crate::CARGO_PKG_VERSION;

const TORRENT_ID_LENGTH: usize = 20;

#[derive(Debug, Fail, Clone)]
pub enum MagnetiteError {
    #[fail(display = "message truncated")]
    Truncated,
    #[fail(display = "protocol violation")]
    ProtocolViolation,
    #[fail(display = "storage engine corruption")]
    StorageEngineCorruption,
    #[fail(display = "bad handshake: {:?}", reason)]
    BadHandshake { reason: BadHandshakeReason },
    #[fail(display = "I/O error: {:?}", kind)]
    IoError { kind: io::ErrorKind },
    #[fail(display = "insufficient space")]
    InsufficientSpace,
    #[fail(display = "completion lost")]
    CompletionLost,
    #[fail(display = "internal error: {}", msg)]
    InternalError { msg: String },
}

impl From<CompletionLost> for MagnetiteError {
    fn from(_e: CompletionLost) -> MagnetiteError {
        MagnetiteError::CompletionLost
    }
}

impl From<Truncated> for MagnetiteError {
    fn from(_e: Truncated) -> MagnetiteError {
        MagnetiteError::Truncated
    }
}

impl From<ProtocolViolation> for MagnetiteError {
    fn from(_e: ProtocolViolation) -> MagnetiteError {
        MagnetiteError::ProtocolViolation
    }
}

impl From<StorageEngineCorruption> for MagnetiteError {
    fn from(_e: StorageEngineCorruption) -> MagnetiteError {
        MagnetiteError::StorageEngineCorruption
    }
}

impl From<BadHandshake> for MagnetiteError {
    fn from(e: BadHandshake) -> MagnetiteError {
        MagnetiteError::BadHandshake { reason: e.reason }
    }
}

impl From<io::Error> for MagnetiteError {
    fn from(e: io::Error) -> MagnetiteError {
        MagnetiteError::IoError { kind: e.kind() }
    }
}

impl From<InternalError> for MagnetiteError {
    fn from(e: InternalError) -> MagnetiteError {
        MagnetiteError::InternalError {
            msg: e.msg.to_string(),
        }
    }
}
//

#[derive(Debug)]
pub struct CompletionLost;

impl fmt::Display for CompletionLost {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CompletionLost")
    }
}

impl std::error::Error for CompletionLost {}

//

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
pub struct BadHandshake {
    reason: BadHandshakeReason,
}

#[derive(Debug, Clone)]
pub enum BadHandshakeReason {
    JunkData,
    UnknownInfoHash(TorrentId),
}

impl BadHandshake {
    pub fn junk_data() -> BadHandshake {
        BadHandshake {
            reason: BadHandshakeReason::JunkData,
        }
    }

    pub fn unknown_info_hash(ih: &TorrentId) -> BadHandshake {
        BadHandshake {
            reason: BadHandshakeReason::UnknownInfoHash(*ih),
        }
    }
}

impl fmt::Display for BadHandshake {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for BadHandshake {}

// --

#[derive(Debug)]
pub struct InternalError {
    pub msg: &'static str,
}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InternalError: {}", self.msg)
    }
}

impl std::error::Error for InternalError {}

// --

#[derive(Debug)]
struct FileError {
    path: PathBuf,
    cause: io::Error,
}

impl fmt::Display for FileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}", self.path.display(), self.cause)
    }
}

impl std::error::Error for FileError {}

// --

/// panics if chunks is too large.
fn copy_from_byte_vec_nofill<'a>(mut into: &'a mut [u8], chunks: &[&[u8]]) -> &'a mut [u8] {
    for ch in chunks {
        let (cur, rest) = into.split_at_mut(ch.len());
        cur.copy_from_slice(ch);
        into = rest;
    }
    into
}

pub fn generate_peer_id_seeded(random: &str) -> TorrentId {
    let mut hasher = Sha1::new();
    hasher.input(random.as_bytes());
    let hash_result = hasher.result();

    let mut data = [0; TORRENT_ID_LENGTH];
    data.copy_from_slice(&hash_result[..]);
    copy_from_byte_vec_nofill(&mut data, &[b"YM-", CARGO_PKG_VERSION.as_bytes(), b"-"]);
    TorrentId::from_slice(&data[..]).unwrap()
}

pub fn generate_peer_id_rng(r: &mut dyn RngCore) -> TorrentId {
    let mut data = [0; TORRENT_ID_LENGTH];
    let rest = copy_from_byte_vec_nofill(&mut data, &[b"YM-", CARGO_PKG_VERSION.as_bytes(), b"-"]);
    r.fill_bytes(rest);
    TorrentId::from_slice(&data[..]).unwrap()
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
