use std::fmt;
use std::io;
use std::ops::{BitAnd, BitXor};
use std::path::PathBuf;

use failure::Fail;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

pub mod config;
pub mod proto;

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
    UnknownInfoHash(TorrentID),
}

impl BadHandshake {
    pub fn junk_data() -> BadHandshake {
        BadHandshake {
            reason: BadHandshakeReason::JunkData,
        }
    }

    pub fn unknown_info_hash(ih: &TorrentID) -> BadHandshake {
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

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TorrentID(pub [u8; TORRENT_ID_LENGTH]);

impl TorrentID {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0[..]
    }

    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        &mut self.0[..]
    }
}

impl BitAnd for TorrentID {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        let mut out = TorrentID::zero();
        let lhs_bytes = self.as_bytes().iter();
        let rhs_bytes = rhs.as_bytes().iter();

        for (o, (a, b)) in out.as_mut_bytes().iter_mut().zip(lhs_bytes.zip(rhs_bytes)) {
            *o = *a & *b;
        }

        out
    }
}

impl BitXor for TorrentID {
    type Output = Self;

    fn bitxor(self, rhs: Self) -> Self::Output {
        let mut out = TorrentID::zero();
        let lhs_bytes = self.as_bytes().iter();
        let rhs_bytes = rhs.as_bytes().iter();

        for (o, (a, b)) in out.as_mut_bytes().iter_mut().zip(lhs_bytes.zip(rhs_bytes)) {
            *o = *a ^ *b;
        }

        out
    }
}

impl fmt::Debug for TorrentID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("TorrentID")
            .field(&bencode::HexStr(self.as_bytes()))
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
        TorrentID([0; TORRENT_ID_LENGTH])
    }

    pub fn is_zero(&self) -> bool {
        for by in self.as_bytes() {
            if *by != 0 {
                return false;
            }
        }
        true
    }

    pub fn from_slice(r: &[u8]) -> Result<TorrentID, failure::Error> {
        if r.len() != TORRENT_ID_LENGTH {
            return Err(failure::format_err!(
                "slice length is {}, must be {}",
                r.len(),
                TORRENT_ID_LENGTH
            ));
        }
        let mut out = Self::zero();
        out.as_mut_bytes().copy_from_slice(r);
        Ok(out)
    }

    pub fn generate_peer_id_seeded(random: &str) -> TorrentID {
        let mut hasher = Sha1::new();
        hasher.input(random.as_bytes());
        let hash_result = hasher.result();

        let mut data = [0; TORRENT_ID_LENGTH];
        data.copy_from_slice(&hash_result[..]);
        copy_from_byte_vec_nofill(&mut data, &[b"YM-", CARGO_PKG_VERSION.as_bytes(), b"-"]);
        TorrentID(data)
    }

    pub fn generate_peer_id_rng(r: &mut dyn RngCore) -> TorrentID {
        let mut data = [0; TORRENT_ID_LENGTH];
        let rest =
            copy_from_byte_vec_nofill(&mut data, &[b"YM-", CARGO_PKG_VERSION.as_bytes(), b"-"]);
        r.fill_bytes(rest);
        TorrentID(data)
    }

    pub fn count_ones(&self) -> u32 {
        let mut acc = 0;
        for b in self.as_bytes() {
            acc += b.count_ones();
        }
        acc
    }

    pub fn leading_zeros(&self) -> u32 {
        let mut acc = 0;
        for b in self.as_bytes() {
            let lz = b.leading_zeros();
            acc += lz;

            if lz != 8 {
                break;
            }
        }
        acc
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TorrentMeta {
    #[serde(default)]
    pub announce: String,
    #[serde(rename = "announce-list", default)]
    pub announce_list: Vec<Vec<String>>,
    #[serde(default)]
    pub comment: String,
    #[serde(rename = "created by", default)]
    pub created_by: String,
    #[serde(rename = "creation date", default)]
    pub creation_date: u64,
    pub info: TorrentMetaInfo,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TorrentMetaInfo {
    #[serde(default)]
    pub files: Vec<TorrentMetaInfoFile>,
    #[serde(rename = "piece length")]
    pub piece_length: u32,
    #[serde(with = "serde_bytes")]
    pub pieces: Vec<u8>,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TorrentMetaInfoFile {
    pub length: u64,
    #[serde(with = "bt_pathbuf")]
    pub path: PathBuf,
}

mod bt_pathbuf {
    use std::borrow::Cow;
    use std::fmt;
    use std::path::{Component, Path, PathBuf};

    use serde::de::{self, Deserializer, SeqAccess, Visitor};
    use serde::ser::{self, SerializeSeq, Serializer};

    pub fn serialize<S>(buf: &PathBuf, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(buf.components().count()))?;

        for co in buf.components() {
            match co {
                Component::Prefix(..) | Component::RootDir => {
                    return Err(ser::Error::custom("path must not be absolute"));
                }
                Component::CurDir | Component::ParentDir => {
                    return Err(ser::Error::custom("path must be canonical"));
                }
                Component::Normal(v) => {
                    seq.serialize_element(v)?;
                }
            }
        }

        seq.end()
    }

    struct _Visitor;

    impl<'de> Visitor<'de> for _Visitor {
        type Value = PathBuf;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            write!(formatter, "a non-empty vector of paths")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut buf = PathBuf::new();

            let mut observed_element = false;
            while let Some(part) = seq.next_element::<Cow<Path>>()? {
                buf.push(part);
                observed_element = true;
            }
            if !observed_element {
                return Err(de::Error::custom("path vec must be non-empty"));
            }

            Ok(buf)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(_Visitor)
    }
}

#[derive(Clone)]
pub struct TorrentMetaWrapped {
    pub meta: TorrentMeta,
    pub total_length: u64,
    pub info_hash: TorrentID,
    pub piece_shas: Vec<TorrentID>,
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
            tid.as_mut_bytes().copy_from_slice(c);
            piece_shas.push(tid);
        }

        Ok(TorrentMetaWrapped {
            meta,
            total_length,
            info_hash,
            piece_shas,
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

#[derive(Debug, Clone)]
pub struct BitField {
    pub bit_length: u32,
    pub set_count: u32,
    pub data: Box<[u8]>,
}

impl<'a> BitAnd for &'a BitField {
    type Output = BitField;

    fn bitand(self, rhs: Self) -> Self::Output {
        if self.bit_length != rhs.bit_length {
            return BitField {
                bit_length: 0,
                set_count: 0,
                data: Vec::new().into(),
            }
        }

        let mut out = BitField {
            bit_length: self.bit_length,
            set_count: 0,
            data: vec![0; self.data.len()].into(),
        };

        let lhs_bytes = self.data.iter();
        let rhs_bytes = rhs.data.iter();

        for (o, (a, b)) in out.data.iter_mut().zip(lhs_bytes.zip(rhs_bytes)) {
            *o = *a & *b;
            out.set_count += o.count_ones() as u32;
        }

        out
    }
}

impl<'a> BitXor for &'a BitField {
    type Output = BitField;

    fn bitxor(self, rhs: Self) -> Self::Output {
        if self.bit_length != rhs.bit_length {
            return BitField {
                bit_length: 0,
                set_count: 0,
                data: Vec::new().into(),
            }
        }

        let mut out = BitField {
            bit_length: self.bit_length,
            set_count: 0,
            data: vec![0; self.data.len()].into(),
        };

        let lhs_bytes = self.data.iter();
        let rhs_bytes = rhs.data.iter();

        for (o, (a, b)) in out.data.iter_mut().zip(lhs_bytes.zip(rhs_bytes)) {
            *o = *a & *b;
            out.set_count += o.count_ones() as u32;
        }

        out
    }
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
            bit_length,
            set_count: bit_length,
            data: v.into_boxed_slice(),
        }
    }

    pub fn none(bit_length: u32) -> BitField {
        let mut byte_length = (bit_length / 8) as usize;
        if bit_length % 8 > 0 {
            byte_length += 1;
        }
        BitField {
            bit_length,
            set_count: 0,
            data: vec![0; byte_length].into_boxed_slice(),
        }
    }

    pub fn set(&mut self, index: u32, value: bool) -> bool {
        assert!(index < self.bit_length);
        let byte_index = (index / 8) as usize;
        let bit_index = (index % 8) as u8;
        let current_byte = &mut self.data[byte_index];
        let with_set_bit = 1 << bit_index;

        let isset = (*current_byte & with_set_bit) != 0;
        let mut bit_modified = false;
        if value {
            if !isset {
                self.set_count += 1;
                bit_modified = true;
            }
            *current_byte |= with_set_bit;
        } else {
            if isset {
                self.set_count -= 1;
                bit_modified = true;
            }
            *current_byte &= !with_set_bit;
        }
        bit_modified
    }

    pub fn has(&self, index: u32) -> bool {
        if self.is_filled() {
            return true;
        }
        if self.is_empty() {
            return false;
        }

        assert!(index < self.bit_length);
        let byte_index = (index / 8) as usize;
        let bit_index = (index % 8) as u8;
        (self.data[byte_index] & (1 << bit_index)) > 0
    }

    pub fn count_ones(&self) -> u32 {
        self.set_count
    }

    pub fn is_empty(&self) -> bool {
        self.set_count == 0
    }

    pub fn is_filled(&self) -> bool {
        self.bit_length == self.set_count
    }

    pub fn iter(&self) -> Iter {
        Iter {
            parent: self.data.iter(),
            bit_length: self.bit_length,
            bit_offset: 8,
            cur_byte: 0,
        }
    }
}

pub struct Iter<'a> {
    parent: std::slice::Iter<'a, u8>,
    bit_length: u32,
    bit_offset: u8,
    cur_byte: u8,
}

impl<'a> Iterator for Iter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<bool> {
        if self.bit_length == 0 {
            return None;
        }

        loop {
            if self.bit_offset < 8 {
                let mask = 1 << self.bit_offset;
                self.bit_offset += 1;
                self.bit_length -= 1;
                return Some(self.cur_byte & mask > 0);
            }

            self.cur_byte = *self.parent.next()?;
            self.bit_offset = 0;
        }
    }
}
