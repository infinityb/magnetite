use std::fmt;
use std::path::PathBuf;

use serde::{Serialize, Deserialize};
use sha1::{Sha1, Digest};

use magnetite_common::TorrentId;

pub mod bitfield;
pub use self::bitfield::BitField;


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
    pub length: Option<u64>,
    #[serde(default)]
    pub files: Vec<TorrentMetaInfoFile>,
    #[serde(rename = "piece length")]
    pub piece_length: u32,
    #[serde(with = "torrent_info_pieces")]
    pub pieces: Vec<TorrentId>,
    pub name: String,
}

mod torrent_info_pieces {
    use crate::TorrentId;
    use serde::{Deserializer, Serializer};

    struct TorrentPieceVisitor;

    impl<'de> serde::de::Visitor<'de> for TorrentPieceVisitor {
        type Value = Vec<TorrentId>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("an byte-aray where the length is a multiple of 20")
        }

        fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value.len() % 20 != 0 {
                return Err(E::custom(format!("bad length: {}", value.len())));
            }

            let mut out = Vec::new();
            for ch in value.chunks(20) {
                let mut buf: [u8; 20] = [0; 20];
                buf.copy_from_slice(&ch[..20]);
                out.push(TorrentId(buf));
            }

            Ok(out)
        }
    }

    pub fn serialize<S>(
        addrs: &Vec<TorrentId>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut out = Vec::with_capacity(addrs.len() * 20);
        for a in addrs {
            out.extend(a.as_bytes());
        }
        serializer.serialize_bytes(&out)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<TorrentId>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(TorrentPieceVisitor)
    }
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
            write!(formatter, "a non-empty vector of path elements")
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
    pub info_hash: TorrentId,
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
            info_hash = TorrentId::from_slice(&hash_result[..])?;
        } else {
            return Err(failure::format_err!("invalid torrent: missing info"));
        }

        
        let total_length = if let Some(length) = meta.info.length {
            length
        } else {
            let mut total_length = 0;
            for file in &meta.info.files {
                total_length += file.length;
            }
            total_length
        };

        Ok(TorrentMetaWrapped {
            meta,
            total_length,
            info_hash,
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
