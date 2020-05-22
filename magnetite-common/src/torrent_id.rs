use std::fmt;
use std::ops::{BitAnd, BitXor};

const TORRENT_ID_LENGTH: usize = 20;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TorrentId(pub [u8; TORRENT_ID_LENGTH]);

pub struct TorrentIdHexFormat<'a> {
    torrent_id: &'a TorrentId,
}

#[derive(Debug)]
pub struct TorrentIdError;

// -- impls --

impl TorrentId {
    /// The byte length of a TorrentId.
    pub const LENGTH: usize = TORRENT_ID_LENGTH;

    /// Returns a TorrentId with all bits set to zero.
    pub fn zero() -> TorrentId {
        TorrentId([0; TORRENT_ID_LENGTH])
    }

    /// Checks if all bits are set to zero, useful for the results of bitwise
    /// operations.
    pub fn is_zero(&self) -> bool {
        for by in self.as_bytes() {
            if *by != 0 {
                return false;
            }
        }
        true
    }

    /// Compies the data from `r` into the TorrentId.
    pub fn from_slice(r: &[u8]) -> Result<TorrentId, TorrentIdError> {
        if r.len() != TORRENT_ID_LENGTH {
            return Err(TorrentIdError);
        }
        let mut out = Self::zero();
        out.as_mut_bytes().copy_from_slice(r);
        Ok(out)
    }

    /// Copies the data from the TorrentId into `r`.
    pub fn write_slice(&self, r: &mut [u8]) -> Result<(), TorrentIdError> {
        if r.len() != TORRENT_ID_LENGTH {
            return Err(TorrentIdError);
        }
        r.copy_from_slice(self.as_bytes());
        Ok(())
    }

    /// Count the number of one bits present
    pub fn count_ones(&self) -> u32 {
        let mut acc = 0;
        for b in self.as_bytes() {
            acc += b.count_ones();
        }
        acc
    }

    /// Count the number of leading zero bits, useful for the results of bitwise
    /// operations.
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

    /// Get a formatter that will format to a hex string representing the
    /// contents of this TorrentId
    pub fn hex(&self) -> TorrentIdHexFormat {
        TorrentIdHexFormat { torrent_id: self }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0[..]
    }

    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        &mut self.0[..]
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let tid: TorrentId = *self;
        tid.into()
    }
}

impl fmt::Display for TorrentIdHexFormat<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for b in self.torrent_id.as_bytes() {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

impl fmt::Debug for TorrentId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TorrentId(\"{}\")", self.hex())
    }
}

impl BitAnd for TorrentId {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        let mut out = TorrentId::zero();
        let lhs_bytes = self.as_bytes().iter();
        let rhs_bytes = rhs.as_bytes().iter();

        for (o, (a, b)) in out.as_mut_bytes().iter_mut().zip(lhs_bytes.zip(rhs_bytes)) {
            *o = *a & *b;
        }

        out
    }
}

impl BitXor for TorrentId {
    type Output = Self;

    fn bitxor(self, rhs: Self) -> Self::Output {
        let mut out = TorrentId::zero();
        let lhs_bytes = self.as_bytes().iter();
        let rhs_bytes = rhs.as_bytes().iter();

        for (o, (a, b)) in out.as_mut_bytes().iter_mut().zip(lhs_bytes.zip(rhs_bytes)) {
            *o = *a ^ *b;
        }

        out
    }
}

impl fmt::Display for TorrentIdError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TorrentIdError")
    }
}

impl std::error::Error for TorrentIdError {}

impl From<[u8; TORRENT_ID_LENGTH]> for TorrentId {
    fn from(data: [u8; TORRENT_ID_LENGTH]) -> TorrentId {
        TorrentId(data)
    }
}

impl From<TorrentId> for Vec<u8> {
    fn from(tid: TorrentId) -> Vec<u8> {
        let mut out = vec![0; 20];
        out.copy_from_slice(&tid.0[..]);
        out
    }
}

// --

impl serde::ser::Serialize for TorrentId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            let s = format!("{}", self.hex());
            s.serialize(serializer)
        } else {
            serializer.serialize_bytes(self.as_bytes())
        }
    }
}

impl<'de> serde::de::Deserialize<'de> for TorrentId {
    fn deserialize<D>(deserializer: D) -> Result<TorrentId, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(TorrentIdVisitor)
    }
}

struct TorrentIdVisitor;

impl<'de> serde::de::Visitor<'de> for TorrentIdVisitor {
    type Value = TorrentId;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a 40 byte hex string or 20 bytes")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if s.len() == TorrentId::LENGTH {
            return self.visit_bytes(s.as_bytes());
        }
        if s.len() != TorrentId::LENGTH * 2 {
            return Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(s),
                &self,
            ));
        }
        let mut buf: [u8; 20] = [0; 20];
        let dehexed = match dehex_fixed_size(s, &mut buf[..]) {
            Ok(v) => v,
            Err(()) => {
                return Err(serde::de::Error::invalid_value(
                    serde::de::Unexpected::Str(s),
                    &self,
                ))
            }
        };
        Ok(TorrentId::from_slice(dehexed).unwrap())
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.len() != TorrentId::LENGTH {
            return Err(serde::de::Error::invalid_length(v.len(), &self));
        }
        Ok(TorrentId::from_slice(v).unwrap())
    }
}

fn dehex_fixed_size<'a>(val: &str, into: &'a mut [u8]) -> Result<&'a [u8], ()> {
    fn nibble_from_char(ch: u8) -> Result<u8, ()> {
        match ch {
            b'A'..=b'F' => Ok(ch - b'A' + 10),
            b'a'..=b'f' => Ok(ch - b'a' + 10),
            b'0'..=b'9' => Ok(ch - b'0'),
            _ => Err(()),
        }
    }
    let mut copied_bytes = 0;
    let mut inbytes = val.bytes();
    for oby in into.iter_mut() {
        let mut buf = 0;
        if let Some(ch) = inbytes.next() {
            buf |= nibble_from_char(ch)?;
        } else {
            return Err(());
        }
        buf <<= 4;
        if let Some(ch) = inbytes.next() {
            buf |= nibble_from_char(ch)?;
        } else {
            return Err(());
        }
        *oby = buf;
        copied_bytes += 1;
    }
    Ok(&into[copied_bytes..])
}
