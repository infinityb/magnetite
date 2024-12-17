use std::fmt;
use std::ops::{BitAnd, BitXor, BitOr};

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

    /// Returns a TorrentId with all bits set to one.
    pub fn max_value() -> TorrentId {
        TorrentId([0xFF; TORRENT_ID_LENGTH])
    }

    /// Returns a TorrentId with `bit_count` bits set to zero.
    pub fn with_high_bits(mut bit_count: u32) -> TorrentId {
        assert!(bit_count <= 160);
        let mut buf = [0; TORRENT_ID_LENGTH];

        let mut byiter = buf.iter_mut();
        while 8 <= bit_count {
            bit_count -= 8;
            if let Some(by) = byiter.next() {
                *by = 0xFF;
            }
        }
        if let Some(by) = byiter.next() {
            *by = u8::max_value() ^ (u8::max_value() >> bit_count);
        }
        TorrentId(buf)
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
}

impl std::str::FromStr for TorrentId {
    type Err = TorrentIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut buf: [u8; 20] = [0; 20];
        let dehexed = dehex_fixed_size(s, &mut buf[..])
            .map_err(|()| TorrentIdError)?;
        match TorrentId::from_slice(dehexed) {
            Ok(v) => Ok(v),
            Err(..) => Err(TorrentIdError),
        }
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


impl BitOr for TorrentId {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        let mut out = TorrentId::zero();
        let lhs_bytes = self.as_bytes().iter();
        let rhs_bytes = rhs.as_bytes().iter();

        for (o, (a, b)) in out.as_mut_bytes().iter_mut().zip(lhs_bytes.zip(rhs_bytes)) {
            *o = *a | *b;
        }

        out
    }
}

impl std::ops::Not for TorrentId {
    type Output = TorrentId;

    fn not(mut self) -> TorrentId {
        for i in &mut self.0 {
            *i = !*i
        }
        self
    }
}

impl fmt::Display for TorrentIdError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TorrentIdError")
    }
}

impl std::error::Error for TorrentIdError {}

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

        if s.len() != TorrentId::LENGTH * 2 {
            return Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(s),
                &self,
            ));
        }
        match s.parse() {
            Ok(v) => Ok(v),
            Err(..) => Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(s),
                &self,
            )),
        }
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        // TODO: handle strings that get passed as bytes.
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
    Ok(&into[..copied_bytes])
}


#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct TorrentIdPrefix {
    pub base: TorrentId,
    pub prefix_len: u32,
}

#[test]
fn with_high_bits() {
    assert_eq!(TorrentId::with_high_bits(0), TorrentId::zero());
    assert_eq!(TorrentId::with_high_bits(1), "8000000000000000000000000000000000000000".parse::<TorrentId>().unwrap());

    assert_eq!(TorrentId::with_high_bits(159), "fffffffffffffffffffffffffffffffffffffffe".parse::<TorrentId>().unwrap());
    assert_eq!(TorrentId::with_high_bits(160), "ffffffffffffffffffffffffffffffffffffffff".parse::<TorrentId>().unwrap());
}

#[test]
fn foobar2() {
    let t0 = TorrentIdPrefix::new(TorrentId::max_value(), 1);
    assert_eq!(t0.base, "8000000000000000000000000000000000000000".parse().unwrap());

    let t1 = TorrentIdPrefix::new(TorrentId::max_value(), 2);
    assert_eq!(t1.longer().unwrap().base, "c000000000000000000000000000000000000000".parse().unwrap());
}

#[test]
fn TorrentIdPrefix_mask() {
    assert_eq!(
        TorrentId::max_value(),
        TorrentIdPrefix {
            base: TorrentId::zero(),
            prefix_len: 0,
        }.mask());
    assert_eq!(
        "7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF".parse::<TorrentId>().unwrap(),
        TorrentIdPrefix {
            base: TorrentId::zero(),
            prefix_len: 1,
        }.mask());
}



impl fmt::Display for TorrentIdPrefix {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.base.hex(), self.prefix_len)
    }
}

impl TorrentIdPrefix {
    pub fn zero() -> TorrentIdPrefix {
        TorrentIdPrefix {
            base: TorrentId::zero(),
            prefix_len: 0,
        }
    }

    pub fn new(tid: TorrentId, prefix_len: u32) -> TorrentIdPrefix {
        TorrentIdPrefix {
            base: tid & TorrentId::with_high_bits(prefix_len),
            prefix_len,
        }
    }

    pub fn to_range(&self) -> std::ops::RangeInclusive<TorrentId> {
        let max = self.base | (TorrentId::max_value() & !self.mask());
        self.base..=max
    }

    pub fn longer(&self) -> Option<TorrentIdPrefix> {
        if 160 <= self.prefix_len {
            return None;
        }
        let base = self.base & TorrentId::with_high_bits(self.prefix_len + 1);
        Some(TorrentIdPrefix {
            base,
            prefix_len: self.prefix_len + 1,
        })
    }

    pub fn contains(&self, id: &TorrentId) -> bool {
        self.prefix_len <= (self.base ^ *id).leading_zeros()
    }

    pub fn split(&self) -> (TorrentIdPrefix, TorrentIdPrefix) {
        let (bytes, bits) = (self.prefix_len / 8, self.prefix_len % 8);
        let mut new_base = self.base;
        let slice = new_base.as_mut_bytes();
        slice[bytes as usize] |= 0x80 >> bits;

        (
            TorrentIdPrefix {
                base: self.base,
                prefix_len: self.prefix_len + 1,
            },
            TorrentIdPrefix {
                base: new_base,
                prefix_len: self.prefix_len + 1,
            },
        )
    }

    pub fn split_swap(&self, target: &TorrentId) -> (TorrentIdPrefix, TorrentIdPrefix) {
        assert!(self.contains(target));
        let (left, right) = self.split();
        if left.contains(target) {
            (left, right)
        } else {
            (right, left)
        }
    }

    pub fn rand_within<R>(&self, rng: &mut R) -> TorrentId where R: rand::Rng {
        let mut randomized = TorrentId::zero();
        rng.fill_bytes(randomized.as_mut_bytes());
        let rand_mask = !TorrentId::with_high_bits(self.prefix_len);
        self.base | (rand_mask & randomized)
    }

    pub fn mask(&self) -> TorrentId {
        let mut buf = [0; 20];
        let mut set_ones_left = 160 - self.prefix_len;
        let mut byte_off = 19;
        while set_ones_left > 8 {
            set_ones_left -= 8;
            buf[byte_off] = 0xFF;
            byte_off -= 1;
        }
        buf[byte_off] = (((1_u32 << set_ones_left) - 1) & 0xFF) as u8;
        TorrentId(buf)
    }
}
