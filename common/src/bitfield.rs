use std::{iter, slice};

#[derive(Clone, Debug)]
pub struct UnmeasuredBitfield(Option<Vec<u8>>);

#[derive(Clone, Debug)]
pub struct Bitfield {
    length: u32,
    buf: Vec<u8>,
}

#[inline(always)]
fn bit_split(val: u32) -> (u32, u32) {
    (val >> 3, 7 - (val & 0x7))
}


// TODO(sell): errors
// out of range bits must not be set.
// the vector must be long enough to contain all bits
fn validate_bitfield(buf: &[u8], length: u32) -> Result<(), &'static str> {
    Ok(())
}

impl UnmeasuredBitfield {
    pub fn new(buf: Vec<u8>) -> UnmeasuredBitfield {
        UnmeasuredBitfield(Some(buf))
    }

    pub fn empty() -> UnmeasuredBitfield {
        UnmeasuredBitfield(None)
    }

    pub fn measure(self, length: u32) -> Result<Bitfield, &'static str> {
        let UnmeasuredBitfield(mbuf) = self;
        match mbuf {
            Some(buf) => Bitfield::new(length, buf),
            None => Ok(Bitfield::new_empty(length)),
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        self.0.as_ref().map(|v| &**v)
    }
}

impl Bitfield {
    pub fn new(length: u32, buf: Vec<u8>) -> Result<Bitfield, &'static str> {
        try!(validate_bitfield(&buf, length));
        Ok(Bitfield {
            length: length,
            buf: buf,
        })
    }

    // A correctly sized Bitfield with all bits set to false
    pub fn new_empty(length: u32) -> Bitfield {
        Bitfield {
            length: length,
            buf: vec![0; (length as usize + 7) / 8],
        }
    }

    pub fn get(&self, idx: u32) -> bool {
        let (byte, bit) = bit_split(idx);
        ((self.buf[byte as usize] >> bit) & 1) == 1
    }

    pub fn set(&mut self, idx: u32, val: bool) {
        let (byte, bit) = bit_split(idx);
        let our_bit = 1 << bit;
        let mut new = self.buf[byte as usize] & (0xFF ^ our_bit);
        if val {
            new = new | our_bit;
        }
        self.buf[byte as usize] = new;
    }

    pub fn len(&self) -> u32 {
        self.length
    }

    pub fn iter<'a>(&'a self) -> Iter<'a> {
        Iter {
            length: self.length,
            bit_idx: 0,
            buf: &self.buf,
        }
    }

    pub fn set_bits(&self) -> u32 {
        self.iter().map(|s| s as u32).sum()
    }
}


pub struct Iter<'a> {
    length: u32,
    bit_idx: u32,
    buf: &'a [u8],
}

impl<'a> Iterator for Iter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<bool> {
        if self.length == self.bit_idx {
            return None;
        }
        let (byte, bit) = bit_split(self.bit_idx);
        let rv = ((self.buf[byte as usize] >> bit) & 1) == 1;
        self.bit_idx += 1;
        Some(rv)
    }
}

pub struct SeenMap {
    buf: Vec<u32>,
}

impl SeenMap {
    pub fn new(length: u32) -> SeenMap {
        SeenMap {
            buf: vec![0; length as usize],
        }
    }

    pub fn incr(&mut self, idx: u32) {
        let count = &mut self.buf[idx as usize];
        *count = count.saturating_add(1);
    }

    pub fn decr(&mut self, idx: u32) {
        let count = &mut self.buf[idx as usize];
        *count = count.saturating_sub(1);
    }

    pub fn add(&mut self, bitfield: &Bitfield) {
        assert_eq!(self.buf.len(), bitfield.len() as usize);
        for (count, is_set) in self.buf.iter_mut().zip(bitfield.iter()) {
            if is_set {
                *count = count.saturating_add(1);
            }
        }
    }

    pub fn sub(&mut self, bitfield: &Bitfield) {
        assert_eq!(self.buf.len(), bitfield.len() as usize);
        for (count, is_set) in self.buf.iter_mut().zip(bitfield.iter()) {
            if is_set {
                *count = count.saturating_sub(1);
            }
        }
    }

    pub fn get(&self, idx: u32) -> u32 {
        self.buf[idx as usize]
    }

    pub fn iter<'a>(&'a self) -> iter::Cloned<slice::Iter<'a, u32>> {
        self.buf.iter().cloned()
    }

    pub fn iter_normalized<'a>(&'a self) -> IterScaled<'a> {
        let max = self.buf.iter().cloned().max().unwrap_or(0);
        IterScaled { iter: self.iter(), max: max }
    }
}

pub struct IterScaled<'a> {
    iter: iter::Cloned<slice::Iter<'a, u32>>,
    max: u32,
}

impl<'a> Iterator for IterScaled<'a> {
    type Item = f32;

    fn next(&mut self) -> Option<f32> {
        self.iter.next().map(|v| {
            if self.max == 0 {
                0.0
            } else {
                v as f32 / self.max as f32
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{SeenMap, Bitfield};

    #[test]
    fn test_basics() {
        let mut bitfield = Bitfield::new_empty(41);
        assert_eq!(bitfield.set_bits(), 0);

        bitfield.set(0, true);
        assert_eq!(bitfield.set_bits(), 1);
        for (idx, isset) in bitfield.iter().enumerate() {
            assert_eq!(isset, idx == 0);
        }

        bitfield.set(0, false);
        assert_eq!(bitfield.set_bits(), 0);
        for (idx, isset) in bitfield.iter().enumerate() {
            assert_eq!(isset, false);
        }

        bitfield.set(39, true);
        assert_eq!(bitfield.set_bits(), 1);
        for (idx, isset) in bitfield.iter().enumerate() {
            assert_eq!(isset, idx == 39);
        }

        bitfield.set(39, false);
        assert_eq!(bitfield.set_bits(), 0);
        for (idx, isset) in bitfield.iter().enumerate() {
            assert_eq!(isset, false);
        }
    }


    #[test]
    fn test_seen_map() {
        let mut seen_map = SeenMap::new(41);

        let mut bitfield = Bitfield::new_empty(41);
        bitfield.set(0, true);
        seen_map.add(&bitfield);

        let mut bitfield = Bitfield::new_empty(41);
        bitfield.set(1, true);
        seen_map.add(&bitfield);

        assert_eq!(seen_map.get(0), 1);
        assert_eq!(seen_map.get(1), 1);
        assert_eq!(seen_map.get(2), 0);
    }
}