pub struct UnmeasuredBitfield(Option<Vec<u8>>);

#[derive(Clone)]
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

#[cfg(test)]
mod tests {
    use super::Bitfield;

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
}