use std::ops::{BitAnd, BitXor};


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
            };
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
            };
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

    pub fn copy_from_slice(&mut self, slice: &[u8]) {
        assert_eq!(self.data.len(), slice.len());
        self.set_count = 0;
        for o in slice {
            self.set_count += o.count_ones() as u32;
        }
        self.data.copy_from_slice(slice);
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
