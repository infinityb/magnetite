pub struct UnmeasuredBitfield(Option<Vec<u8>>);
 
pub struct Bitfield {
    length: u32,
    buf: Vec<u8>,
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
        unimplemented!();
    }

    pub fn set(&mut self, idx: u32, val: bool) {
        unimplemented!();
    }
}
