use std::fmt;

#[derive(Copy, Clone, Hash, Eq, PartialEq, Debug)]
pub struct Sha1([u8; 20]);

impl Sha1 {
    pub fn new(inner: [u8; 20]) -> Sha1 {
        Sha1(inner)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0[..]
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.0[..]
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<&[Sha1], ()> {
        use std::raw::{Slice, Repr};
        use std::mem::size_of;
        use std::slice;

        let sha1sz: usize = size_of::<Sha1>();

        let repr: Slice<u8> = bytes.repr();
        if repr.len % sha1sz > 0 {
            return Err(());
        }
        
        let data = repr.data as *const Sha1;
        Ok(unsafe { slice::from_raw_parts(data, repr.len / sha1sz) })
    }
}

impl fmt::Display for Sha1 {
    fn fmt(&self, wri: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        for &byte in self.0.iter() {
            try!(write!(wri, "{:02x}", byte));
        }
        Ok(())
    }
    
}