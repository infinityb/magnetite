use std::fmt::{self, Write};

pub fn hex<'a>(scratch: &'a mut [u8], input: &[u8]) -> Option<&'a str> {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

    if scratch.len() < input.len() * 2 {
        return None;
    }

    let mut sciter = scratch.iter_mut();
    for by in input {
        *sciter.next().unwrap() = HEX_CHARS[usize::from(*by >> 4)];
        *sciter.next().unwrap() = HEX_CHARS[usize::from(*by & 0xF)];
    }
    drop(sciter);

    Some(std::str::from_utf8(&scratch[..input.len() * 2]).unwrap())
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
pub struct HexStr<'a>(pub &'a [u8]);

impl<'a> fmt::Debug for HexStr<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
        f.write_str("dehex(\"")?;
        for c in self.0.iter() {
            f.write_char(HEX_CHARS[usize::from(c >> 4)] as char)?;
            f.write_char(HEX_CHARS[usize::from(c & 0xF)] as char)?;
        }
        f.write_str("\")")?;
        Ok(())
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
pub struct BinStr<'a>(pub &'a [u8]);

impl fmt::Debug for BinStr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "b\"")?;
        for &b in self.0 {
            match b {
                b'\0' => write!(f, "\\0")?,
                b'\n' => write!(f, "\\n")?,
                b'\r' => write!(f, "\\r")?,
                b'\t' => write!(f, "\\t")?,
                b'\\' => write!(f, "\\\\")?,
                b'"' => write!(f, "\\\"")?,
                _ if 0x20 <= b && b < 0x7F => write!(f, "{}", b as char)?,
                _ => write!(f, "\\x{:02x}", b)?,
            }
        }
        write!(f, "\"")?;
        Ok(())
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct BinStrBuf(pub Vec<u8>);

impl fmt::Debug for BinStrBuf {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let bin_str = BinStr(&self.0);
        write!(f, "{:?}.to_vec()", bin_str)
    }
}
