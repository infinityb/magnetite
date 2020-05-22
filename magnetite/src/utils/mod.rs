use std::fmt;

mod bytes_cow;
pub mod close_waiter;
#[cfg(target_os = "linux")]
mod owned_fd;

pub use self::bytes_cow::BytesCow;
#[cfg(target_os = "linux")]
pub use self::owned_fd::OwnedFd;

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

// --

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

pub struct BinStrBuf(pub Vec<u8>);

impl fmt::Debug for BinStrBuf {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let bin_str = BinStr(&self.0);
        write!(f, "{:?}.to_vec()", bin_str)
    }
}

// --

pub struct ByteSize(pub u64);

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        const UNIT_NAMES: &[&str] = &["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei"];
        let mut unit_acc = self.0;
        let mut unit_num = 0;
        while 1024 <= unit_acc && unit_num < UNIT_NAMES.len() {
            unit_acc /= 1024;
            unit_num += 1;
        }

        let value = match unit_num {
            0 => self.0 as f64,
            1 => self.0 as f64 / 1024.0,
            _ => {
                // use integer division first, to ensure that the value
                // fits into floats
                let unit_denom_int = 1 << (10 * (unit_num - 1));
                (self.0 / unit_denom_int) as f64 / 1024.0
            }
        };

        if unit_num == 0 {
            write!(f, "{}B", value)
        } else {
            write!(f, "{:.1}{}B", value, UNIT_NAMES[unit_num])
        }
    }
}
