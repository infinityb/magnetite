extern crate libc;
extern crate nix;

use std::fmt;
use std::ops;
use std::os::unix::io::AsRawFd;

mod constants;
mod ffi;

#[derive(Copy, Clone)]
pub struct Mode(i32);

static KEEP_SIZE_NAME: &'static str = "FALLOC_FL_KEEP_SIZE";

static PUNCH_HOLE_NAME: &'static str = "FALLOC_FL_PUNCH_HOLE";

static COLLAPSE_RANGE_NAME: &'static str = "FALLOC_FL_COLLAPSE_RANGE";

static ZERO_RANGE_NAME: &'static str = "FALLOC_FL_ZERO_RANGE";

impl Mode {
    /// No flags set
    pub fn empty() -> Mode {
        Mode(0)
    }

    /// Don't extend size of file even if offset + len is greater than file size.
    pub fn keep_size() -> Mode {
        Mode(constants::KEEP_SIZE)
    }

    /// Create a hole in the file, also sets FALLOC_FL_KEEP_SIZE
    pub fn punch_hole() -> Mode {
        Mode(constants::PUNCH_HOLE | constants::KEEP_SIZE)
    }

    /// Remove a range of a file without leaving a hole.
    pub fn collapse_range() -> Mode {
        Mode(constants::COLLAPSE_RANGE)
    }

    /// Convert a range of a file to zeros
    pub fn zero_range() -> Mode {
        Mode(constants::ZERO_RANGE)
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut names: Vec<&str> = Vec::new();
        let Mode(mode) = *self;

        if mode & constants::KEEP_SIZE > 0 {
            names.push(KEEP_SIZE_NAME);
        }
        if mode & constants::PUNCH_HOLE > 0 {
            names.push(PUNCH_HOLE_NAME);
        }
        if mode & constants::COLLAPSE_RANGE > 0 {
            names.push(COLLAPSE_RANGE_NAME);
        }
        if mode & constants::ZERO_RANGE > 0 {
            names.push(ZERO_RANGE_NAME);
        }

        write!(f, "Mode[{}]", names.join(" | "))
    }
}

impl ops::BitOr for Mode {
    type Output = Mode;

    fn bitor(self, rhs: Mode) -> Self::Output {
        Mode(ops::BitOr::bitor(self.0, rhs.0))
    }
}

pub fn fallocate<F: AsRawFd>(
    fd: &mut F,
    mode: Mode,
    offset: i64,
    len: i64,
) -> Result<(), nix::Error> {
    let rv = unsafe { ffi::fallocate(fd.as_raw_fd(), mode.0, offset, len) };
    match rv {
        0 => Ok(()),
        -1 => Err(nix::Error::last()),
        _ => panic!("fallocate returned unhandled value: {}", rv),
    }
}

#[cfg(test)]
mod tests {
    use super::{fallocate, Mode};
    use std::fs::OpenOptions;
    use std::io::{self, Read, Seek, SeekFrom, Write};

    #[test]
    fn it_works() {
        let file_length = 1024 * 1024;
        let punch_length = 16 * 1024;

        let test = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("/tmp/fallocate-test")
            .unwrap();

        let mut test = {
            let mut buffered = io::BufWriter::new(test);
            for _ in 0..file_length {
                buffered.write_all(b"\xCF").unwrap();
            }
            buffered.into_inner().unwrap()
        };

        if let Err(err) = fallocate(&mut test, Mode::punch_hole(), 0, punch_length) {
            panic!("fallocate error: {:?}", err);
        }

        test.seek(SeekFrom::Start(0)).unwrap();
        let mut byte_iterator = test.bytes();
        for i in 0..file_length {
            let byte = byte_iterator.next().unwrap().unwrap();
            let want = if i < punch_length { 0 } else { 0xCF };
            assert_eq!(byte, want);
        }
    }
}
