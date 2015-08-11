use std::io::{self};

use super::Bitfield;

pub trait Storage {
    fn check(&mut self) -> io::Result<Bitfield>;
    fn complete_pieces(&mut self) -> io::Result<u32>;
    fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()>;
}
