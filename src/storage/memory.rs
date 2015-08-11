use std::io::{self};
use std::slice::bytes::copy_memory;

use ::mt::common::{Bitfield, Storage, TorrentInfo};
use ::mt::util::{Sha1, PieceLength};

/// Just store the whole torrent in a contiguous chunk of memory.
pub struct MemoryStorage {
    pieces_finished: Bitfield,
    piece_len: PieceLength,
    pieces: Vec<Sha1>,
    data: Vec<u8>,
}

fn digest(buf: &[u8]) -> Sha1 {
    let mut m = ::sha1::Sha1::new();
    m.update(&buf);

    let mut buf = [0; 20];
    let digest = m.digest();
    copy_memory(&digest, &mut buf);
    Sha1::new(buf)
}

impl Storage for MemoryStorage {
    fn check(&mut self) -> io::Result<Bitfield> {
        for (i, _) in self.pieces.iter().enumerate() {
            let i = i as u32;
            let is_valid = try!(self.check_piece(i));
            self.pieces_finished.set(i, is_valid);
        }
        Ok(self.pieces_finished.clone())
    }

    fn complete_pieces(&mut self) -> io::Result<u32> {
        Ok(self.pieces_finished.iter().map(|is_set| is_set as u32).sum())
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        if data.len() == 0 {
            return Ok(());
        }
        if offset as usize + data.len() < self.data.len() {
            return Err(io::Error::new(io::ErrorKind::Other, "Out of bounds"));
        }
        copy_memory(data, &mut self.data[offset as usize..]);

        let piece_num = self.piece_len.piece_num(offset);
        if try!(self.check_piece(piece_num)) {
            self.pieces_finished.set(piece_num, true)
        }

        Ok(())
    }
}

impl MemoryStorage {
    pub fn new(length: u64, pieces: Vec<Sha1>, piece_len_shl: u8) -> MemoryStorage {
        MemoryStorage {
            pieces_finished: Bitfield::new_empty(pieces.len() as u32),
            piece_len: PieceLength::new(piece_len_shl),
            pieces: pieces,
            data: Vec::with_capacity(length as usize),
        }
    }

    fn check_piece(&self, piece: u32) -> io::Result<bool> {
        let offset = self.piece_len.offset(piece) as usize;
        let piece_length = self.piece_len.length();
        let piece_data = &self.data[offset..][..piece_length as usize];
        Ok(digest(piece_data) == self.pieces[piece as usize])
    }
}

