#![feature(slice_bytes)]
extern crate byteorder;
extern crate mio;
extern crate bytes;

use std::path::PathBuf;

mod util;
mod connection;
mod message_types;
mod bitfield;

use bitfield::Bitfield;

fn main() {
    println!("Hello, world!");
}

pub trait Storage {
    //
}

/// Lowest common denominator storage
pub struct DefaultStorage {
    base_path: PathBuf,
}

pub struct Torrent {
    info: connection::TorrentInfo,
    pieces: Bitfield,

    // Dynamic dispatch because disks are expensive anyway.
    storage: Box<Storage>,

    // want_map: HashMap<u32, u32>,
}