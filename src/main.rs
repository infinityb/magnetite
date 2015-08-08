#![feature(slice_bytes)]
extern crate byteorder;
extern crate mio;
extern crate bytes;

mod connection;
mod message_types;
mod bitfield;

use bitfield::Bitfield;

fn main() {
    println!("Hello, world!");
}

static pstr: &'static [u8] = b"BitTorrent protocol";

pub struct Torrent {
    info_hash: [u8; 20],
    pieces: Bitfield,
    // want_map: HashMap<u32, u32>,
}