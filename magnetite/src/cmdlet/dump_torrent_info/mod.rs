use std::fs::File;
use std::io::Read;
use std::path::Path;

use clap::{App, Arg, SubCommand};
use salsa20::stream_cipher::SyncStreamCipher;
use salsa20::XSalsa20;
use sha1::{Digest, Sha1};
use std::io::Write;
use std::path::PathBuf;

use crate::model::{TorrentID, TorrentMetaWrapped};
use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "dump-torrent-info";

pub fn get_subcommand() -> App<'static, 'static> {
    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("dump torrent metadata")
        .arg(
            Arg::with_name("torrent-file")
                .long("torrent-file")
                .value_name("FILE")
                .help("The torrent file to serve")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("tome")
                .long("tome")
                .value_name("FILE")
                .help("The tome file to read")
                .required(true)
                .takes_value(true),
        )
}

pub fn main(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    let torrent_file = matches.value_of_os("torrent-file").unwrap();
    let torrent_file = Path::new(torrent_file).to_owned();
    let tome = matches.value_of_os("tome").unwrap();
    let tome = Path::new(tome).to_owned();

    println!("torrent_file = {:?}", torrent_file.display());

    let mut by = Vec::new();
    let mut file = File::open(&torrent_file).unwrap();
    file.read_to_end(&mut by).unwrap();

    let tm: bencode::Value = bencode::from_bytes(&by[..]).unwrap();

    let mut infohash = TorrentID::zero();
    if let bencode::Value::Dict(ref d) = tm {
        let mut hasher = Sha1::new();
        let info = bencode::to_bytes(d.get(&b"info"[..]).unwrap()).unwrap();
        println!("info = {:?}", bencode::BinStr(&info[..]));
        hasher.input(&info[..]);
        let infohash_data = hasher.result();
        println!("infohash {:x}", infohash_data);
        infohash.as_mut_bytes().copy_from_slice(&infohash_data[..]);
    }

    Ok(())
}
