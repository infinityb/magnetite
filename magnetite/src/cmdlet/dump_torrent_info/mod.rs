use std::fs::File;
use std::io::Read;
use std::path::Path;

use clap::{App, Arg, SubCommand};
use sha1::{Digest, Sha1};

use magnetite_common::TorrentId;

use crate::model::TorrentMeta;
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
}

pub fn main(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    let torrent_file = matches.value_of_os("torrent-file").unwrap();
    let torrent_file = Path::new(torrent_file).to_owned();

    let mut by = Vec::new();
    let mut file = File::open(&torrent_file).unwrap();
    file.read_to_end(&mut by).unwrap();

    let tm: bencode::Value = bencode::from_bytes(&by[..]).unwrap();

    let mut infohash = TorrentId::zero();
    if let bencode::Value::Dict(ref d) = tm {
        let mut hasher = Sha1::new();
        let info = bencode::to_bytes(d.get(&b"info"[..]).unwrap()).unwrap();
        hasher.input(&info[..]);
        let infohash_data = hasher.result();
        println!("Info hash: {:x}", infohash_data);
        infohash.as_mut_bytes().copy_from_slice(&infohash_data[..]);
    }

    let tm: TorrentMeta = bencode::from_bytes(&by[..]).unwrap();
    println!("Name: {}", tm.info.name);
    println!("Files:");
    for file in tm.info.files {
        println!("  {} {}", file.length, file.path.display());
    }
    println!("");

    Ok(())
}
