use std::fmt;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::collections::{BTreeMap, VecDeque};
use std::io::{self, Read};

use clap::{App, Arg, SubCommand};
use failure::{Fail, Fallible};

use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "dump-torrent-info";

struct TorrentMeta {
    announce: String,
    #[serde(rename="creation date")]
    creation_date: i64,
    info: TorrentMetaInfo,
}

struct TorrentMetaInfo {
    #[serde(rename="piece length")]
    piece_length: i64,
    pieces: i64,
    name: String,
    length: i64,
}

pub fn get_subcommand() -> App<'static, 'static> {
    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("Seed a torrent")
        .arg(
            Arg::with_name("torrent-file")
                .long("torrent-file")
                .value_name("FILE")
                .help("The torrent file to serve")
                .required(true)
                .takes_value(true),
        )
}

pub fn main(matches: &clap::ArgMatches) {
    let torrent_file = matches.value_of_os("torrent-file").unwrap();
    let torrent_file = Path::new(torrent_file).to_owned();

    println!("torrent_file = {:?}", torrent_file.display());

    let mut buffer = Vec::new();
    let mut file = File::open(&torrent_file).unwrap();
    file.read_to_end(&mut buffer).unwrap();

    let mut tokenizer = bencode::Tokenizer::new(&buffer[..]);
    while let bencode::IResult::Done(v) = tokenizer.next() {
        println!("{:?}", v);
    }

    // http://bttracker.debian.org:6969/announce

}