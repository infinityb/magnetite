use std::fmt;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::collections::{BTreeMap, VecDeque};
use std::io;

use clap::{App, Arg, SubCommand};
use failure::{Fail, Fallible};

use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "seed";

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
        .arg(
            Arg::with_name("piece-file")
                .long("piece-file")
                .value_name("piece-file")
                .help("The piece file to serve")
                .required(true)
                .takes_value(true),
        )
}

pub fn main(matches: &clap::ArgMatches) {
    let torrent_file = matches.value_of_os("torrent-file").unwrap();
    let torrent_file = Path::new(torrent_file).to_owned();

    let piece_file = matches.value_of_os("piece-file").unwrap();
    let piece_file = Path::new(piece_file).to_owned();

    println!("torrent_file = {:?}", torrent_file.display());
    println!("piece_file = {:?}", piece_file.display());
}