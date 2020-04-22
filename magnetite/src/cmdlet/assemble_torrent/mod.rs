// use std::fs::File;
// use std::io::{self, Read, Write};
// use std::path::Path;

use clap::{App, Arg, SubCommand};

use crate::model::{TorrentMeta, TorrentMetaInfo, TorrentMetaInfoFile};
use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "assemble-torrent";

pub fn get_subcommand() -> App<'static, 'static> {
    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("Assemble a torrent")
        .arg(
            Arg::with_name("torrent-file")
                .long("torrent-file")
                .value_name("PATH")
                .help("The output torrent file")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("source-file")
                .long("source-file")
                .value_name("PATH")
                .help("Files to include")
                .required(true)
                .multiple(true)
                .takes_value(true),
        )
    // TODO: glob?
}

pub fn main(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    println!("Unimplemented! Passed {:?}", &matches);

    let filepaths = &matches.value_of_os("source-file").unwrap();

    let file = TorrentMetaInfoFile {
        length: 0,
        path: filepaths.into(),
    };

    let torrent_info = TorrentMetaInfo {
        files: vec![file],
        piece_length: 0,
        pieces: vec![],
        name: "".into(),
    };

    let torrent = TorrentMeta {
        announce: "".into(),
        announce_list: vec![],
        comment: "".into(),
        created_by: "".into(),
        creation_date: 0,
        info: torrent_info,
    };

    println!("TorrentMeta: {:?}", &torrent);

    Ok(())
}
