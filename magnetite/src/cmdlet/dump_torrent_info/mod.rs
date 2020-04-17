use std::fs::File;
use std::io::Read;
use std::path::Path;

use clap::{App, Arg, SubCommand};
use sha1::{Digest, Sha1};

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

    println!("torrent_file = {:?}", torrent_file.display());

    let mut buffer = Vec::new();
    let mut file = File::open(&torrent_file).unwrap();
    file.read_to_end(&mut buffer).unwrap();

    let tm: bencode::Value = bencode::from_bytes(&buffer[..]).unwrap();
    if let bencode::Value::Dict(ref d) = tm {
        let mut hasher = Sha1::new();
        let info = bencode::to_bytes(d.get(&b"info"[..]).unwrap()).unwrap();
        println!("info = {:?}", bencode::BinStr(&info[..]));
        hasher.input(&info[..]);
        println!("{:x}", hasher.result());
    }

    // let mut tokenizer = bencode::Tokenizer::new(&buffer[..]);

    // let stdout = io::stdout();
    // let mut stdout = BufWriter::new(stdout.lock());
    // while let iresult::IResult::Done(v) = tokenizer.next(false) {
    //     writeln!(&mut stdout, "{:?}", v).unwrap();
    // }

    Ok(())
}
