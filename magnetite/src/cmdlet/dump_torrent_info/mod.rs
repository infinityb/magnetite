use std::fs::File;
use std::io::Read;
use std::path::Path;

use clap::{App, Arg, SubCommand};
use sha1::{Digest, Sha1};

use magnetite_common::TorrentId;

use crate::model::TorrentMeta;
use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "dump-torrent-info";

pub fn get_subcommand() -> App<'static> {
    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("dump torrent metadata")
        .arg(
            Arg::with_name("torrent-file")
                .allow_invalid_utf8(true)
                .long("torrent-file")
                .value_name("FILE")
                .help("The torrent file to serve")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("file-paths")
                .long("file-paths")
                .action(clap::ArgAction::SetTrue)
                .help("Only dump file paths")
        )
        .arg(
            Arg::with_name("name")
                .long("name")
                .action(clap::ArgAction::SetTrue)
                .help("Only dump the name")
        )
        .arg(
            Arg::with_name("info-hash")
                .long("info-hash")
                .action(clap::ArgAction::SetTrue)
                .help("Only dump the info-hash")
        )
        .arg(
            Arg::with_name("zero")
                .long("zero")
                .action(clap::ArgAction::SetTrue)
                .help("Use null as delimiter")
        )
}

#[cfg(unix)]
mod imp {
    use std::path::Path;
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    pub fn print_path_w_newline(wri: &mut dyn std::io::Write, p: &Path) -> Result<(), failure::Error> {
        let s: &OsStr = p.as_ref();
        let b = s.as_bytes();
        if b.iter().any(|x| *x == b'\n') {
            return Err(failure::format_err!("file-name contains newline but we are delimiting by newlines."));
        }
        wri.write_all(b)?;
        wri.write_all(b"\n")?;
        Ok(())
    }


    pub fn print_path_w_zero(wri: &mut dyn std::io::Write, p: &Path) -> Result<(), failure::Error> {
        let s: &OsStr = p.as_ref();
        let b = s.as_bytes();
        wri.write_all(b)?;
        wri.write_all(&[0])?;
        Ok(())
    }
}

pub async fn main(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    let torrent_file = matches.value_of_os("torrent-file").unwrap();
    let torrent_file = Path::new(torrent_file).to_owned();

    let mut by = Vec::new();
    let mut file = File::open(&torrent_file).unwrap();
    file.read_to_end(&mut by).unwrap();

    let tm: bencode::Value = bencode::from_bytes(&by[..]).unwrap();
    let zero = matches.get_flag("zero");
    if matches.get_flag("file-paths") {
        if cfg!(windows) {
            eprintln!("I dunno how to delimit paths properly on this chaotic OS - aborting");

            return Err(failure::format_err!("bad argument combination"));
        }

        let mut writer: fn(wri: &mut dyn std::io::Write, p: &Path) -> Result<(), failure::Error> = imp::print_path_w_newline;
        if zero {
           writer = imp::print_path_w_zero;
        }

        let mut stdout = std::io::stdout();
        let tm: TorrentMeta = bencode::from_bytes(&by[..]).unwrap();
        for file in tm.info.files {
            writer(&mut stdout, &file.path)?;
        } 

        return Ok(());
    }
    

    if zero {
        eprintln!("--zero only supported when dumping file-paths for now");

        return Err(failure::format_err!("bad argument combination"));
    }

    let mut infohash = TorrentId::zero();
    if let bencode::Value::Dict(ref d) = tm {
        let mut hasher = Sha1::new();
        let info = bencode::to_bytes(d.get(&b"info"[..]).unwrap()).unwrap();
        hasher.input(&info[..]);
        let infohash_data = hasher.result();
        infohash.as_mut_bytes().copy_from_slice(&infohash_data[..]);
    } else {
        return Err(failure::format_err!("bad torrent file"));
    }

    let tm: TorrentMeta = bencode::from_bytes(&by[..]).unwrap();
    
    if matches.get_flag("info-hash") {
        println!("{}", infohash.hex());
        return Ok(())
    }
    if matches.get_flag("name") {
        println!("{}", tm.info.name);
        return Ok(())
    }

    println!("Info hash: {}", infohash.hex());
    println!("Name: {}", tm.info.name);
    println!("Files:");
    for file in tm.info.files {
        println!("  {} {}", file.length, file.path.display());
    }
    println!("Pieces:");
    for piece in tm.info.pieces {
        println!("  {}", piece.hex());
    }
    println!("");

    Ok(())
}
