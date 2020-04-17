use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

use clap::{App, Arg, SubCommand};
use salsa20::stream_cipher::generic_array::GenericArray;
use salsa20::stream_cipher::{NewStreamCipher, SyncStreamCipher};
use salsa20::XSalsa20;

use crate::model::{TorrentMeta, TorrentMetaWrapped};
use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "assemble-mse-tome";

pub fn get_subcommand() -> App<'static, 'static> {
    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("Assemble a Magnetite Storage Engine Tome")
        .arg(
            Arg::with_name("torrent-file")
                .long("torrent-file")
                .value_name("FILE")
                .help("The torrent file to serve")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("source")
                .long("source")
                .value_name("PATH")
                .help("The source")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("target")
                .long("target")
                .value_name("PATH")
                .help("The target")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("secret")
                .long("secret")
                .value_name("VALUE")
                .help("The secret")
                .required(true)
                .takes_value(true),
        )
}

pub fn main(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    let torrent_file = matches.value_of_os("torrent-file").unwrap();
    let torrent_file = Path::new(torrent_file).to_owned();

    let source_path = matches.value_of_os("source").unwrap();
    let source_path = Path::new(source_path).to_owned();

    let target_path = matches.value_of_os("target").unwrap();
    let target_path = Path::new(target_path).to_owned();

    let secret = matches.value_of("secret").unwrap();

    let mut buffer = Vec::new();
    let mut file = File::open(&torrent_file).unwrap();
    file.read_to_end(&mut buffer).unwrap();

    let wrapped = TorrentMetaWrapped::from_bytes(&buffer).unwrap();

    let key = GenericArray::from_slice(secret.as_bytes());
    let mut nonce_data = [0; 24];
    nonce_data.copy_from_slice(wrapped.info_hash.as_bytes());

    let nonce = GenericArray::from_slice(&nonce_data[..]);
    let mut cipher = XSalsa20::new(&key, &nonce);
    let tm: TorrentMeta = bencode::from_bytes(&buffer).unwrap();

    let mut output = File::create(target_path).unwrap();

    let mut buf = vec![0; 32 * 1024];
    for file in &tm.info.files {
        if file.length == 0 {
            continue;
        }

        let mut file_path = source_path.to_owned();
        for p in &file.path {
            file_path.push(p);
        }

        let mut f = match File::open(&file_path) {
            Ok(f) => f,
            Err(err) => {
                panic!("error on {}: {}", file_path.display(), err);
            }
        };
        copy_and_encrypt(&mut buf[..], &mut cipher, &mut output, &mut f).unwrap();
        drop(f);
    }

    Ok(())
}

fn copy_and_encrypt<R, W>(b: &mut [u8], s: &mut XSalsa20, w: &mut W, r: &mut R) -> io::Result<()>
where
    R: Read,
    W: Write,
{
    loop {
        let length = r.read(&mut b[..])?;
        if length == 0 {
            break;
        }
        s.apply_keystream(&mut b[..length]);
        w.write_all(&b[..length])?;
    }

    Ok(())
}
