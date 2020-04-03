use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

use clap::{App, Arg, SubCommand};
use salsa20::stream_cipher::generic_array::GenericArray;
use salsa20::stream_cipher::{NewStreamCipher, SyncStreamCipher}; // SyncStreamCipherSeek
use salsa20::XSalsa20;
use sha1::{Digest, Sha1};

use crate::model::TorrentMeta;
use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "validate-mse-tome";

pub fn get_subcommand() -> App<'static, 'static> {
    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("Assemble a Magnetite Seeding Engine Tome")
        .arg(
            Arg::with_name("torrent-file")
                .long("torrent-file")
                .value_name("FILE")
                .help("The torrent file to serve")
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

pub fn hex<'a>(scratch: &'a mut [u8], input: &[u8]) -> Option<&'a str> {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

    if scratch.len() < input.len() * 2 {
        return None;
    }

    let mut sciter = scratch.iter_mut();
    for by in input {
        *sciter.next().unwrap() = HEX_CHARS[usize::from(*by >> 4)];
        *sciter.next().unwrap() = HEX_CHARS[usize::from(*by & 0xF)];
    }
    drop(sciter);

    Some(std::str::from_utf8(&scratch[..input.len() * 2]).unwrap())
}

pub fn main(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    let torrent_file = matches.value_of_os("torrent-file").unwrap();
    let torrent_file = Path::new(torrent_file).to_owned();

    let target_path = matches.value_of_os("target").unwrap();
    let target_path = Path::new(target_path).to_owned();

    let mut buffer = Vec::new();
    let mut file = File::open(&torrent_file).unwrap();
    file.read_to_end(&mut buffer).unwrap();

    let info_hash;
    let tm: bencode::Value = bencode::from_bytes(&buffer[..]).unwrap();
    if let bencode::Value::Dict(ref d) = tm {
        let mut hasher = Sha1::new();
        let info = bencode::to_bytes(d.get(&b"info"[..]).unwrap()).unwrap();
        hasher.input(&info[..]);
        info_hash = hasher.result();
    } else {
        panic!("failed to load torrent file");
    }

    let secret = matches.value_of("secret").unwrap();
    let key = GenericArray::from_slice(secret.as_bytes());
    let mut nonce_data = [0; 24];
    for (o, i) in nonce_data[4..].iter_mut().zip(info_hash.iter()) {
        *o = *i;
    }
    let nonce = GenericArray::from_slice(&nonce_data[..]);
    let mut cipher = XSalsa20::new(&key, &nonce);
    let tm: TorrentMeta = bencode::from_bytes(&buffer).unwrap();

    let mut input = File::open(&target_path).unwrap();

    let mut buf = vec![0; 32 * 1024];

    let mut info_hash_hex = [0; 40];
    let info_hash_hex_str = hex(&mut info_hash_hex[..], &info_hash).unwrap();
    println!(
        "validating {} with {}",
        target_path.display(),
        info_hash_hex_str
    );

    let mut ok_pieces = 0;
    let mut checked_pieces = 0;
    for (idx, sha) in tm.info.pieces.chunks(20).enumerate() {
        let mut hasher = Sha1::new();
        decrypt_and_sha(
            &mut buf,
            &mut cipher,
            tm.info.piece_length.into(),
            &mut hasher,
            &mut input,
        )
        .unwrap();

        let hash = hasher.result();

        let mut piece_sha_expect = [0; 40];
        let mut piece_sha_got = [0; 40];
        let expect = hex(&mut piece_sha_expect[..], &sha).unwrap();
        let got = hex(&mut piece_sha_got[..], &hash[..]).unwrap();

        if expect != got {
            println!("p{} {} != {}", idx, expect, got);
        } else {
            ok_pieces += 1;
        }
        checked_pieces += 1;
    }

    println!(
        "complete - {} of {} have passed.",
        ok_pieces, checked_pieces
    );

    Ok(())
}

fn decrypt_and_sha<R>(
    b: &mut [u8],
    s: &mut XSalsa20,
    plen: u64,
    hasher: &mut Sha1,
    r: &mut R,
) -> io::Result<()>
where
    R: Read,
{
    let mut rem_bytes: usize = plen as usize;
    while rem_bytes > 0 {
        let rbuflen = if rem_bytes < b.len() {
            rem_bytes
        } else {
            b.len()
        };

        let length = r.read(&mut b[..rbuflen])?;
        if length == 0 {
            break;
        }

        s.apply_keystream(&mut b[..length]);
        hasher.input(&b[..length]);
        rem_bytes -= length;
    }
    Ok(())
}
