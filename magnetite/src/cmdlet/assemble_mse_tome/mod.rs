use std::fs::File;
use std::path::Path;
use std::io::{self, Read, Write};

use clap::{App, Arg, SubCommand};
use salsa20::XSalsa20;
use salsa20::stream_cipher::generic_array::GenericArray;
use salsa20::stream_cipher::{NewStreamCipher, SyncStreamCipher}; // SyncStreamCipherSeek

use crate::model::{TorrentMeta, TorrentMetaWrapped};
use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "assemble-mse-tome";


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

pub fn main(matches: &clap::ArgMatches)  -> Result<(), failure::Error> {
    // danbooru2019-0 complete 5bba8ecc6ddb44f202f2d99a2d3e00e81b3ed0d6
    // danbooru2019-1 complete e06ed5c1a7305f6a1a8eedb81b3a8a521d383234
    // danbooru2019-2 complete 3216efd194c371df5de7724bb2d11391de046259
    // danbooru2019-3 complete 621200ef145d3094a0610258b84817afc49e553b
    // danbooru2019-4 complete f62017e4ccf8b68c6ead709e0ea02ebe2e96202a
    // danbooru2019-5 complete 80ccde7f60b920821b078a8e46e2074d2b542aef
    // danbooru2019-6 in-progr 18d8055fd8119c43c1b81fab16079b3acc0f86e0
    // danbooru2019-7 in-progr 
    // danbooru2019-8 complete bf7b67687d6fbd801158121ae74891734ded3f6d
    // danbooru2019-9 complete 9337d939a358c60d47479f7935e8045ed3b85d70
    // 
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
    for (o, i) in nonce_data[4..].iter_mut().zip(wrapped.info_hash.data.iter()) {
        *o = *i;
    }

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
    where R: Read, W: Write
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