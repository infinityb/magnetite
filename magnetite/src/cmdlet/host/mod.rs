use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{App, Arg, SubCommand};
use salsa20::stream_cipher::generic_array::GenericArray;
use salsa20::stream_cipher::NewStreamCipher;
use salsa20::XSalsa20;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tracing::{event, Level};

use crate::model::{TorrentID, TorrentMetaWrapped};
use crate::storage::disk_cache_layer::CacheWrapper;
use crate::storage::remote_magnetite::start_server;
use crate::storage::{piece_file, state_wrapper, PieceFileStorageEngine, StateWrapper};
use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "host";

pub fn get_subcommand() -> App<'static, 'static> {
    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("Host a torrent via magnetite remote protocol")
        .arg(
            Arg::with_name("config")
                .long("config")
                .value_name("FILE")
                .help("config file")
                .required(true)
                .takes_value(true),
        )
}

pub fn get_torrent_salsa(crypto_secret: &str, info_hash: &TorrentID) -> Option<XSalsa20> {
    if crypto_secret.len() > 0 {
        let mut nonce_data = [0; 24];
        nonce_data[4..].copy_from_slice(info_hash.as_bytes());
        let nonce = GenericArray::from_slice(&nonce_data[..]);
        let key = GenericArray::from_slice(crypto_secret.as_bytes());
        Some(XSalsa20::new(&key, &nonce))
    } else {
        None
    }
}

pub fn main(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    #[derive(Serialize, Deserialize)]
    struct Config {
        torrents: Vec<TorrentFactory>,
    }

    #[derive(Serialize, Deserialize)]
    struct TorrentFactory {
        torrent_file: PathBuf,
        source_file: PathBuf,
        secret: String,
    }

    let config = matches.value_of("config").unwrap();
    let mut cfg_fi = File::open(&config).unwrap();
    let mut cfg_by = Vec::new();
    cfg_fi.read_to_end(&mut cfg_by).unwrap();
    let config: Config = toml::de::from_slice(&cfg_by).unwrap();

    let mut rt = Runtime::new()?;

    let mut pf_builder = PieceFileStorageEngine::builder();
    let mut state_builder = StateWrapper::builder();

    for s in config.torrents.iter() {
        let mut fi = File::open(&s.torrent_file).unwrap();
        let mut by = Vec::new();
        fi.read_to_end(&mut by).unwrap();

        let pf = File::open(&s.source_file).unwrap();
        let mut torrent = TorrentMetaWrapped::from_bytes(&by).unwrap();

        // deallocate files, we don't need them for seed on piece file engine
        torrent.meta.info.files = Vec::new();

        let tm = Arc::new(torrent);

        let piece_count = tm.piece_shas.len() as u32;

        let mut crypto = None;
        if let Some(salsa) = get_torrent_salsa(&s.secret, &tm.info_hash) {
            crypto = Some(salsa);
        }

        pf_builder.register_info_hash(
            &tm.info_hash,
            piece_file::Registration {
                piece_count: piece_count,
                crypto: crypto,
                piece_file: pf.into(),
            },
        );

        state_builder.register_info_hash(
            &tm.info_hash,
            state_wrapper::Registration {
                total_length: tm.total_length,
                piece_length: tm.meta.info.piece_length,
                piece_shas: tm.piece_shas.clone().into(),
            },
        );

        println!("added info_hash: {:?}", tm.info_hash);
    }

    let storage_engine = pf_builder.build();
    let cache = CacheWrapper::build_with_capacity_bytes(3 * 1024 * 1024 * 1024);
    let cache_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open("magnetite.cache")
        .unwrap();

    let storage_engine = state_builder.build(cache.build(cache_file.into(), storage_engine));

    rt.block_on(async {
        let mut listener = TcpListener::bind("[::]:17862").await.unwrap();
        loop {
            let storage_engine = storage_engine.clone();
            let (socket, addr) = listener.accept().await.unwrap();
            event!(Level::INFO, "got connection from {:?}", addr);

            tokio::spawn(async move {
                if let Err(err) = start_server(socket, storage_engine).await {
                    event!(Level::ERROR, "error: {}", err);
                }
            });
        }
    });

    Ok(())
}
