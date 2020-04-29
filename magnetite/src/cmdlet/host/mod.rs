use std::fs::File;
use std::io::Read;

use clap::{App, Arg, SubCommand};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tracing::{event, Level};

use crate::model::config::build_storage_engine;
use crate::model::config::{Frontend, FrontendHost};

use crate::storage::remote_magnetite::start_server;

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

pub fn main(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    use crate::model::config::Config;

    let config = matches.value_of("config").unwrap();
    let mut cfg_fi = File::open(&config).unwrap();
    let mut cfg_by = Vec::new();
    cfg_fi.read_to_end(&mut cfg_by).unwrap();
    let config: Config = toml::de::from_slice(&cfg_by).unwrap();

    let mut rt = Runtime::new()?;
    let storage_engine = build_storage_engine(&mut rt, &config).unwrap();

    let mut futures = Vec::new();
    for fe in &config.frontends {
        match fe {
            Frontend::Host(ref host) => {
                let host: FrontendHost = host.clone();
                let storage_engine = storage_engine.clone();
                futures.push(async move {
                    let mut listener = TcpListener::bind(&host.bind_address).await.unwrap();
                    event!(
                        Level::INFO,
                        "host backend bind successful: {:?}",
                        host.bind_address
                    );
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
            }
        }
    }

    rt.block_on(futures::future::join_all(futures));

    Ok(())
}
