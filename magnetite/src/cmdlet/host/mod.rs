use clap::{App, Arg, SubCommand};
use tracing::{event, Level};

use crate::storage::remote_magnetite::start_remote_magnetite_host_service;
use crate::storage::root_storage_service::{start_piece_storage_engine, RootStorageOpts};
use crate::storage::state_holder_system::state_holder_system;
use crate::CommonInit;

use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "host";

pub fn get_subcommand() -> App<'static, 'static> {
    let cmd = SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("Host a torrent via magnetite remote protocol")
        .arg(
            Arg::with_name("host-bind-address")
                .long("host-bind-address")
                .value_name("ADDRESS")
                .help("host bind address")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("bittorrent-bind-address")
                .long("bittorrent-bind-address")
                .value_name("ADDRESS")
                .help("bittorrent bind address")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("webserver-bind-address")
                .long("webserver-bind-address")
                .value_name("ADDRESS")
                .help("webserver bind address")
                .takes_value(true),
        );

    #[cfg(feature = "with-fuse")]
    let cmd = cmd.arg(
        Arg::with_name("fuse-mount-path")
            .long("fuse-mount-path")
            .value_name("ADDRESS")
            .help("fuse mount path")
            .takes_value(true),
    );

    cmd
}

pub async fn main(
    common: CommonInit,
    matches: &clap::ArgMatches<'_>,
) -> Result<(), failure::Error> {
    let common2 = common.clone();
    let mut futures = Vec::new();

    event!(Level::DEBUG, "starting magnetite host");

    let opts = RootStorageOpts {
        concurrent_requests: 10,
        concurrent_open_files: 64,
        memory_cache_size_bytes: 128 * 1024 * 1024,
        disk_cache_size_bytes: 20 * 1024 * 1024 * 1024,
        disk_crypto_secret: "".to_string(),
    };
    futures.push(start_piece_storage_engine(common.clone(), opts));
    futures.push(state_holder_system(common.clone()));

    if let Some(b) = matches.value_of("host-bind-address") {
        futures.push(start_remote_magnetite_host_service(common.clone(), b));
    }
    if let Some(b) = matches.value_of("bittorrent-bind-address") {
        futures.push(start_remote_magnetite_host_service(common.clone(), b));
    }

    unimplemented!("load databases"); // v 
    // futures.push(load_databases(common2));

    for v in futures::future::join_all(futures).await {
        v?;
    }

    event!(Level::DEBUG, "shut down magnetite host");

    Ok(())
}
