use std::any::Any;
use std::fs::File;
use std::io::Read;

use clap::{App, AppSettings, Arg, SubCommand};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tonic::transport::channel::Channel;
use tracing::{event, Level};

use magnetite_common::TorrentId;

use crate::model::config::{
    build_storage_engine, Frontend, FrontendHost, StorageEngineServicesWithMeta,
};
use crate::storage::remote_magnetite::start_server;
use crate::utils::ByteSize;
use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "ctl";

pub const SUBCOMMAND_ADD_TORRENT_NAME: &str = "add-torrent";

pub const SUBCOMMAND_REMOVE_TORRENT_NAME: &str = "remove-torrent";

pub const SUBCOMMAND_LIST_TORRENTS_NAME: &str = "list-torrents";

use crate::control::api::{
    add_torrent_request::{BackingFile, TorrentFile},
    torrent_host_client::TorrentHostClient,
    AddTorrentRequest, AddTorrentResponse, ListTorrentsRequest, ListTorrentsResponse,
    RemoveTorrentRequest, RemoveTorrentResponse, TorrentDataSourceMultiFile,
};

pub fn get_subcommand() -> App<'static, 'static> {
    let add_torrent = SubCommand::with_name(SUBCOMMAND_ADD_TORRENT_NAME)
        .version(CARGO_PKG_VERSION)
        .about("Add a torrent")
        .arg(
            Arg::with_name("TORRENT")
                .help("load from this path or URL")
                .required(true)
                .index(1),
        );

    let remove_torrent = SubCommand::with_name(SUBCOMMAND_REMOVE_TORRENT_NAME)
        .version(CARGO_PKG_VERSION)
        .about("Remove a torrent")
        .arg(
            Arg::with_name("INFO_HASH")
                .help("the info-hash to remove")
                .required(true)
                .index(1),
        );

    let list_torrents = SubCommand::with_name(SUBCOMMAND_LIST_TORRENTS_NAME)
        .version(CARGO_PKG_VERSION)
        .about("List loaded torrents");

    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("send command")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(
            Arg::with_name("control-uri")
                .long("control-uri")
                .help("the uri to connect to")
                .required(true)
                .takes_value(true),
        )
        .subcommand(add_torrent)
        .subcommand(remove_torrent)
        .subcommand(list_torrents)
}

pub async fn add_torrent(
    client: &mut TorrentHostClient<Channel>,
    matches: &clap::ArgMatches<'_>,
) -> Result<(), failure::Error> {
    let torrent_file = matches.value_of("TORRENT").unwrap();

    let mut fi = File::open(torrent_file)?;
    let mut file_data = Vec::new();
    fi.read_to_end(&mut file_data)?;

    let request = tonic::Request::new(AddTorrentRequest {
        torrent_file: Some(TorrentFile::Bytes(file_data)),
        crypto_key: Vec::new(),
        backing_file: None,
    });
    let response = client.add_torrent(request).await?;
    let resp_data = response.get_ref();
    let info_hash = TorrentId::from_slice(&resp_data.info_hash[..])?;

    println!("added {}", info_hash.hex());

    Ok(())
}

pub async fn remove_torrent(
    client: &mut TorrentHostClient<Channel>,
    matches: &clap::ArgMatches<'_>,
) -> Result<(), failure::Error> {
    let request = tonic::Request::new(RemoveTorrentRequest {
        info_hash: TorrentId::zero().into(),
    });
    client.remove_torrent(request).await?;
    Ok(())
}

pub async fn list_torrents(
    client: &mut TorrentHostClient<Channel>,
    matches: &clap::ArgMatches<'_>,
) -> Result<(), failure::Error> {
    let request = tonic::Request::new(ListTorrentsRequest {});
    let response = client.list_torrents(request).await?;

    for e in &response.get_ref().entries {
        let tid = TorrentId::from_slice(&e.info_hash[..])?;
        println!("{} {:<40} {} ", tid.hex(), e.name, ByteSize(e.size_bytes));
    }

    Ok(())
}

pub async fn main(matches: &clap::ArgMatches<'_>) -> Result<(), failure::Error> {
    let (sub_name, args) = matches.subcommand();
    let args = args.expect("subcommand args");

    let uri = matches.value_of("control-uri").unwrap().to_string();
    event!(Level::DEBUG, "connecting to control socket {}", uri);
    let mut client = TorrentHostClient::connect(uri).await?;

    match sub_name {
        SUBCOMMAND_ADD_TORRENT_NAME => {
            add_torrent(&mut client, args).await?;
        }
        SUBCOMMAND_REMOVE_TORRENT_NAME => {
            remove_torrent(&mut client, args).await?;
        }
        SUBCOMMAND_LIST_TORRENTS_NAME => {
            list_torrents(&mut client, args).await?;
        }
        _ => panic!("unreachable subcommand {}", sub_name),
    }

    Ok(())
}
