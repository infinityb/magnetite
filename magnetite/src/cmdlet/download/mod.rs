use std::fs::File;
use std::io::Read;
use std::cell::RefCell;
use std::rc::Rc;
use std::pin::Pin;


use clap::{App, Arg, SubCommand};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::{self, LocalSet};
use tracing::{event, Level};
use anyhow::{Context};
use futures::{Future, FutureExt, StreamExt, SinkExt};

use crate::model::config::{Config, build_storage_engine, Frontend, FrontendHost};

use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "download";

pub fn get_subcommand() -> App<'static> {
    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("Download one torrent, seed for a while, then exit.")
        .arg(
            Arg::with_name("config")
                .long("config")
                .value_name("FILE")
                .help("config file")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("torrent-file")
                .long("torrent-file")
                .value_name("FILE")
                .help("torrent file")
                .required(true)
                .takes_value(true),
        )
}

type LocalBoxFutResult<T> = Pin<Box<dyn Future<Output=Result<T, anyhow::Error>> + 'static>>;


pub async fn start_tracker_task(state: Rc<RefCell<()>>, mut tx: futures::channel::mpsc::Sender<LocalBoxFutResult<()>>) {
    let f: LocalBoxFutResult<()> = async {
        Ok(())
    }.boxed_local();

    if let Err(err) = tx.try_send(f) {
        eprintln!("error start_tracker_task: {:?}", err);
    }
}

pub async fn start_management_task(state: Rc<RefCell<()>>, mut tx: futures::channel::mpsc::Sender<LocalBoxFutResult<()>>) {
    let f: LocalBoxFutResult<()> = async {
        Ok(())
    }.boxed_local();

    if let Err(err) = tx.try_send(f) {
        eprintln!("error start_management_task: {:?}", err);
    }
}

pub async fn start_outgoing_connections(state: Rc<RefCell<()>>, mut tx: futures::channel::mpsc::Sender<LocalBoxFutResult<()>>) {
    let f: LocalBoxFutResult<()> = async {
        Ok(())
    }.boxed_local();

    if let Err(err) = tx.try_send(f) {
        eprintln!("error start_outgoing_connections: {:?}", err);
    }
}

pub async fn start_incoming_connections(state: Rc<RefCell<()>>, mut tx: futures::channel::mpsc::Sender<LocalBoxFutResult<()>>) {
    let f: LocalBoxFutResult<()> = async {
        Ok(())
    }.boxed_local();

    if let Err(err) = tx.try_send(f) {
        eprintln!("error start_outgoing_connections: {:?}", err);
    }
}

pub fn start_download_thread<'a>(local: &'a task::LocalSet) -> impl Future<Output=anyhow::Result<()>> + 'a {
    let shared_state = ();
    let shared_state: Rc<RefCell<_>> = Rc::new(RefCell::new(shared_state));


    local.run_until(async move {
        Ok(())
    })
    // if let Err(err) = res {
    //     return Err(failure::format_err!("{}", err));
    // }
    // Ok(())

    // .run_until(async {


    //     // driver/reaper task
    //     let (tx, rx) = futures::channel::mpsc::channel(64);

    //     start_tracker_task(Rc::clone(&shared_state), tx.clone());
    //     start_management_task(Rc::clone(&shared_state), tx.clone());
    //     start_outgoing_connections(Rc::clone(&shared_state), tx.clone());
    //     start_incoming_connections(Rc::clone(&shared_state), tx.clone());

    //     let mut buffered = rx.buffer_unordered(64);
    //     while let Some(v) = buffered.next().await {
    //         v?;
    //     }
    //     Ok(())
    // }).await
}

pub async fn main(matches: &clap::ArgMatches) -> Result<(), anyhow::Error> {
    let config = matches.value_of("config").unwrap();
    let mut cfg_fi = File::open(&config).unwrap();
    let mut cfg_by = Vec::new();
    cfg_fi.read_to_end(&mut cfg_by).unwrap();
    let config: Config = toml::de::from_slice(&cfg_by).unwrap();

    unimplemented!();

    // let (send, mut recv) = mpsc::unbounded_channel();
    // let rt = tokio::runtime::Builder::new_current_thread()
    //     .enable_all()
    //     .build()
    //     .unwrap();
    //
    // std::thread::Builder::new().name("foo".into()).spawn(move || {
    //     let local = LocalSet::new();
    //     local.spawn_local(async {
    //         send.send(start_download_thread(&local).await);
    //     });

    //     // This will return once all senders are dropped and all
    //     // spawned tasks have returned.
    //     rt.block_on(local);
    // });

    // if let Some(v) = recv.recv().await {
    //     v?;
    // }
    // Ok(())
}
