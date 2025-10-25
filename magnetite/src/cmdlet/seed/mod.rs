use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::marker::Unpin;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};
use magnetite_common::proto::IErr;


use bytes::BytesMut;
use clap::{App, Arg, SubCommand};
use futures::future::Future;
use futures::stream::{Stream, StreamExt};
use iresult::IResult;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tracing::{event, Level};

use magnetite_common::TorrentId;
use magnetite_model::BitField;
use bin_str::BinStr;

use crate::bittorrent::peer_state::{
    merge_global_payload_stats, GlobalState, PeerState, Session, Stats,
};
use crate::model::config::{build_storage_engine, Config, Frontend, FrontendSeed};
use magnetite_common::proto::{deserialize_pop_opt, serialize, Handshake, Message, PieceSlice, HANDSHAKE_SIZE};
use crate::model::MagnetiteError;
use crate::model::{generate_peer_id_seeded, BadHandshake, ProtocolViolation, Truncated};
use crate::storage::PieceStorageEngine;
use crate::utils::BytesCow;
use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "seed";

pub fn get_subcommand() -> App<'static> {
    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("Seed a torrent")
        .arg(
            Arg::with_name("config")
                .long("config")
                .value_name("FILE")
                .help("config file")
                .required(true)
                .takes_value(true),
        )
}

fn pop_messages_into(
    r: &mut BytesMut,
    messages: &mut Vec<Message<'static>>,
) -> Result<(), anyhow::Error> {
    while let Some(v) = deserialize_pop_opt(r)? {
        messages.push(v);
    }
    Ok(())
}

async fn collect_pieces(
    content_key: &TorrentId,
    requests: &[PieceSlice],
    pse: &(dyn PieceStorageEngine + Sync + Sync + 'static),
) -> Result<Vec<Message<'static>>, MagnetiteError> {
    let mut messages = Vec::new();
    let _offset: usize = 0;
    for p in requests {
        let piece_data = pse.get_piece(content_key, p.index).await?;
        if piece_data.len() < p.begin as usize {
            return Err(ProtocolViolation.into());
        }
        let view0 = &piece_data[p.begin as usize..];
        if view0.len() < p.length as usize {
            return Err(ProtocolViolation.into());
        }
        let piece_data_slice = piece_data.slice_ref(&view0[..p.length as usize]);

        messages.push(Message::Piece {
            index: p.index,
            begin: p.begin,
            data: BytesCow::Owned(piece_data_slice),
        });
    }

    Ok(messages)
}

struct TorrentDownloadStateManager {
    torrents: HashMap<TorrentId, TorrentDownloadState>,
}

struct TorrentDownloadState {
    // disallow peers that have not been provided to us by the tracker at some point,
    // perhaps useful in the case of private trackers?
    allow_unknown_peers: bool,
    known_peer_ids: HashMap<IpAddr, Option<TorrentId>>,
}

impl TorrentDownloadStateManager {
    fn accept_peer(
        &self,
        socket_addr: &IpAddr,
        peer_id: &TorrentId,
        info_hash: &TorrentId,
    ) -> bool {
        if let Some(ds) = self.torrents.get(info_hash) {
            match ds.known_peer_ids.get(socket_addr) {
                Some(Some(pid)) => peer_id == pid,
                Some(None) => true, // fail open when in compact tracker mode
                None => ds.allow_unknown_peers,
            }
        } else {
            false
        }
    }
}

async fn start_connection(
    ts: &TorrentDownloadStateManager,
    socket: &mut TcpStream,
    rbuf: &mut BytesMut,
    self_pid: &TorrentId,
    outgoing: Option<&TorrentId>,
) -> Result<Handshake, anyhow::Error> {
    let remote_handshake;
    let mut hs_buf = [0; HANDSHAKE_SIZE];
    if let Some(ih) = outgoing {
        // send handhake, then rx handshake
        let hs = Handshake::serialize_bytes(&mut hs_buf[..], &ih, self_pid, &[]).unwrap();
        socket.write_all(&hs).await?;
        remote_handshake = read_to_completion(socket, rbuf, Handshake::parse).await?;
    } else {
        remote_handshake = read_to_completion(socket, rbuf, Handshake::parse).await?;

        let addr = socket.peer_addr()?.ip();

        if !ts.accept_peer(
            &addr,
            &remote_handshake.peer_id,
            &remote_handshake.info_hash,
        ) {
            return Err(BadHandshake::unknown_info_hash(&remote_handshake.info_hash).into());
        }

        let hs =
            Handshake::serialize_bytes(&mut hs_buf[..], &remote_handshake.info_hash, self_pid, &[])
                .unwrap();
        socket.write_all(&hs).await?;
    }
    Ok(remote_handshake)
}

enum StreamReaderItem {
    Message(Message<'static>),
    AntiIdle,
}

fn apply_work_state(
    ps: &mut PeerState,
    work: &StreamReaderItem,
    now: Instant,
    session_id: u64,
) -> Result<(), MagnetiteError> {
    if let StreamReaderItem::Message(..) = work {
        ps.last_read = now;
    }

    match work {
        StreamReaderItem::Message(Message::Choke) => {
            ps.choked = true;
            Ok(())
        }
        StreamReaderItem::Message(Message::Unchoke) => {
            ps.choked = false;
            Ok(())
        }
        StreamReaderItem::Message(Message::Interested) => {
            ps.interested = true;
            Ok(())
        }
        StreamReaderItem::Message(Message::Uninterested) => {
            ps.interested = false;
            Ok(())
        }
        StreamReaderItem::Message(Message::Have { piece_id }) => {
            if *piece_id < ps.peer_bitfield.bit_length {
                ps.peer_bitfield.set(*piece_id, true);
            }
            Ok(())
        }
        StreamReaderItem::Message(Message::Bitfield { ref field_data }) => {
            if field_data.as_slice().len() != ps.peer_bitfield.data.len() {
                return Err(ProtocolViolation.into());
            }
            ps.peer_bitfield.data = field_data.as_slice().to_vec().into_boxed_slice();
            let completed = ps.peer_bitfield.count_ones();
            let total_pieces = ps.peer_bitfield.bit_length;
            event!(
                Level::TRACE,
                name = "update-bitfield",
                session_id = session_id,
                completed_pieces = completed,
                total_pieces = ps.peer_bitfield.bit_length,
                "#{}: {:.2}% confirmed",
                session_id,
                100.0 * completed as f64 / total_pieces as f64
            );
            Ok(())
        }
        _ => Ok(()),
    }
}

pub async fn main(matches: &clap::ArgMatches) -> Result<(), anyhow::Error> {
    let config = matches.value_of("config").unwrap();
    let mut cfg_fi = File::open(&config).unwrap();
    let mut cfg_by = Vec::new();
    cfg_fi.read_to_end(&mut cfg_by).unwrap();
    let config: Config = toml::de::from_slice(&cfg_by).unwrap();

    let peer_id = generate_peer_id_seeded(&config.client_secret);
    let storage_engine = build_storage_engine(&config).unwrap();

    let torrent_stats: Arc<Mutex<HashMap<TorrentId, Arc<Mutex<Stats>>>>> = Default::default();
    let gs: Arc<Mutex<GlobalState>> = Default::default();

    let mut futures = Vec::new();
    for fe in &config.frontends {
        if let Frontend::Seed(ref seed) = fe {
            let seed: FrontendSeed = seed.clone();
            let storage_engine = storage_engine.clone();
            let torrent_stats = torrent_stats.clone();
            let gs = gs.clone();
            futures.push(async move {
                let mut listener = TcpListener::bind(&seed.bind_address).await.unwrap();
                event!(
                    Level::INFO,
                    "seed backend bind successful: {:?}",
                    seed.bind_address
                );

                loop {
                    let storage_engine = storage_engine.clone();
                    let torrent_stats = torrent_stats.clone();
                    let gs = gs.clone();

                    let (socket, addr) = listener.accept().await.unwrap();
                    event!(Level::INFO, "got connection from {:?}", addr);

                    // suppress connections to these until the value has expired.
                    // let connect_blacklist: HashMap<SocketAddr, Instant> = Default::default();
                    //
                    // ???
                    // let (state_channel_tx, mut state_channel_rx) = tokio::sync::mpsc::channel(10);
                    // tokio::spawn(async move {
                    //     while let Some(stat_update) = state_channel_rx.recv().await {
                    //         // stat_update
                    //     }
                    // });

                    tokio::spawn(async move {
                        if let Err(err) =
                            start_server(socket, storage_engine, addr, torrent_stats, gs, peer_id)
                                .await
                        {
                            event!(Level::ERROR, "error: {}", err);
                        }
                    });
                }
            });
        }
    }

    futures::future::join_all(futures).await;

    Ok(())
}

async fn start_server<S>(
    mut socket: TcpStream,
    storage_engine: S,
    addr: SocketAddr,
    torrent_stats: Arc<Mutex<HashMap<TorrentId, Arc<Mutex<Stats>>>>>,
    gs: Arc<Mutex<GlobalState>>,
    peer_id: TorrentId,
) -> Result<(), MagnetiteError>
where
    S: PieceStorageEngine + Sync + Send + 'static,
{
    unimplemented!();
}

async fn read_to_completion<T, F, E>(
    s: &mut TcpStream,
    buf: &mut BytesMut,
    p: F,
) -> Result<T, anyhow::Error>
where
    E: Into<anyhow::Error>,
    F: Fn(&[u8]) -> magnetite_common::proto::IResult<T, E>,
{
    let mut needed: usize = 0;
    loop {
        let length = s.read_buf(buf).await?;
        if length != 0 && length < needed {
            needed -= length;
            // we need more bytes, as the parser reported before.
            continue;
        }
        match p(buf) {
            Ok((length, val)) => {
                buf.split_to(length);
                return Ok(val);
            }
            Err(IErr::Incomplete(n)) => {
                needed = n;
                if length == 0 {
                    return Err(Truncated.into());
                }
            }
            Err(IErr::Error(err)) => {
                return Err(err.into());
            }
        }
    }
}
