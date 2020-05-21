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

use crate::bittorrent::peer_state::{
    merge_global_payload_stats, GlobalState, PeerState, Session, Stats,
};
use crate::model::config::{build_storage_engine, Config, Frontend, FrontendSeed, StorageEngineServicesWithMeta};
use crate::model::proto::{deserialize, serialize, Handshake, Message, PieceSlice, HANDSHAKE_SIZE};
use crate::model::MagnetiteError;
use crate::model::{generate_peer_id_seeded, BadHandshake, BitField, ProtocolViolation, Truncated};
use crate::storage::PieceStorageEngine;
use crate::utils::BytesCow;
use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "seed";

pub fn get_subcommand() -> App<'static, 'static> {
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
) -> Result<(), MagnetiteError> {
    loop {
        match deserialize(r) {
            IResult::Done(v) => messages.push(v),
            IResult::ReadMore(..) => return Ok(()),
            IResult::Err(err) => {
                return Err(err);
            }
        }
    }
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

async fn send_pieces(
    content_key: &TorrentId,
    persist_buf: &mut [u8],
    addr: SocketAddr,
    w: &mut ::tokio::net::tcp::WriteHalf<'_>,
    requests: &[PieceSlice],
    pse: &(dyn PieceStorageEngine + Sync + Sync + 'static),
) -> Result<u64, MagnetiteError> {
    let messages = collect_pieces(content_key, requests, pse).await?;
    // XXX: allocation - use an iovec when serialize can support it.
    // XXX: trusting user input to be within bounds: piece.length < 64k - header_size
    //      iovecs solve this.
    let mut bytes_written = 0;
    for m in &messages {
        let ss = serialize(&mut persist_buf[..], m).unwrap();
        w.write_all(ss).await?;
        w.flush().await?;
        bytes_written += ss.len() as u64;
    }
    event!(Level::DEBUG, "{} is at {} bw", addr, bytes_written);

    Ok(bytes_written)
}

// struct ConnectHandlerState {
//     self_peer_id: TorrentId,
//     acceptable_peers: Arc<Mutex<BTreeSet<TorrentId>>>,

// }

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
) -> Result<Handshake, MagnetiteError> {
    let remote_handshake;
    let mut hs_buf = [0; HANDSHAKE_SIZE];
    if let Some(ih) = outgoing {
        // send handhake, then rx handshake
        let hs = Handshake::serialize_bytes(&mut hs_buf[..], &ih, self_pid, &[]).unwrap();
        socket.write_all(&hs).await?;
        remote_handshake = read_to_completion(socket, rbuf, Handshake::parse2).await?;
    } else {
        remote_handshake = read_to_completion(socket, rbuf, Handshake::parse2).await?;

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

fn client_stream_reader<R>(
    mut rbuf: BytesMut,
    mut reader: R,
) -> impl Stream<Item = Result<StreamReaderItem, MagnetiteError>>
where
    R: AsyncRead + Unpin,
{
    use futures::ready;
    use tokio::time::{delay_for, Instant};

    let ping_interval: Duration = Duration::new(40, 0);

    futures::stream::poll_fn(move |cx| {
        let mut anti_idle_timer = delay_for(ping_interval);
        let mut must_read: usize = 0;
        let mut stream_ended = false;

        loop {
            if must_read == 0 {
                match deserialize(&mut rbuf) {
                    IResult::Done(v) => return Poll::Ready(Some(Ok(StreamReaderItem::Message(v)))),
                    IResult::ReadMore(bytes) => must_read = bytes,
                    IResult::Err(err) => {
                        event!(
                            Level::TRACE,
                            "read+deserialize error (protocol violation): buffer = {:?}",
                            rbuf
                        );
                        return Poll::Ready(Some(Err(err)));
                    }
                }
            }

            let anti_idle_timer_pin = Pin::new(&mut anti_idle_timer);
            if let Poll::Ready(()) = Future::poll(anti_idle_timer_pin, cx) {
                anti_idle_timer.reset(Instant::now() + ping_interval);
                return Poll::Ready(Some(Ok(StreamReaderItem::AntiIdle)));
            }

            if stream_ended {
                return Poll::Ready(None);
            }

            let reader_pin = Pin::new(&mut reader);
            match ready!(reader_pin.poll_read_buf(cx, &mut rbuf)) {
                Ok(0) => stream_ended = true,
                Ok(len) => must_read = must_read.saturating_sub(len),
                Err(err) => {
                    event!(Level::INFO, "read error: {}", err);
                    stream_ended = true;
                }
            }
        }
    })
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

pub async fn main(matches: &clap::ArgMatches<'_>) -> Result<(), failure::Error> {
    let config = matches.value_of("config").unwrap();
    let mut cfg_fi = File::open(&config).unwrap();
    let mut cfg_by = Vec::new();
    cfg_fi.read_to_end(&mut cfg_by).unwrap();
    let config: Config = toml::de::from_slice(&cfg_by).unwrap();

    let peer_id = generate_peer_id_seeded(&config.client_secret);
    let StorageEngineServicesWithMeta {
        piece_fetcher,
        torrent_managers,
    } = build_storage_engine(&config).unwrap();

    let torrent_stats: Arc<Mutex<HashMap<TorrentId, Arc<Mutex<Stats>>>>> = Default::default();
    let gs: Arc<Mutex<GlobalState>> = Default::default();

    let mut futures = Vec::new();
    for fe in &config.frontends {
        if let Frontend::Seed(ref seed) = fe {
            let seed: FrontendSeed = seed.clone();
            let piece_fetcher = piece_fetcher.clone();
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
                    let piece_fetcher = piece_fetcher.clone();
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
                            start_server(socket, piece_fetcher, addr, torrent_stats, gs, peer_id)
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
    let torrent_stats = torrent_stats.clone();

    // let state_channel_tx = state_channel_tx.clone();

    let mut rbuf = BytesMut::new();
    let handshake = match read_to_completion(&mut socket, &mut rbuf, Handshake::parse2).await {
        Ok(hs) => hs,
        Err(err) => {
            let hs_err = BadHandshake::junk_data();
            event!(Level::INFO,
                name = "bad-handshake",
                addr = ?addr,
                handshake_error = ?hs_err,
                data = ?bencode::BinStr(&rbuf[..]),
                "bad handshake: {}",
                err);

            return Err(hs_err.into());
        }
    };

    let mut gsl = gs.lock().await;

    event!(Level::INFO,
        name = "bittorrent-connection",
        info_hash = ?handshake.info_hash,
        peer_id = ?handshake.peer_id,
        addr = ?addr);

    let session_id = gsl.session_id_seq;
    gsl.session_id_seq += 1;

    let mut torrent_stats_locked = torrent_stats.lock().await;
    let self_torrent_stats = torrent_stats_locked
        .entry(handshake.info_hash)
        .or_insert_with(|| {
            Arc::new(Mutex::new(Stats {
                sent_payload_bytes: 0,
                recv_payload_bytes: 0,
            }))
        })
        .clone();
    drop(torrent_stats_locked);

    let torrent = match gsl.torrents.get(&handshake.info_hash) {
        Some(tt) => tt,
        None => {
            let hs_err = BadHandshake::unknown_info_hash(&handshake.info_hash);
            event!(Level::INFO,
                name = "bad-handshake",
                addr = ?addr,
                handshake_error = ?hs_err,
                "bad handshake: unknown info hash");

            return Err(hs_err.into());
        }
    };

    let mut hs_buf = [0; HANDSHAKE_SIZE];
    let hs =
        Handshake::serialize_bytes(&mut hs_buf[..], &handshake.info_hash, &peer_id, &[]).unwrap();
    socket.write(&hs).await.unwrap();

    let torrent_meta = Arc::clone(&torrent.meta);
    let target = torrent_meta.info_hash;
    let bf_length = torrent_meta.meta.info.pieces.chunks(20).count() as u32;

    event!(Level::INFO,
        session_id = session_id,
        info_hash = ?handshake.info_hash,
        peer_id = ?handshake.peer_id,
        "configured");

    gsl.sessions.insert(
        session_id,
        Session {
            id: session_id,
            addr,
            handshake,
            target,
            state: PeerState::new(bf_length),
        },
    );

    drop(gsl);

    let (srh, mut swh) = socket.split();

    let mut request_queue_rx = client_stream_reader(rbuf, srh);
    let handler = async {
        let mut state = PeerState::new(bf_length);
        let mut msg_buf = vec![0; 64 * 1024];
        let mut requests = Vec::<PieceSlice>::new();
        let piece_count = torrent_meta.meta.info.pieces.chunks(20).count() as u32;

        {
            let have_bitfield = BitField::all(piece_count);

            let ss = serialize(
                &mut msg_buf[..],
                &Message::Bitfield {
                    field_data: BytesCow::Borrowed(have_bitfield.as_raw_slice()),
                },
            )
            .unwrap();
            swh.write_all(ss).await?;

            let ss = serialize(&mut msg_buf[..], &Message::Unchoke).unwrap();
            swh.write_all(ss).await?;
        }

        let mut next_bytes_out_milestone: u64 = 1024 * 1024;
        let mut next_global_stats_update = Instant::now() + Duration::new(120, 0);

        while let Some(work) = request_queue_rx.next().await {
            let work = work?;

            let now = Instant::now();
            event!(Level::DEBUG, session_id = session_id, "processing-work");
            requests.clear();

            if state.next_keepalive < now {
                let ss = serialize(&mut msg_buf[..], &Message::Keepalive).unwrap();
                swh.write_all(ss).await?;
                state.next_keepalive = now + Duration::new(90, 0);
            }

            apply_work_state(&mut state, &work, now, session_id)?;

            if let StreamReaderItem::Message(Message::Request(ps)) = work {
                requests.push(ps);
            }

            let sent_len = send_pieces(
                &torrent_meta.info_hash,
                &mut msg_buf[..],
                addr,
                &mut swh,
                &requests,
                &storage_engine,
            )
            .await?;

            state.stats.sent_payload_bytes += sent_len;
            state.global_uncommitted_stats.sent_payload_bytes += sent_len;

            let mut stats_locked = self_torrent_stats.lock().await;
            stats_locked.sent_payload_bytes += sent_len;
            drop(stats_locked);

            if next_bytes_out_milestone < state.stats.sent_payload_bytes {
                next_bytes_out_milestone += 100 << 20;
                let completed = state.peer_bitfield.count_ones();
                let total_pieces = state.peer_bitfield.bit_length;
                event!(
                    Level::INFO,
                    name = "update",
                    session_id = session_id,
                    bandwidth_milestone_bytes = state.stats.sent_payload_bytes,
                    completed_pieces = completed,
                    total_pieces = state.peer_bitfield.bit_length,
                    "#{}: bw-out milestone of {} MB, {:.2}% confirmed",
                    session_id,
                    state.stats.sent_payload_bytes / 1024 / 1024,
                    100.0 * completed as f64 / total_pieces as f64
                );
            }

            if next_global_stats_update < now {
                next_global_stats_update = now + Duration::new(12, 0);

                let mut gsl = gs.lock().await;
                merge_global_payload_stats(&mut gsl, &mut state);
                if let Some(s) = gsl.sessions.get_mut(&session_id) {
                    s.state = state.clone();
                }
                drop(gsl);
            }
        }

        Ok(())
    };

    let res1: Result<(), MagnetiteError> = handler.await;

    let mut gsl = gs.lock().await;
    let mut removed = gsl.sessions.remove(&session_id).unwrap();
    merge_global_payload_stats(&mut gsl, &mut removed.state);
    let remaining_connections = gsl.sessions.len();
    drop(gsl);

    event!(
        Level::WARN,
        name = "disconnected",
        session_id = session_id,
        sent_payload_bytes = removed.state.stats.sent_payload_bytes,
        recv_payload_bytes = removed.state.stats.recv_payload_bytes,
        remaining_connections = remaining_connections
    );

    if let Err(err) = res1 {
        event!(
            Level::INFO,
            session_id = session_id,
            "err from client reader: {:?}",
            err
        );
    }

    Ok(())
}

async fn read_to_completion<T, F>(
    s: &mut TcpStream,
    buf: &mut BytesMut,
    p: F,
) -> Result<T, MagnetiteError>
where
    F: Fn(&mut BytesMut) -> IResult<T, MagnetiteError>,
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
            IResult::Done(val) => return Ok(val),
            IResult::Err(err) => return Err(err),
            IResult::ReadMore(n) => {
                needed = n;
                if length == 0 {
                    return Err(Truncated.into());
                }
            }
        }
    }
}
