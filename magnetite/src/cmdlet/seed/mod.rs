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
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tracing::{event, Level};

use crate::bittorrent::peer_state::{
    merge_global_payload_stats, ClientContext, PeerState, Session, Stats,
};
use crate::model::config::get_torrent_salsa;
use crate::model::proto::{deserialize, serialize, Handshake, Message, PieceSlice, HANDSHAKE_SIZE};
use crate::model::MagnetiteError;
use crate::model::{
    BadHandshake, BitField, ProtocolViolation, TorrentID, TorrentMetaWrapped, Truncated,
};
use crate::storage::{
    piece_file, state_wrapper, PieceFileStorageEngine, PieceStorageEngine, StateWrapper,
};
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
    content_key: &TorrentID,
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
    content_key: &TorrentID,
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
//     self_peer_id: TorrentID,
//     acceptable_peers: Arc<Mutex<BTreeSet<TorrentID>>>,

// }

struct TorrentDownloadStateManager {
    torrents: HashMap<TorrentID, TorrentDownloadState>,
}

struct TorrentDownloadState {
    // disallow peers that have not been provided to us by the tracker at some point,
    // perhaps useful in the case of private trackers?
    allow_unknown_peers: bool,
    known_peer_ids: HashMap<IpAddr, Option<TorrentID>>,
}

impl TorrentDownloadStateManager {
    fn accept_peer(
        &self,
        socket_addr: &IpAddr,
        peer_id: &TorrentID,
        info_hash: &TorrentID,
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
    self_pid: &TorrentID,
    outgoing: Option<&TorrentID>,
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

pub fn main(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    use crate::model::config::LegacyConfig as Config;

    let config = matches.value_of("config").unwrap();
    let mut cfg_fi = File::open(&config).unwrap();
    let mut cfg_by = Vec::new();
    cfg_fi.read_to_end(&mut cfg_by).unwrap();
    let config: Config = toml::de::from_slice(&cfg_by).unwrap();

    let peer_id = TorrentID::generate_peer_id_seeded(&config.client_secret);
    let mut rt = Runtime::new()?;

    let cc: ClientContext = Default::default();

    let mut pf_builder = PieceFileStorageEngine::builder();
    let mut state_builder = StateWrapper::builder();

    for s in &config.torrents {
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
                piece_count,
                crypto,
                piece_file: pf.into(),
            },
        );
        state_builder.register_info_hash(
            &tm.info_hash,
            state_wrapper::Registration {
                total_length: tm.total_length,
                piece_length: tm.meta.info.piece_length,
                piece_shas: tm.piece_shas.clone(),
            },
        );

        println!("added info_hash: {:?}", tm.info_hash);
    }

    let storage_engine = state_builder.build(pf_builder.build());
    let cc = Arc::new(Mutex::new(cc));

    let torrent_stats: Arc<Mutex<HashMap<TorrentID, Arc<Mutex<Stats>>>>> = Default::default();

    rt.block_on(async {
        // suppress connections to these until the value has expired.
        // let connect_blacklist: HashMap<SocketAddr, Instant> = Default::default();

        let mut listener = TcpListener::bind(&config.seed_bind_addr).await.unwrap();
        // let (state_channel_tx, mut state_channel_rx) = tokio::sync::mpsc::channel(10);
        // tokio::spawn(async move {
        //     while let Some(stat_update) = state_channel_rx.recv().await {
        //         // stat_update
        //     }
        // });

        loop {
            let torrent_stats = torrent_stats.clone();
            let (mut socket, addr) = listener.accept().await.unwrap();

            // let state_channel_tx = state_channel_tx.clone();
            let storage_engine = storage_engine.clone();
            let cc = Arc::clone(&cc);

            tokio::spawn(async move {
                let mut rbuf = BytesMut::new();
                let handshake =
                    match read_to_completion(&mut socket, &mut rbuf, Handshake::parse2).await {
                        Ok(hs) => hs,
                        Err(_err) => {
                            event!(Level::INFO,
                            name = "bad-handshake",
                            addr = ?addr,
                            "bad handshake {:?}", bencode::BinStr(&rbuf[..]));
                            return;
                        }
                    };

                let mut ccl = cc.lock().await;

                event!(Level::INFO,
                    name = "bittorrent-connection",
                    info_hash = ?handshake.info_hash,
                    peer_id = ?handshake.peer_id,
                    addr = ?addr);

                let session_id = ccl.session_id_seq;
                ccl.session_id_seq += 1;

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

                let torrent = match ccl.torrents.get(&handshake.info_hash) {
                    Some(tt) => tt,
                    None => {
                        event!(Level::INFO,
                            peer_id = ?handshake.peer_id,
                            addr = ?addr,
                            "unknown-info-hash");
                        return;
                    }
                };

                let mut hs_buf = [0; HANDSHAKE_SIZE];
                let hs = Handshake::serialize_bytes(
                    &mut hs_buf[..],
                    &handshake.info_hash,
                    &peer_id,
                    &[],
                )
                .unwrap();
                socket.write(&hs).await.unwrap();

                let torrent_meta = Arc::clone(&torrent.meta);
                let target = torrent_meta.info_hash;
                let bf_length = torrent_meta.meta.info.pieces.chunks(20).count() as u32;

                event!(Level::INFO,
                    session_id = session_id,
                    info_hash = ?handshake.info_hash,
                    peer_id = ?handshake.peer_id,
                    "configured");

                ccl.sessions.insert(
                    session_id,
                    Session {
                        id: session_id,
                        addr,
                        handshake,
                        target,
                        state: PeerState::new(bf_length),
                    },
                );

                drop(ccl);

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

                            let mut ccl = cc.lock().await;
                            merge_global_payload_stats(&mut ccl, &mut state);
                            if let Some(s) = ccl.sessions.get_mut(&session_id) {
                                s.state = state.clone();
                            }
                            drop(ccl);
                        }
                    }

                    Ok(())
                };

                let res1: Result<(), MagnetiteError> = handler.await;

                let mut ccl = cc.lock().await;
                let mut removed = ccl.sessions.remove(&session_id).unwrap();
                merge_global_payload_stats(&mut ccl, &mut removed.state);
                let remaining_connections = ccl.sessions.len();
                drop(ccl);

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
            });
        }
    });

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
