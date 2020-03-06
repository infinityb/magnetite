use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::BytesMut;
use clap::{App, Arg, SubCommand};
use iresult::IResult;
use lru::LruCache;
use salsa20::stream_cipher::generic_array::GenericArray;
use salsa20::stream_cipher::NewStreamCipher;
use salsa20::XSalsa20;
use sha1::Digest;
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tracing::{event, Level};

use crate::model::proto::{
    deserialize, serialize, BytesCow, Handshake, Message, PieceSlice, HANDSHAKE_SIZE,
};
use crate::model::{BitField, ProtocolViolation, TorrentID, TorrentMetaWrapped, Truncated};
use crate::storage::{
    PieceFileStorageEngine, PieceFileStorageEngineLockables, PieceFileStorageEngineVerifyMode,
};
use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "seed";

pub fn get_subcommand() -> App<'static, 'static> {
    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("Seed a torrent")
        .arg(
            Arg::with_name("client-secret")
                .long("client-secret")
                .value_name("SECRET")
                .help("Seeds PeerID")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("torrent-file")
                .long("torrent-file")
                .value_name("FILE")
                .help("The torrent file to serve")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("piece-file")
                .long("piece-file")
                .value_name("piece-file")
                .help("The piece file to serve")
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

fn pop_messages_into(
    r: &mut BytesMut,
    messages: &mut Vec<Message<'static>>,
) -> Result<(), failure::Error> {
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

// async fn check_collect_piece(pfse: &PieceFileStorageEngine) -> Result<(), failure::Error> {
//     let mut req_queue: Vec<PieceSlice> = Vec::new();
//     for i in 0..(pfse.piece_length / 16_384) {
//         req_queue.push(PieceSlice {
//             index: 100,
//             begin: (i * 16_384) as u32,
//             length: 16_384,
//         });
//     }

//     let parts = collect_pieces(&req_queue[..], pfse).await?;

//     let mut ser_buf = Vec::new();
//     let mut tmp_buf = [0; 64 * 1024];

//     for (idx, p) in parts.iter().enumerate() {
//         let vv = serialize(&mut tmp_buf[..], p).unwrap();
//         ser_buf.extend(vv);
//     }

//     let mut reader = BytesMut::from(&ser_buf[..]);
//     let mut hasher = Sha1::new();

//     for (idx, p) in parts.iter().enumerate() {
//         match deserialize(&mut reader) {
//             IResult::Done(Message::Piece { index, begin, data }) => {
//                 assert_eq!(index, 100);
//                 assert_eq!(begin as u64, idx as u64 * 16_384);
//                 assert_eq!(data.as_slice().len(), 16_384);
//                 hasher.input(data.as_slice());
//             }
//             IResult::Done(vv) => panic!("unexpected message: {:?}", vv),
//             IResult::ReadMore(..) => break,
//             IResult::Err(err) => panic!("error deserializing: {:?}", err),
//         }
//     }

//     if pfse.piece_shas[100].data[..] != hasher.result()[..] {
//         panic!("Error: human is dead, mismatch.");
//     }

//     Ok(())
// }

async fn collect_pieces(
    requests: &[PieceSlice],
    pfse: &PieceFileStorageEngine,
) -> Result<Vec<Message<'static>>, failure::Error> {
    let mut messages = Vec::new();
    let _offset: usize = 0;

    for p in requests {
        let piece_data = pfse.get_piece(p.index).await?;
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
    persist_buf: &mut [u8],
    addr: std::net::SocketAddr,
    w: &mut ::tokio::net::tcp::WriteHalf<'_>,
    requests: &[PieceSlice],
    pfse: &PieceFileStorageEngine,
) -> Result<u64, failure::Error> {
    let messages = collect_pieces(requests, pfse).await?;
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

struct ClientContext {
    session_id_seq: u64,
    torrents: HashMap<TorrentID, Torrent>,
    sessions: HashMap<u64, Session>,
}

struct Torrent {
    id: TorrentID,
    meta: Arc<TorrentMetaWrapped>,
    have_bitfield: BitField,
    piece_file_path: PathBuf,
    crypto_secret: String,
    tracker_groups: Vec<TrackerGroup>,
}

struct TrackerGroup {
    //
}

struct Session {
    id: u64,
    addr: std::net::SocketAddr,
    handshake: Handshake,
    target: TorrentID,
    // Arc<TorrentMetaWrapped>,
    state: PeerState,
    // storage_engine: PieceFileStorageEngine,
}

#[derive(Clone)]
struct PeerState {
    last_read: std::time::Instant,
    next_keepalive: std::time::Instant,
    sent_bytes: u64,
    recv_bytes: u64,
    peer_bitfield: BitField,
    choking: bool,
    interesting: bool,
    choked: bool,
    interested: bool,
}

impl PeerState {
    pub fn new(bf_length: u32) -> PeerState {
        let now = std::time::Instant::now();
        PeerState {
            last_read: now,
            next_keepalive: now,
            sent_bytes: 0,
            recv_bytes: 0,
            peer_bitfield: BitField::none(bf_length),
            choking: true,
            interesting: false,
            choked: true,
            interested: false,
        }
    }
}

pub fn main(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    struct TorrentFactory {
        torrent_file: PathBuf,
        source_file: PathBuf,
        secret: String,
    }

    let client_secret = matches.value_of("client-secret").unwrap();
    let peer_id = TorrentID::generate_peer_id_seeded(&client_secret);
    let torrent_file = matches.value_of_os("torrent-file").unwrap();
    let _torrent_file = Path::new(torrent_file).to_owned();

    let piece_file = matches.value_of_os("piece-file").unwrap();
    let _piece_file = Path::new(piece_file).to_owned();

    let mut rt = Runtime::new()?;

    let _secret: String = matches.value_of("secret").unwrap().to_string();

    let mut cc = ClientContext {
        session_id_seq: 0,
        torrents: HashMap::new(),
        sessions: HashMap::new(),
    };
    let mut sources = Vec::new();
    sources.push(TorrentFactory {
        torrent_file: Path::new("/home/sell/compile/magnetite/danbooru2019-0-loaded.torrent")
            .to_owned(),
        source_file: Path::new("/mnt/home/danbooru2019-0.tome").to_owned(),
        secret: "C3EsrGPe62jQx6U6Z6JTxCcWKSWpA4G2".to_string(),
    });
    // sources.push(TorrentFactory {
    //     torrent_file: Path::new("/home/sell/compile/magnetite/danbooru2019-1-loaded.torrent").to_owned(),
    //     source_file: Path::new("/mnt/home/danbooru2019-1.tome").to_owned(),
    //     secret: "RhdmQRQZzbSwbqyKk4T4tzxcQ4BG6V9b".to_string(),
    // });
    // sources.push(TorrentFactory {
    //     torrent_file: Path::new("/home/sell/compile/magnetite/danbooru2019-2.torrent").to_owned(),
    //     source_file: Path::new("/mnt/home/danbooru2019-2.tome").to_owned(),
    //     secret: "afqR5ALeMtoyYej9UNTQi7YEM4dtdbjQ".to_string(),
    // });

    for s in sources.into_iter() {
        let mut fi = File::open(&s.torrent_file).unwrap();
        let mut by = Vec::new();
        fi.read_to_end(&mut by).unwrap();

        let mut torrent = TorrentMetaWrapped::from_bytes(&by).unwrap();
        torrent.meta.info.files = Vec::new();
        let tm = Arc::new(torrent);

        let info_hash = tm.info_hash;
        let have_bitfield = BitField::all(tm.meta.info.pieces.chunks(20).count() as u32);

        cc.torrents.insert(
            info_hash,
            Torrent {
                id: info_hash,
                meta: tm,
                have_bitfield,
                piece_file_path: s.source_file,
                crypto_secret: s.secret,
                tracker_groups: Vec::new(),
            },
        );

        println!("added info_hash: {:?}", info_hash);
    }

    let cc = Arc::new(Mutex::new(cc));

    rt.block_on(async {
        let mut listener = TcpListener::bind("[::]:17862").await.unwrap();
        // let (state_channel_tx, mut state_channel_rx) = tokio::sync::mpsc::channel(10);
        // tokio::spawn(async move {
        //     while let Some(stat_update) = state_channel_rx.recv().await {
        //         // stat_update
        //     }
        // });

        loop {
            let (mut socket, addr) = listener.accept().await.unwrap();
            // let state_channel_tx = state_channel_tx.clone();
            let peer_id = peer_id.clone();
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

                drop(torrent);
                let torrent_meta = Arc::clone(&torrent.meta);

                let target = torrent_meta.info_hash;
                let mut nonce_data = [0; 24];
                for (o, i) in nonce_data[4..]
                    .iter_mut()
                    .zip(torrent_meta.info_hash.as_bytes().iter())
                {
                    *o = *i;
                }
                let nonce = GenericArray::from_slice(&nonce_data[..]);
                let piece_file = TokioFile::open(&torrent.piece_file_path).await.unwrap();
                let bf_length = torrent_meta.meta.info.pieces.chunks(20).count() as u32;
                let key = GenericArray::from_slice(torrent.crypto_secret.as_bytes());

                let storage_engine = PieceFileStorageEngine {
                    total_length: torrent_meta.total_length,
                    piece_length: torrent_meta.meta.info.piece_length,
                    lockables: Arc::new(Mutex::new(PieceFileStorageEngineLockables {
                        crypto: Some(XSalsa20::new(&key, &nonce)),
                        verify_piece: PieceFileStorageEngineVerifyMode::First {
                            verified: BitField::none(bf_length),
                        },
                        piece_cache: LruCache::new(16),
                        piece_file,
                    })),
                    piece_shas: torrent_meta.piece_shas.clone(),
                };

                let _have_bitfield =
                    BitField::all(torrent_meta.meta.info.pieces.chunks(20).count() as u32);

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
                        target: target,
                        state: PeerState::new(bf_length),
                    },
                );

                drop(ccl);

                let _last_handshake = std::time::Instant::now();
                let (mut request_queue_tx, mut request_queue_rx) = tokio::sync::mpsc::channel(10);
                let (mut srh, mut swh) = socket.split();

                let reader = async {
                    loop {
                        use tokio::time::{timeout, Duration};

                        let mut reqs = Vec::new();
                        pop_messages_into(&mut rbuf, &mut reqs)?;
                        event!(Level::DEBUG, read_requests=?reqs);
                        request_queue_tx.send(reqs).await?;

                        let length =
                            match timeout(Duration::new(40, 0), srh.read_buf(&mut rbuf)).await {
                                Ok(res) => res?,
                                Err(err) => {
                                    let _err: tokio::time::Elapsed = err;
                                    request_queue_tx.send(Vec::new()).await?;
                                    continue;
                                }
                            };

                        if length == 0 {
                            event!(Level::DEBUG, session_id = session_id, "read-closed");
                            break;
                        }
                    }

                    drop(request_queue_tx);

                    Ok(())
                };

                let handler = async {
                    let mut state = PeerState::new(bf_length);
                    let mut msg_buf = vec![0; 64 * 1024];
                    let mut requests = Vec::<PieceSlice>::new();

                    {
                        let have_bitfield =
                            BitField::all(torrent_meta.meta.info.pieces.chunks(20).count() as u32);

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
                    let mut next_global_stats_update =
                        std::time::Instant::now() + std::time::Duration::new(120, 0);
                    while let Some(work) = request_queue_rx.recv().await {
                        let now = std::time::Instant::now();

                        event!(Level::DEBUG, session_id = session_id, "processing-work");
                        requests.clear();
                        if work.len() > 0 {
                            state.last_read = now;
                        }
                        if state.next_keepalive < now {
                            let ss = serialize(&mut msg_buf[..], &Message::Keepalive).unwrap();
                            swh.write_all(ss).await?;
                            state.next_keepalive = now + std::time::Duration::new(90, 0);
                        }

                        for w in work {
                            match w {
                                Message::Keepalive => {
                                    // nothing. keepalives are discarded
                                }
                                Message::Choke => {
                                    state.choked = true;
                                }
                                Message::Unchoke => {
                                    state.choked = false;
                                }
                                Message::Interested => {
                                    state.interested = true;
                                }
                                Message::Uninterested => {
                                    state.interested = false;
                                }
                                Message::Have { piece_id } => {
                                    if piece_id < state.peer_bitfield.bit_length {
                                        state.peer_bitfield.set(piece_id, true);
                                    }
                                }
                                Message::Bitfield { ref field_data } => {
                                    if field_data.as_slice().len() != state.peer_bitfield.data.len()
                                    {
                                        return Err(ProtocolViolation.into());
                                    }
                                    state.peer_bitfield.data =
                                        field_data.as_slice().to_vec().into_boxed_slice();
                                    let completed = state.peer_bitfield.count_ones();
                                    let total_pieces = state.peer_bitfield.bit_length;
                                    event!(
                                        Level::INFO,
                                        name = "update-bitfield",
                                        session_id = session_id,
                                        completed_pieces = completed,
                                        total_pieces = state.peer_bitfield.bit_length,
                                        "#{}: {:.2}% confirmed",
                                        session_id,
                                        100.0 * completed as f64 / total_pieces as f64
                                    );
                                }
                                Message::Request(ps) => {
                                    requests.push(ps);
                                }
                                Message::Cancel(ref _ps) => {
                                    // nothing, we don't support cancellations yet. maybe we never will.
                                }
                                Message::Piece {
                                    index: _,
                                    begin: _,
                                    data: _,
                                } => {
                                    // nothing, we don't support downloading.
                                }
                                Message::Port { dht_port: _ } => {
                                    // nothing, we don't support DHT.
                                }
                            }
                        }

                        state.sent_bytes += send_pieces(
                            &mut msg_buf[..],
                            addr,
                            &mut swh,
                            &requests,
                            &storage_engine,
                        )
                        .await?;

                        if next_bytes_out_milestone < state.sent_bytes {
                            next_bytes_out_milestone += 100 << 20;
                            let _completed = 0;

                            let completed = state.peer_bitfield.count_ones();
                            let total_pieces = state.peer_bitfield.bit_length;
                            event!(
                                Level::INFO,
                                name = "update",
                                session_id = session_id,
                                bandwidth_milestone_bytes = state.sent_bytes,
                                completed_pieces = completed,
                                total_pieces = state.peer_bitfield.bit_length,
                                "#{}: bw-out milestone of {} MB, {:.2}% confirmed",
                                session_id,
                                state.sent_bytes / 1024 / 1024,
                                100.0 * completed as f64 / total_pieces as f64
                            );
                        }

                        if next_global_stats_update < now {
                            next_global_stats_update = now + std::time::Duration::new(120, 0);

                            let mut ccl = cc.lock().await;
                            if let Some(s) = ccl.sessions.get_mut(&session_id) {
                                s.state = state.clone();
                            }
                            drop(ccl);
                        }
                    }

                    Ok(())
                };

                let (res1, res2): (Result<(), failure::Error>, Result<(), failure::Error>) =
                    futures::future::join(reader, handler).await;

                let mut ccl = cc.lock().await;
                let removed = ccl.sessions.remove(&session_id).unwrap();
                let remaining_connections = ccl.sessions.len();
                drop(ccl);

                event!(
                    Level::WARN,
                    name = "disconnected",
                    session_id = session_id,
                    sent_bytes = removed.state.sent_bytes,
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
                if let Err(err) = res2 {
                    event!(
                        Level::INFO,
                        session_id = session_id,
                        "err from client handler: {:?}",
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
) -> Result<T, failure::Error>
where
    F: Fn(&mut BytesMut) -> IResult<T, failure::Error>,
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
