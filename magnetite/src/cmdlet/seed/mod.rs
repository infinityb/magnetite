use std::collections::{hash_map, HashMap};
use std::fmt;
use std::fs::File;
use std::io::{Read, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tracing::{event, Level};
use clap::{App, Arg, SubCommand};
use tokio::net::TcpListener;
// use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use bytes::BytesMut;
use iresult::IResult;
use tokio::fs::File as TokioFile;
use salsa20::XSalsa20;
use salsa20::stream_cipher::generic_array::GenericArray;
use salsa20::stream_cipher::{NewStreamCipher, SyncStreamCipher, SyncStreamCipherSeek};
use bytes::Bytes;
use sha1::{Sha1, Digest};

// ( peer_id="YM-0.1.0-/%0C%D0h%B2%8E%CEf%CD%AF9"; info_hash="%5b%ba%8e%cc%6d%db%44%f2%02%f2%d9%9a%2d%3e%00%e8%1b%3e%d0%d6";     curl -4vvv "http://tracker.internetwarriors.net:1337/announce?info_hash=${info_hash}&peer_id=${peer_id}&port=17862&uploaded=0&downloaded=100&left=0&numwant=0&key=342c6ad5&compact=1&event=started" | xxd &&     while sleep 1854;     do         curl -4vvv "http://tracker.internetwarriors.net:1337/announce?info_hash=${info_hash}&peer_id=${peer_id}&port=17862&uploaded=0&downloaded=100&left=0&numwant=0&key=342c6ad5&compact=1" | xxd; done)
use crate::CARGO_PKG_VERSION;
use crate::model::{BitField, Truncated, ProtocolViolation, StorageEngineCorruption, TorrentID, TorrentMetaWrapped};
use crate::model::proto::{HANDSHAKE_SIZE, PieceSlice, Message, BytesCow, Handshake, deserialize, serialize};

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
                .takes_value(true)
        )
}

fn pop_messages_into(r: &mut BytesMut, messages: &mut Vec<Message<'static>>) -> Result<(), failure::Error> {
    loop {
        match deserialize(r) {
            IResult::Done(v) => messages.push(v),
            IResult::ReadMore(..) => {
                return Ok(())
            }
            IResult::Err(err) => {
                return Err(err);
            }
        }
    }
}

async fn check_collect_piece(pfse: &PieceFileStorageEngine) -> Result<(), failure::Error> {
    

    let mut req_queue: Vec<PieceSlice> = Vec::new();
    for i in 0..(pfse.piece_length / 16_384) {
        req_queue.push(PieceSlice {
            index: 100,
            begin: (i * 16_384) as u32,
            length: 16_384,
        });
    }

    let parts = collect_pieces(&req_queue[..], pfse).await?;
    
    let mut ser_buf = Vec::new();
    let mut tmp_buf = [0; 64 * 1024];

    for (idx, p) in parts.iter().enumerate() {
        let vv = serialize(&mut tmp_buf[..], p).unwrap();
        ser_buf.extend(vv);
    }

    let mut reader = BytesMut::from(&ser_buf[..]);
    let mut hasher = Sha1::new();

    for (idx, p) in parts.iter().enumerate() {
        match deserialize(&mut reader) {
            IResult::Done(Message::Piece { index, begin, data }) => {
                assert_eq!(index, 100);
                assert_eq!(begin as u64, idx as u64 * 16_384);
                assert_eq!(data.as_slice().len(), 16_384);
                hasher.input(data.as_slice());
            }
            IResult::Done(vv) => panic!("unexpected message: {:?}", vv),
            IResult::ReadMore(..) => break,
            IResult::Err(err) => panic!("error deserializing: {:?}", err),
        }
    }

    if pfse.piece_shas[100].data[..] != hasher.result()[..] {
        panic!("Error: human is dead, mismatch.");
    }

    Ok(())
}

async fn collect_pieces(
    requests: &[PieceSlice],
    pfse: &PieceFileStorageEngine,
) -> Result<Vec<Message<'static>>, failure::Error> {
    let mut messages = Vec::new();
    let mut offset: usize = 0;

    let mut pieces: HashMap<u32, Bytes> = HashMap::new();
    
    for p in requests {
        let piece_data = match pieces.entry(p.index) {
            hash_map::Entry::Occupied(occ) => occ.get().clone(),
            hash_map::Entry::Vacant(mut vac) => {
                let piece_data = pfse.get_piece(p.index).await?;
                vac.insert(piece_data).clone()
            }
        };        
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

struct PieceFileStorageEngineLockables {
    piece_file: TokioFile,
    crypto: Option<XSalsa20>,
}

#[derive(Clone)]
struct PieceFileStorageEngine {
    total_length: u64,
    piece_length: u64,
    lockables: Arc<Mutex<PieceFileStorageEngineLockables>>,
    piece_shas: Arc<Vec<TorrentID>>,
}

impl PieceFileStorageEngine {
    pub fn get_piece(&self, piece_id: u32) -> impl std::future::Future<Output=Result<Bytes, failure::Error>> {
        let lockables = Arc::clone(&self.lockables);
        let piece_shas = Arc::clone(&self.piece_shas);
        let total_length = self.total_length;
        let piece_length = self.piece_length;

        async move {
            let piece_sha = piece_shas.get(piece_id as usize)
                .ok_or_else(|| ProtocolViolation)?;

            let piece_offset_start = piece_length * u64::from(piece_id);
            let mut piece_offset_end = piece_length * u64::from(piece_id + 1);
            if total_length < piece_offset_end {
                piece_offset_end = total_length;
            }

            let mut chonker = vec![0; (piece_offset_end - piece_offset_start) as usize];
            let mut locked = lockables.lock().await;
            locked.piece_file.seek(SeekFrom::Start(piece_offset_start)).await?;
            locked.piece_file.read_exact(&mut chonker).await?;
            
            if let Some(ref mut cr) = locked.crypto {
                cr.seek(piece_offset_start);
                cr.apply_keystream(&mut chonker);
            }

            drop(locked);

            let mut hasher = Sha1::new();
            hasher.input(&chonker);
            let sha = hasher.result();
            if sha[..] != piece_sha.data[..] {
                return Err(StorageEngineCorruption.into());
            }

            Ok(Bytes::from(chonker))
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
    let torrent_file = Path::new(torrent_file).to_owned();

    let piece_file = matches.value_of_os("piece-file").unwrap();
    let piece_file = Path::new(piece_file).to_owned();
    


    let mut rt = Runtime::new()?;

    let secret: String = matches.value_of("secret").unwrap().to_string();
    
    let mut cc = ClientContext {
        session_id_seq: 0,
        torrents: HashMap::new(),
        sessions: HashMap::new(),
    };
    let mut sources = Vec::new();
    sources.push(TorrentFactory {
        torrent_file: Path::new("/home/sell/compile/magnetite/danbooru2019-0-loaded.torrent").to_owned(),
        source_file: Path::new("/mnt/home/danbooru2019-0.tome").to_owned(),
        secret: "".to_string(),
    });
    sources.push(TorrentFactory {
        torrent_file: Path::new("/home/sell/compile/magnetite/danbooru2019-1-loaded.torrent").to_owned(),
        source_file: Path::new("/mnt/home/danbooru2019-1.tome").to_owned(),
        secret: "".to_string(),
    });
    sources.push(TorrentFactory {
        torrent_file: Path::new("/home/sell/compile/magnetite/danbooru2019-2.torrent").to_owned(),
        source_file: Path::new("/mnt/home/danbooru2019-2.tome").to_owned(),
        secret: "".to_string(),
    });

    for s in sources.into_iter() {
        let mut fi = File::open(&s.torrent_file).unwrap();
        let mut by = Vec::new();
        fi.read_to_end(&mut by).unwrap();

        let mut torrent = TorrentMetaWrapped::from_bytes(&by).unwrap();
        torrent.meta.info.files = Vec::new();
        let tm = Arc::new(torrent);

        let info_hash = tm.info_hash;
        let have_bitfield = BitField::all(tm.meta.info.pieces.chunks(20).count() as u32);

        cc.torrents.insert(info_hash, Torrent {
            id: info_hash,
            meta: tm,
            have_bitfield,
            piece_file_path: s.source_file,
            crypto_secret: s.secret,
            tracker_groups: Vec::new(),
        });

        println!("added info_hash: {:?}", info_hash);
    }

    let mut cc = Arc::new(Mutex::new(cc));

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
                let handshake = match read_to_completion(&mut socket, &mut rbuf, Handshake::parse2).await {
                    Ok(hs) => hs,
                    Err(err) => {
                        event!(Level::INFO, "bad handshake from {} - {:?}", addr, bencode::BinStr(&rbuf[..]));
                        return;
                    }
                };

                let mut ccl = cc.lock().await;
                let session_id = ccl.session_id_seq;
                ccl.session_id_seq += 1;

                let torrent = match ccl.torrents.get(&handshake.info_hash) {
                    Some(tt) => tt,
                    None => {
                        event!(Level::INFO, "unknown info_hash {:?} from {}", handshake.info_hash, addr);
                        return;
                    }
                };

                let mut hs_buf = [0; HANDSHAKE_SIZE];
                let hs = Handshake::serialize_bytes(&mut hs_buf[..], &handshake.info_hash, &peer_id, &[]).unwrap();
                socket.write(&hs).await.unwrap();

                drop(torrent);
                let torrent_meta = Arc::clone(&torrent.meta);

                let target = torrent_meta.info_hash;
                let mut nonce_data = [0; 24];
                for (o, i) in nonce_data[4..].iter_mut().zip(torrent_meta.info_hash.data.iter()) {
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
                        piece_file,
                    })),
                    piece_shas: torrent_meta.piece_shas.clone(),
                };

                let have_bitfield = BitField::all(torrent_meta.meta.info.pieces.chunks(20).count() as u32);

                event!(Level::INFO, "connected #{}: {} -- {:?}", session_id, addr, handshake);
                ccl.sessions.insert(session_id, Session {
                    id: session_id,
                    addr,
                    handshake,
                    target: target,
                    state: PeerState::new(bf_length),
                });

                drop(ccl);

                let last_handshake = std::time::Instant::now();
                let (mut request_queue_tx, mut request_queue_rx) = tokio::sync::mpsc::channel(10);
                let (mut srh, mut swh) = socket.split();

                let reader = async move {
                    loop {
                        use tokio::time::{Duration, timeout};

                        let length = match timeout(Duration::new(40, 0), srh.read_buf(&mut rbuf)).await {
                            Ok(res) => res?,
                            Err(err) => {
                                let err: tokio::time::Elapsed = err;
                                request_queue_tx.send(Vec::new()).await?;
                                continue;
                            }
                        };

                        if length == 0 {
                            event!(Level::DEBUG, "#{}: EOF", session_id);
                            break;
                        }

                        let mut reqs = Vec::new();
                        pop_messages_into(&mut rbuf, &mut reqs)?;
                        event!(Level::DEBUG, "#{}: got {:?}", session_id, reqs);
                        request_queue_tx.send(reqs).await?;
                    }

                    drop(request_queue_tx);
                    
                    Ok(())
                };

                
                let handler = async {
                    let mut state = PeerState::new(bf_length);
                    let mut msg_buf = vec![0; 64 * 1024];
                    let mut requests = Vec::<PieceSlice>::new();

                    {
                        let have_bitfield = BitField::all(torrent_meta.meta.info.pieces.chunks(20).count() as u32);

                        let ss = serialize(&mut msg_buf[..], &Message::Bitfield {
                            field_data: BytesCow::Borrowed(have_bitfield.as_raw_slice()),
                        }).unwrap();
                        swh.write_all(ss).await?;
                        
                        let ss = serialize(&mut msg_buf[..], &Message::Unchoke).unwrap();
                        swh.write_all(ss).await?;
                    }

                    let mut next_bytes_out_milestone: u64 = 1024 * 1024;
                    let mut next_global_stats_update = std::time::Instant::now() + std::time::Duration::new(120, 0);
                    while let Some(work) = request_queue_rx.recv().await {
                        let now = std::time::Instant::now();

                        event!(Level::DEBUG, "#{}: processing work", session_id);
                        requests.clear();
                        if work.len() > 0 {
                            state.last_read = now;
                        }
                        if state.next_keepalive < now {
                            event!(Level::DEBUG, "#{}: sending keepalive", session_id);
                            let ss = serialize(&mut msg_buf[..], &Message::Keepalive).unwrap();
                            swh.write_all(ss).await?;
                            state.next_keepalive = now + std::time::Duration::new(90, 0);

                            let mut completed = 0;
                            for by in state.peer_bitfield.as_raw_slice() {
                                completed += by.count_ones() as u32;
                            }
                            event!(Level::DEBUG, "#{} is at {:.2}% confirmed",
                                session_id, 100.0 * (completed as f64 / state.peer_bitfield.bit_length as f64));
                        } else {
                            event!(Level::DEBUG, "#{}: next keepalive in {:?}", session_id, state.next_keepalive - now);
                        }

                        for w in work {
                            match w {
                                Message::Keepalive => {
                                    // nothing. keepalives are discarded
                                },
                                Message::Choke => {
                                    state.choked = true;
                                },
                                Message::Unchoke => {
                                    state.choked = false;
                                },
                                Message::Interested => {
                                    state.interested = true;
                                },
                                Message::Uninterested => {
                                    state.interested = false;
                                },
                                Message::Have { piece_id } => {
                                    if piece_id < state.peer_bitfield.bit_length {
                                        state.peer_bitfield.set(piece_id, true);
                                    }
                                },
                                Message::Bitfield { ref field_data } => {
                                    if field_data.as_slice().len() != state.peer_bitfield.data.len() {
                                        return Err(ProtocolViolation.into());
                                    }
                                    state.peer_bitfield.data = field_data.as_slice().to_vec().into_boxed_slice();
                                },
                                Message::Request(ps) => {
                                    requests.push(ps);
                                },
                                Message::Cancel(ref ps) => {
                                    // nothing, we don't support cancellations yet. maybe we never will.
                                },
                                Message::Piece { index, begin, data } => {
                                    // nothing, we don't support downloading.
                                },
                                Message::Port { dht_port } => {
                                    // nothing, we don't support DHT.
                                }
                            }
                        }
                        
                        state.sent_bytes += send_pieces(&mut msg_buf[..], addr, &mut swh, &requests, &storage_engine).await?;

                        if next_bytes_out_milestone < state.sent_bytes {
                            next_bytes_out_milestone += 100 << 20;
                            event!(Level::INFO, "#{}: bw-out milestone of {} MB, {:.2}% confirmed",
                                session_id,
                                state.sent_bytes / 1024 / 1024,
                                100.0 * (completed as f64 / state.peer_bitfield.bit_length as f64));
                        }
                        if next_global_stats_update < now {
                            next_global_stats_update = now + std::time::Duration::new(120, 0);

                            let mut ccl = cc.lock().await;
                            if let Some(s) = ccl.sessions.get_mut(&session_id) {
                                s.state = state.clone();
                            }
                            drop(ccl);
                        }
                    };

                    Ok(())
                };

                let (res1, res2): (
                    Result<(), failure::Error>,
                    Result<(), failure::Error>) = futures::future::join(reader, handler).await;

                let mut ccl = cc.lock().await;
                let removed = ccl.sessions.remove(&session_id).unwrap();
                let remaining_connections = ccl.sessions.len();
                drop(ccl);

                event!(Level::WARN, "disconnected #{}: {}, {} bytes written, {} connections remaining",
                        session_id, addr, removed.state.sent_bytes, remaining_connections);

                if let Err(err) = res1 {
                     event!(Level::INFO, "err from client reader: {:?}", err);
                }
                if let Err(err) = res2 {
                     event!(Level::INFO, "err from client handler: {:?}", err);
                }
            });
        }
    });

    Ok(())
}


async fn read_to_completion<T, F>(s: &mut TcpStream, buf: &mut BytesMut, p: F)
    -> Result<T, failure::Error>
    where F: Fn(&mut BytesMut) -> IResult<T, failure::Error>
{
    let mut needed: usize = 0;
    loop {
        let length = s.read_buf(buf).await?;
        if length != 0 && length < needed {
            needed -= length;
            // we definitely need more bytes, as the parser reported before.
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