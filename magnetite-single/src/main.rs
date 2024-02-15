use std::env;
use std::os::fd::{RawFd, OwnedFd, FromRawFd};
use std::io::{self, Seek, SeekFrom, Read};
use std::fs::File;
use std::fmt;

use rand::{SeedableRng, Rng};
use rand::rngs::StdRng;
use bytes::BytesMut;
use clap::{Arg, ArgAction, value_parser};
use failure::ResultExt;
// use metrics_runtime::Receiver;

use tracing::{span, event, Level};
use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
use tracing_subscriber::FmtSubscriber;

use magnetite_model::{TorrentMetaWrapped, BitField};
use magnetite_common::TorrentId;
use magnetite_common::proto::{serialize, PieceSlice, Message, Handshake, BadHandshake, IResult, IErr, HANDSHAKE_SIZE, deserialize_pop_opt};
use magnetite_single_api::{tokio, tonic, TorrentSessionStartItem, proto::TorrentSessionStartRequest};
use magnetite_single_api::tokio::io::{AsyncReadExt, AsyncWriteExt};
use magnetite_single_api::tokio::net::TcpStream;
use magnetite_single_api::tokio::runtime;
use magnetite_single_api::tokio::time::{self, timeout, timeout_at, Duration};


const KEEPALIVE_INTERVAL: Duration = Duration::new(120, 0);
const CONNECTION_IDLE_TIMEOUT: Duration = Duration::new(360, 0); // 3 * KEEPALIVE_INTERVAL
const HANDSHAKE_TIMEOUT: Duration = Duration::new(12, 0);

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_NAME: &str = env!("CARGO_PKG_NAME");
const STDIN: RawFd = 0;

#[derive(Debug)]
// XXX: duplicated
pub struct UnknownInfoHash(TorrentId);

impl fmt::Display for UnknownInfoHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UnknownInfoHash({:?})", self.0)
    }
}

impl std::error::Error for UnknownInfoHash {}

struct PieceSliceSmallSlice<'a>(&'a [PieceSlice]);

impl<'a> fmt::Display for PieceSliceSmallSlice<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut first = true;
        for v in self.0 {
            if !first {
                write!(f, ", ")?;
            }
            first = false;
            write!(f, "{}", v.fmt_small())?;
        }
        Ok(())
    }
}

fn main() -> Result<(), failure::Error> {
    let mut my_subscriber_builder = FmtSubscriber::builder()
        // .with_ansi(false)
        .json()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL);
        //.pretty();

    let app = clap::Command::new(CARGO_PKG_NAME)
        .version(CARGO_PKG_VERSION)
        .author("Stacey Ell <stacey.ell@gmail.com>")
        .about("Demonstration Torrent Seeder")
        // .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(
            Arg::new("v")
                .short('v')
                .action(ArgAction::Count)
                .value_parser(value_parser!(u8).range(..5))
                .help("Sets the level of verbosity"),
        )
        .arg(
            Arg::new("peer-id")
                .action(ArgAction::Set)
                .value_parser(value_parser!(TorrentId))
                .help("Sets our own peer-id)"),
        )
        .arg(
            Arg::new("info-hash")
                .action(ArgAction::Set)
                .value_parser(value_parser!(TorrentId))
                .help("Sends the info-hash and sends the handshake first (for outgoing connections)"),
        );

    let matches = app.get_matches();
    let verbosity = matches.get_count("v");
    let should_print_test_logging = 4 < verbosity;
    my_subscriber_builder = my_subscriber_builder.with_max_level(match verbosity {
        0 => TracingLevelFilter::ERROR,
        1 => TracingLevelFilter::WARN,
        2 => TracingLevelFilter::INFO,
        3 => TracingLevelFilter::DEBUG,
        _ => TracingLevelFilter::TRACE,
    });

    tracing::subscriber::set_global_default(my_subscriber_builder.finish())
        .expect("setting tracing default failed");

    if should_print_test_logging {
        print_test_logging();
    }

    let rt = runtime::Builder::new_current_thread().enable_io().enable_time().build()?;
    rt.block_on(async {
        if let Err(e) = main2(&matches).await {
            if e.downcast_ref::<UnknownInfoHash>().is_some() {
                return Ok(());
            }
                
            let root = e.find_root_cause();
            if root.downcast_ref::<BadHandshake>().is_some() {
                return Ok(());
            }
            if root.downcast_ref::<io::Error>().is_some() {
                event!(Level::ERROR, "--- i/o error +++: {}", e);
                return Err(e);
            }
            event!(Level::ERROR, "{}", e);
            return Err(e)
        }
        Ok(())
    })
}

async fn open_torrent_meta_file(info_hash: &TorrentId) -> Result<File, failure::Error> {
    let span = span!(Level::INFO, "open_torrent_meta_file");
    let _guard = span.enter();

    let filename = format!("/storage/users/sell/ceph-transmission-backup/torrent-hive/torrents/{}.torrent", info_hash.hex());
    if let Ok(file) = File::open(&filename) {
        return Ok(file);
    }

    let filename = format!("/mnt/media/torrents/{}.torrent", info_hash.hex());
    let file = File::open(&filename).with_context(|_| {
        format!("opening {:?}", filename)
    })?;

    event!(Level::WARN,
        info_hash = %info_hash.hex(),
        "open_torrent_meta_file.fallback");

    Ok(file)
}

async fn load_torrent_meta_file(info_hash: &TorrentId) -> Result<TorrentMetaWrapped, failure::Error> {
    let mut file = open_torrent_meta_file(info_hash).await.with_context(|_| {
        UnknownInfoHash(*info_hash)
    })?;
    
    let mut by = Vec::new();
    file.read_to_end(&mut by)?;

    let torrent = TorrentMetaWrapped::from_bytes(&by)?;
    event!(Level::ERROR,
        name = torrent.meta.info.name,
        piece_count = torrent.meta.info.pieces.len(),
        "load_torrent_meta");

    if !torrent.meta.info.files.is_empty() {
        failure::bail!("multi-torrent not yet supported");
    }

    Ok(torrent)
}

async fn open_torrent_data_file(info_hash: &TorrentId) -> Result<File, failure::Error> {
    let span = span!(Level::INFO, "open_torrent_data_file");
    let _guard = span.enter();

    let filename = format!("/storage/users/sell/ceph-transmission-backup/torrent-hive/data/{}/0", info_hash.hex());
    if let Ok(file) = File::open(&filename) {
        return Ok(file);
    }

    let filename = format!("/mnt/media/data/{}/0", info_hash.hex());
    let file = File::open(&filename).with_context(|_| {
        format!("opening {:?}", filename)
    })?;

    event!(Level::WARN,
        info_hash = %info_hash.hex(),
        "open_torrent_data_file.fallback");

    Ok(file)
}

struct PieceDataRequestsDispatcher<'a> {
    file: File,
    torrent: &'a TorrentMetaWrapped,
    experimental_avoid_seek: bool,

    seeks: i32,
    file_offset: u64,
    piece_data_requests_iter: std::slice::Iter<'a, PieceSlice>,
}

impl<'a> PieceDataRequestsDispatcher<'a> {
    fn new(
        file: File,
        torrent: &'a TorrentMetaWrapped,
        piece_data_requests: &'a [PieceSlice]
    ) -> Result<Self, failure::Error> {
        Ok(PieceDataRequestsDispatcher {
            file, torrent,
            experimental_avoid_seek: false,

            seeks: 0,
            file_offset: 0,
            piece_data_requests_iter: piece_data_requests.iter(),
        })
    }

    async fn dispatch(&mut self, wbuf: &mut BytesMut) -> Result<Option<u64>, failure::Error> {
        if let Some(req) = self.piece_data_requests_iter.next() {
            let want_offset = u64::from(req.index) * u64::from(self.torrent.meta.info.piece_length)
                + u64::from(req.begin);

            if self.experimental_avoid_seek {
                if self.file_offset != want_offset {
                    self.seeks += 1;
                    self.file.seek(SeekFrom::Start(want_offset))?;
                    self.file_offset = want_offset;
                }
            } else {
                self.seeks += 1;
                self.file.seek(SeekFrom::Start(want_offset))?;
            }

            let mut buf = vec![0; req.length as usize];
            self.file.read_exact(&mut buf[..])?;
            self.file_offset += u64::from(req.length);

            serialize(wbuf, &Message::Piece {
                index: req.index,
                begin: req.begin,
                data: buf.into(),
            });

            Ok(Some(u64::from(req.length)))
        } else {
            return Ok(None);
        }
    }
}

async fn main2(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    use magnetite_single_api::proto::magnetite_client::MagnetiteClient;
    let mut client = MagnetiteClient::connect("http://[::1]:10000").await?;
    let mut rng: StdRng = StdRng::from_rng(&mut rand::thread_rng())?;
    let experimental_avoid_seek = rng.gen::<u8>() < 12;

    let stdin = unsafe { OwnedFd::from_raw_fd(STDIN) };
    let stream = std::net::TcpStream::from(stdin);
    let mut stream = TcpStream::from_std(stream)?;

    let mut rbuf = BytesMut::new();
    let mut wbuf = BytesMut::new();
    
    let mut sent_handshake = false;
    let mut self_pid = TorrentId::zero();
    if let Some(info_hash) = matches.get_one::<TorrentId>("info-hash") {
        sent_handshake = true;
        self_pid = matches.get_one::<TorrentId>("peer-id").cloned().ok_or_else(|| {
            failure::format_err!("peer-id and info-hash are mutually inclusive")
        })?;

        let mut hs_buf = [0; HANDSHAKE_SIZE];
        let hs = Handshake::serialize_bytes(&mut hs_buf[..], info_hash, &self_pid, &[]).unwrap();
        stream.write_all(&hs).await?;
        stream.flush().await?;
    }

    let handshake_read = read_to_completion(&mut stream, &mut rbuf, |s| {
        Handshake::parse(s).map_err(|e| match e {
            IErr::Error(e) => IErr::Error(e.into()),
            IErr::Incomplete(more_cnt) => IErr::Incomplete(more_cnt),
        })
    });
    let handshake = timeout(HANDSHAKE_TIMEOUT, handshake_read).await
        .with_context(|_| "handshake timeout expired")?
        .with_context(|_| "during handshake")?;

    if let Some(info_hash) = matches.get_one::<TorrentId>("info-hash") {
        if info_hash != &handshake.info_hash {
            failure::bail!("peer returned different info-hash in handshake!");
        }
    }

    let mut managed = true;
    let mut choking_peer = true;
    let mut sent_data_bytes = 0;
    let mut send_data_permit_bytes = 0;
    let torrent = load_torrent_meta_file(&handshake.info_hash).await?;

    let mut req = tonic::Request::new(TorrentSessionStartRequest {
        info_hash: format!("{}", handshake.info_hash.hex()),
    });
    let req_meta = req.metadata_mut();
    req_meta.insert("x-peer-id", handshake.peer_id.hex().to_string().parse().unwrap());
    req_meta.insert("x-peer-addr", stream.peer_addr()?.to_string().parse().unwrap());
    let mut session_resp = client.start_session(req).await?;
    event!(Level::INFO, metadata=?session_resp.metadata(), "session_init");

    let mut bitfield = BitField::none(torrent.meta.info.pieces.len() as u32);
    let mut session_id = String::new();
    let mut peer_id = TorrentId::zero();
    if let Some(next_message) = session_resp.get_mut().message().await? {
        use magnetite_single_api::proto::torrent_session_start_item::Item;

        match next_message.item {
            Some(Item::Init(ref init)) => {
                session_id = init.session_id.clone();
                self_pid = init.peer_id.parse()?;
                bitfield.copy_from_slice(&init.replace_bitfield);
            },
            _ => {
                failure::bail!("no starting bitfield - got {:?}", next_message.item);
            }
        }
    } else {
        failure::bail!("no starting bitfield.");
    }

    let span = span!(Level::INFO, "torrent_session_start",
        version = %CARGO_PKG_VERSION,
        info_hash = %handshake.info_hash.hex(),
        remote_peer_id = %handshake.peer_id.hex(),
        remote_peer_addr = %stream.peer_addr()?,
        local_peer_id = %self_pid.hex(),
        experimental_avoid_seek,
    );
    let _guard = span.enter();
    if !sent_handshake {
        let mut hs_buf = [0; HANDSHAKE_SIZE];
        let hs = Handshake::serialize_bytes(&mut hs_buf[..], &handshake.info_hash, &self_pid, &[]).unwrap();
        stream.write_all(&hs).await?;
        stream.flush().await?;
    }

    let bitfield = BitField::all(torrent.meta.info.pieces.len() as u32);    
    serialize(&mut wbuf, &Message::bitfield(bitfield.as_raw_slice()));

    let mut piece_data_requests: Vec<PieceSlice> = Vec::new();
    let mut keepalive_next = tokio::time::Instant::now() + KEEPALIVE_INTERVAL;
    let mut last_rx = tokio::time::Instant::now();

    let res = async {
        loop {
            assert!(piece_data_requests.is_empty());

            if send_data_permit_bytes <= sent_data_bytes && !choking_peer {
                choking_peer = true;
                serialize(&mut wbuf, &Message::Choke);
                event!(Level::DEBUG, "choke");
            }
            if sent_data_bytes < send_data_permit_bytes && choking_peer {
                choking_peer = false;
                serialize(&mut wbuf, &Message::Unchoke);
                event!(Level::DEBUG, "unchoke");
            }

            let loop_now = tokio::time::Instant::now();
            if wbuf.len() > 0 {
                keepalive_next = loop_now + KEEPALIVE_INTERVAL;
                stream.write_buf(&mut wbuf).await?;
                stream.flush().await?;
            }

            if (loop_now - last_rx) > CONNECTION_IDLE_TIMEOUT {
                failure::bail!("no messages received recently");
            }
            
            while let Some(v) = deserialize_pop_opt(&mut rbuf)? {
                event!(Level::DEBUG,
                    message = ?v.debug_small(),
                    "torrent_proto_message");

                if let Message::Request(ref ps) = v {
                    if 64 * 1024 < ps.length {
                        failure::bail!("piece size req too large: {}", ps.length);
                    }
                    piece_data_requests.push(*ps);
                    if piece_data_requests.len() >= 32 {
                        break;
                    }
                }
            }

            if piece_data_requests.len() > 0 {
                let mut total_size_bytes: u64 = 0;
                for req in &piece_data_requests {
                    total_size_bytes += u64::from(req.length);
                }

                let span = span!(Level::INFO, "torrent_data_read");
                let _guard = span.enter();

                let file = open_torrent_data_file(&handshake.info_hash).await.with_context(|_| {
                    format!("opening data {:?}", handshake.info_hash)
                })?;

                let mut disp = PieceDataRequestsDispatcher::new(file, &torrent, &piece_data_requests)?;
                disp.experimental_avoid_seek = experimental_avoid_seek;
                while let Some(data_bytes) = disp.dispatch(&mut wbuf).await? {
                    if wbuf.len() > 0 {
                        keepalive_next = loop_now + KEEPALIVE_INTERVAL;
                        stream.write_buf(&mut wbuf).await?;
                        stream.flush().await?;
                    }
                    sent_data_bytes += data_bytes;
                }

                event!(Level::DEBUG,
                    seeks = disp.seeks,
                    request_count = piece_data_requests.len(),
                    total_size_bytes = total_size_bytes,
                    pieces = %PieceSliceSmallSlice(&piece_data_requests),
                    "torrent_data_read_finalize");
                continue;
            }

            let sleep = time::sleep_until(keepalive_next);
            tokio::pin!(sleep);
            let session_iter = session_resp.get_mut();
            tokio::pin!(session_iter);

            tokio::select! {
                _ = &mut sleep, if !sleep.is_elapsed() => {
                    // elapsed
                    serialize(&mut wbuf, &Message::Keepalive);
                    continue;
                }
                r = stream.read_buf(&mut rbuf) => {
                    let got_bytes = r?;
                    if got_bytes == 0 && wbuf.is_empty() {
                        return Ok(());
                    }
                    if got_bytes > 0 {
                        last_rx = loop_now;
                    }
                }
                m = session_iter.message(), if managed => {
                    use magnetite_single_api::proto::torrent_session_start_item::Item;
                    if let Some(ms) = m? {
                        let md = TorrentSessionStartItem::try_from(ms)?;
                        match &md.item {
                            Item::SetSeedPermitBytes(permit) => {
                                let permit = *permit;
                                let before_permit = send_data_permit_bytes;
                                send_data_permit_bytes = permit;
                                if before_permit < permit {
                                    event!(Level::DEBUG,
                                        permit_added=(permit - before_permit),
                                        "permitting more data");
                                }
                            },
                            _ => {
                                event!(Level::WARN,
                                    message = ?md,
                                    "session_unhandled_datagram");
                            }
                        }
                    } else {
                        managed = false;
                    }
                }
            }
        }
    }.await;

    if let Err(ref err) = res {
        event!(Level::ERROR,
            sent_data_bytes = sent_data_bytes,
            error = ?err,
            "torrent_session_finalize");
    } else {
        event!(Level::INFO,
            sent_data_bytes = sent_data_bytes,
            "torrent_session_finalize");
    }

    res
}


async fn read_to_completion<T, F>(
    s: &mut TcpStream,
    buf: &mut BytesMut,
    p: F,
) -> Result<T, failure::Error>
where
    F: Fn(&[u8]) -> IResult<T, failure::Error>,
{
    let mut needed: usize = 0;
    loop {
        let length = s.read_buf(buf).await?;
        if length != 0 && length < needed {
            needed -= length;
            // we need more bytes, as the parser reported before.
            continue;
        }
        match p(&buf[..]) {
            IResult::Ok((length, val)) => {
                drop(buf.split_to(length));
                return Ok(val);
            }
            IResult::Err(IErr::Error(e)) => return Err(e.into()),
            IResult::Err(IErr::Incomplete(more_cnt)) => {
                needed = more_cnt;
                if length == 0 {
                    failure::bail!("EOF");
                }
            }
        }
    }
}


#[allow(clippy::cognitive_complexity)] // macro bug around event!()
fn print_test_logging() {
    event!(Level::TRACE, "logger initialized - trace check");
    event!(Level::DEBUG, "logger initialized - debug check");
    event!(Level::INFO, "logger initialized - info check");
    event!(Level::WARN, "logger initialized - warn check");
    event!(Level::ERROR, "logger initialized - error check");
}
