use std::sync::Arc;
use std::pin::Pin;
use std::collections::VecDeque;
use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::fs::OpenOptions;
use std::panic;
use std::convert::TryInto;

use tokio::sync::{broadcast};
use futures::stream::{self, SelectAll, StreamExt};
use tracing::{event, Level};
use futures::future::FutureExt;

use magnetite_common::TorrentId;

use crate::{CommonInit, BusMessage};
use crate::utils::close_waiter::{Holder, Done};
use crate::storage::DOWNLOAD_CHUNK_SIZE;
use crate::model::{BitField, TorrentMetaWrapped, MagnetiteError};
use crate::bittorrent::peer_state::{
    merge_global_payload_stats, GlobalState, PeerState, Session, Stats,
};

#[derive(Clone)]
pub struct CommonInitTasklet {
    ebus: broadcast::Sender<BusMessage>,
    ebus_rx: broadcast::Receiever<BusMessage>,
    term_sig: Done,
}

struct TorrentStateInit {
    committed_bitfield: BitField,
    wal_state: (),
}

struct AppConfig {
    wal_directory: PathBuf,
}

const PEER_RBUF_SIZE_BYTES: usize = 32 * 1024;

// fn start_torrent_task_seed_only(
//     init: CommonInitTasklet,
//     app_config: Arc<AppConfig>,
//     metainfo: TorrentMetaWrapped,
//     torrent_init: TorrentStateInit,
// ) -> Pin<Box<dyn std::future::Future<Output = Result<(), MagnetiteError>> + Send + 'static>> 
// {
//     unimplemented!();
// }

fn start_torrent_task_leeching(
    init: CommonInitTasklet,
    app_config: Arc<AppConfig>,
    metainfo: TorrentMetaWrapped,
    torrent_init: TorrentStateInit,
) -> Pin<Box<dyn std::future::Future<Output = Result<(), MagnetiteError>> + Send + 'static>> 
{
    // TODO: Use multilayer like level db so we can compact related chunks
    // together into pieces.
    let join = tokio::task::spawn(async move {
        let file_name = format!("magnetite.{}.L0-{:03x}-{:02x}.wal", metainfo.info_hash.hex(), 0, 0);

        let file_path = wal_directory.join(file_name);

        let wal_level0 = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(file_path)?;

        // need some way to let peer tasks send HAVEs to torrent task.  
        struct PeerState {
            holder: Holder,  // replace with cmd channel?
            bitfield: BitField,
        }
        let mut states: BTreeMap<Ksuid, PeerState> = HashMap::new();
        let TorrentInit { mut committed_bitfield } = torrent_init;
        let mut inflight_pieces = BitField::none(committed_bitfield.len());
        
        let mut connections_futures = SelectAll::new();
        let mut wal_state: BTreeMap<u32, PieceState> = Default::default();
        let mut piece_field = Vec::with_capacity(inflight_pieces.len() as usize);

        let rng = thread_rng();
        for isset in committed_bitfield.iter() {
            piece_field.push(PieceInfo {
                status: if isset { PieceInfoStatus::Committed } else { PieceInfoStatus::Unqueued },
                random_cookie: rng.gen(),
                seen_count: 0,
            });
        }
        drop(rng);

        loop {
            let mut ebus_running = true;
            tokio::select! {
                creq = ebus_rx.recv(), if ebus_running => {
                    // if creq is BusKillConnection, search for connection in holders
                    // and remove - dropping the future will signal to the connection
                    // task that it is no longer desired.

                    // if creq is BusAddPeerSource for our info hash, spawn a new outgoing
                    // task for that connection.  The spawned task may need to inform
                    // the requester that the connection attempt was successful.
                }

                fut = connections_futures.next(), if !connections_futures.is_empty() => {
                    let PeerState { holder, bitfield } = states.remove(&fut.session_id).unwrap();
                    drop(holder);

                    let mut broken_state = false;
                    for (isset, pi) in committed_bitfield.iter().zip(&mut piece_field) {
                        if isset {
                            if pi.seen_count == 0 {
                                broken_state = true;
                            }
                            pi.seen_count = pi.seen_count.saturating_sub(1);
                        }
                    }
                    if broken_state {
                        event!(Level::ERROR, "internal state error - piecemap is now inconsistent");
                    }

                    if let Err(err) = fut.result {
                        event!(Level::ERROR, "connection {} terminated with error {}",
                            fut.session_id.base62(), err);
                    }
                }
            }

            let piece_queue = VecDeque::with_capacity(PIECE_QUEUE_LENGTH as usize);

            struct ConnectionCtx {
                session_id: Ksuid,
                result: Result<(), MagnetiteError>,
            }

            states.insert(session_id, PeerState {
                holder,
                bitfield: BitField::zero(metainfo.piece_shas.len() as u32),
            });

            connections_futures.push(async move {
                let result = match spawn_connection(init.clone()).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(mag_err)) => Err(mag_err),
                    Err(err) => {
                        event!(Level::ERROR, "tokio task error: {}", err);

                        if err.is_panic() {
                            panic::resume_unwind(err.into_panic());
                        }

                        Err(MagnetiteError::InternalError {
                            msg: format!("tokio task error: {}", err),
                        })
                    }
                };
                ConnectionCtx { session_id, result }
            }.boxed());
        }
    });

    async move {
        match join.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(mag_err)) => Err(mag_err),
            Err(err) => {
                event!(Level::ERROR, "tokio task error: {}", err);

                if err.is_panic() {
                    panic::resume_unwind(err.into_panic());
                }

                Err(MagnetiteError::InternalError {
                    msg: format!("tokio task error: {}", err),
                })
            }
        }
    }.boxed()
}

#[derive(Copy, Clone, Eq, PartialEq)]
struct WalKey {
    // only one level of WAL, and only one shard - so we can just use a position
    // here.  All lengths are DOWNLOAD_CHUNK_SIZE.  Later we'll include the WAL
    // level and index here.
    position: u64,
}


struct WalState {
    pieces: BTreeMap<(TorrentId, u32), PieceState>,
}

struct PieceState {
    info_hash: TorrentId,
    chunk_size: u32,
    chunks: BTreeMap<u32, WalKey>,
}

enum PieceStateCompletion {
    Complete,
    Incomplete,
}

impl PieceState {
    pub fn new(info_hash: TorrentId, chunk_size: u32) -> PieceState {
        PieceState {
            info_hash,
            chunk_size,
            chunks: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, piece_offset: u32, key: WalKey) -> PieceStateCompletion {
        assert_eq!(piece_offset % DOWNLOAD_CHUNK_SIZE, 0);
        let chunk_index = piece_offset / DOWNLOAD_CHUNK_SIZE;
        assert(chunk_index <= self.chunk_size);
        if self.chunks.insert(chunk_index, key).is_some() {
            event!(Level::WARN, "chunk {}#{} resubmitted", self.info_hash.hex(), chunk_index);
        }
        if self.is_complete() {
            PieceStateCompletion::Complete
        } else {
            PieceStateCompletion::Incomplete
        }
    }

    pub fn is_complete(&self) -> bool {
        self.chunks.len() as u32 == self.chunk_size
    }
}

const PIECE_QUEUE_LENGTH: u32 = 128;

mod piece_queue_random {
    use std::cmp::{Ordering, min};

    use super::{PieceInfo, PieceInfoStatus};

    #[derive(Debug)]
    struct HeapEntry {
        random_cookie: u32,
        piece_index: u32,
    }

    impl Eq for HeapEntry {}

    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> Ordering {
            self.random_cookie.cmp(&other.random_cookie)
        }
    }

    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl PartialEq for HeapEntry {
        fn eq(&self, other: &Self) -> bool {
            self.random_cookie == other.random_cookie
        }
    }

    pub fn mark_piece_failed(pi: &mut PieceInfo) {
        pi.random_cookie = thread_rng().gen();
        pi.status = PieceInfoStatus::Unqueued;
    }

    pub fn fill_piece_queue(pq: &mut VecDeque<u32>, piece_info: &mut [PieceInfo]) {
        let fill_requirement = PIECE_QUEUE_LENGTH - min(pq.len() as u32, PIECE_QUEUE_LENGTH);
        
        if fill_requirement == 0 {
            // already full, no work to do.
            return;
        }

        let mut best_pieces: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(fill_requirement);
        for (piece_index, pi) in piece_info.iter().enumerate() {
            let piece_index: u32 = piece_index.try_into().unwrap();

            if pi.status != PieceInfoStatus::Unqueued {
                continue;
            }

            if pi.seen_count == 0 {
                continue;
            }

            let entry = HeapEntry {
                random_cookie: pi.random_cookie,
                piece_index,
            };
 
            if best_pieces.len() == fill_requirement {
                // full - peek and compare, then push if new item is smaller.
                // unwrap is fine since we've verified that fill_requirement > 0.
                let top = best_pieces.peek().unwrap();
                if entry < *top {
                    best_pieces.pop().unwrap();
                    best_pieces.push(entry);
                }
            } else {
                // not full - just push.
                best_pieces.push(entry);
            }
        }

        // order doesn't need to be super strict, so don't sort while extending
        // the queue.
        for v in best_pieces.drain() {
            pq.push(v.piece_index);
            piece_info[v.piece_index as usize].status = PieceInfoStatus::Queued;
        }
    }
}

#[derive(Eq, PartialEq)]
enum PieceInfoStatus {
    // In stable storage.
    Committed,
    // Obtained and in the WAL, but not written to stable storage yet.
    Uncommitted,
    // Assigned to a peer, in the process of downloading
    Inflight,
    // Queued and ready to be assigned to a peer.
    Queued,
    // None of the above - liable to be enqueued.
    Unqueued,
}

struct PieceInfo {
    status: PieceInfoStatus,
    random_cookie: u32,
    // saturating but we're unlikely to hit 2**16 concurrent connections
    seen_count: u16,
}

pub fn start_torrent_task(
    app_config: Arc<AppConfig>,
    metainfo: TorrentMetaWrapped,
    torrent_init: TorrentInit,
) -> Pin<Box<dyn std::future::Future<Output = Result<(), MagnetiteError>> + Send + 'static>> {
    // if committed_bitfield.is_filled() {
    //     return start_torrent_task_seed_only(app_config, metainfo, torrent_init);
    // }

    return start_torrent_task_leeching(app_config, metainfo, torrent_init);
}

