use std::fmt;
use std::io::SeekFrom;
use std::pin::Pin;
use std::time::SystemTime;

use std::sync::Arc;

use bytes::Bytes;

use futures::future::FutureExt;
use salsa20::stream_cipher::{NewStreamCipher, SyncStreamCipher, SyncStreamCipherSeek};
use salsa20::XSalsa20;
use sha1::{Digest};
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncSeekExt};

use tokio::sync::Mutex;

use crate::model::{ProtocolViolation, TorrentID};
use super::PieceStorageEngine;

pub const DOWNLOAD_CHUNK_SIZE: usize = 16 * 1024;

#[derive(Debug)]
pub enum RemoteMagnetiteStorageEngineVerifyMode {
    Never,
    New,
    Always,
}

// we also need this in the PieceFile storage engine. lets generalize and use composition, if we can.
#[derive(Clone)]
pub struct Inflight {
    // starts off as None and is eventually resolved exactly once.
    finished: watch::Receiver<Option<Result<Bytes, failure::Error>>>,
}

impl Inflight {
    pub fn complete(self) -> impl std::future::Future<Output = Result<Bytes, failure::Error>> + Send {
        async move {
            self.finished.recv().
        }
    }
}

pub struct PieceCacheEntry {
    piece_length: u64,
    file_offset: u64,
    last_touched: SystemTime,
}

pub struct RemoteMagnetiteStorageEngineLockables {
    pub piece_inflight: HashMap<u32, Inflight>,
    // piece_id => file offset in self.piece_cache_storage, and naturally,
    // total cached size = parent.piece_length * self.piece_cache.len()
    pub cache_size_cur: u64,
    pub piece_cache: BTreeMap<(TorrentID, u32), PieceCacheEntry>,
    pub piece_cache_storage: TokioFile,
    pub verify_piece: RemoteMagnetiteStorageEngineVerifyMode,
    pub crypto: Option<XSalsa20>,
}

impl fmt::Debug for RemoteMagnetiteStorageEngineLockables {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RemoteMagnetiteStorageEngineLockables")
            .field("piece_file", &self.piece_file)
            .finish()
    }
}

#[derive(Clone)]
pub struct RemoteMagnetiteStorageEngine {
    pub cache_size_max: u64,
    pub total_length: u64,
    pub piece_length: u64,
    pub lockables: Arc<Mutex<RemoteMagnetiteStorageEngineLockables>>,
    pub piece_shas: Arc<Vec<TorrentID>>,
}

impl fmt::Debug for RemoteMagnetiteStorageEngine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RemoteMagnetiteStorageEngine")
            .field("total_length", &self.total_length)
            .field("piece_length", &self.piece_length)
            .finish()
    }
}

impl PieceStorageEngineMut for PieceFileStorageEngine {
    fn write_chunk(
        &self,
        content_key: &TorrentID,
        piece_id: u32,
        chunk_offset: u32,
        data: Bytes,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<WriteChunkResponse, MagnetiteError>> + Send>> {
        struct HeapEntry<'a> {
            last_touched: SystemTime,
            piece_length: u64,
            btree_key: &'a (TorrentID, u32),
        }

        impl<'a> Ord for HeapEntry<'a> {
            fn cmp(&self, other: &Self) -> Ordering {
                self.last_touched.cmp(&other.last_touched)
            }
        }

        impl<'a> PartialOrd for HeapEntry<'a> {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl<'a> PartialEq for HeapEntry<'a> {
            fn eq(&self, other: &Self) -> bool {
                self.last_touched == other.last_touched
            }
        }

        let info_hash = self.info_hash;
        let lockables = Arc::clone(&self.lockables);
        let piece_shas = Arc::clone(&self.piece_shas);
        // FIXME: integer underflow when cache_size_max < data.len()
        let cache_size_max = self.cache_size_max;
        let total_length = self.total_length;
        let piece_length = self.piece_length;
        async move {
            let locked = lockables.lock().await;
            
            let position = locked.piece_cache_storage.seek(SeekFrom::End(0)).await?;
            assert_eq!(position % self.piece_length, 0);

            let cache_size_after = locked.cache_size_cur + data.len() as u64;
            if cache_size_max < cache_size_after {
                let mut target_free = 0;
                let mut discard = BinaryHeap::new();

                let mut cache_size_cur = 0;
                let mut discard_size_target = cache_size_max - data.len() as u64;
                let mut discard_size_found = 0;

                for (k, v) in locked.piece_cache {                    
                    // take off the newest items until we're below the found threshold.
                    while let Some(v) = discard.peek() {
                        let next_discard_size_found = discard_size_found - v.piece_length;
                        if discard_size_target < next_discard_size_found {
                            discard_size_found = next_discard_size_found;

                            drop(v);
                            drop(discard.pop().unwrap());
                        } else {
                            break;
                        }
                    }

                    discard_size_found += v.piece_length;
                    discard.push(HeapEntry {
                        last_touched: v.last_touched,
                        piece_length: v.piece_length,
                        btree_key: k,
                    });

                    cache_size_cur += v.piece_length;
                }

                locked.cache_size_cur = cache_size_cur;
            }

            locked.piece_cache_storage.write_all(&data[..]).await?;
            locked.piece_cache.insert((tid, piece_id), PieceCacheEntry {
                file_offset: position,
                last_touched: SystemTime::now(),
            });
            
            if let Some(inf) = locked.piece_inflight.remove(piece_id) {
                inf.finished.
            }
        }.boxed()
    }
}

impl PieceStorageEngine for RemoteMagnetiteStorageEngine {
    fn get_piece(
        &self,
        content_key: &TorrentID,
        piece_id: u32,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, failure::Error>> + Send>> {
        let lockables = Arc::clone(&self.lockables);
        let piece_shas = Arc::clone(&self.piece_shas);
        let total_length = self.total_length;
        let piece_length = self.piece_length;

        async move {
            let piece_sha: &TorrentID = piece_shas
                .get(piece_id as usize)
                .ok_or_else(|| ProtocolViolation)?;

            let lockables_later = Arc::clone(&lockables);
            let mut locked = lockables.lock().await;
            if let Some(inf) = locked.piece_inflight.get(piece_id) {
                return inf.clone().complete().await.boxed();
            }

            let (tx, rx) = watch::channel(None);
            let inf = Inflight { finished: rx };
            locked.piece_inflight.insert(piece_id, inf.clone());
            if let Some(off) = locked.piece_cache.get(piece_id) {
                tokio::spawn(async {
                    let resolution = load_piece_from_cache(lockables_later, off).await;

                    // mutate the result's Ok value in-place, if we have a crypto
                    // configuration.
                    if let Ok(ref mut piece_data) = resolution {
                        if let Some(ref mut cr) = locked.crypto {
                            cr.seek(piece_offset_start);
                            cr.apply_keystream(&mut piece_data[..]);
                        }
                    }

                    drop(tx); // unimplement: make it send.
                });

                return inf.complete().await.boxed();
            }

            // go out to the network to get the piece,

            Ok(Bytes::from(chonker))
        }.boxed()
    }
}
