use std::collections::BTreeMap;
use std::fmt;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use bytes::{Bytes, BytesMut};

use futures::future::FutureExt;
use salsa20::stream_cipher::{SyncStreamCipher, SyncStreamCipherSeek};
use salsa20::XSalsa20;
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{watch, Mutex};
use tracing::{event, Level};

#[cfg(target_os = "linux")]
use nix::fcntl::{fallocate, FallocateFlags};

use super::piggyback::Inflight;
use crate::model::{MagnetiteError, TorrentID};
use crate::storage::{GetPieceRequest, PieceStorageEngineDumb};

#[derive(Clone)]
struct PieceCacheEntry {
    last_touched: SystemTime,
    piece_length: u32,
    // none if it's not written yet, which may be the case if we're going upstream.
    position: Option<u64>,
    // none if we don't have any pending requests for this piece and it is merely cached.
    inflight: Option<Inflight>,
}

impl fmt::Debug for PieceCacheEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PieceCacheEntry")
            .field("last_touched", &self.last_touched)
            .field("piece_length", &self.piece_length)
            .field("position", &self.position)
            .finish()
    }
}

impl PieceCacheEntry {
    pub fn new_with_piece_length(piece_length: u32) -> PieceCacheEntry {
        PieceCacheEntry {
            last_touched: SystemTime::now(),
            piece_length,
            position: None,
            inflight: None,
        }
    }
}

struct PieceCacheInfo {
    cache_size_max: u64,
    cache_file_offset: u64,
    cache_size_cur: u64,
    pieces: BTreeMap<(TorrentID, u32), PieceCacheEntry>,
}

#[derive(Clone)]
pub struct CacheWrapper<P> {
    // pieces and punched holes will always start at a multiple of this alignment
    // pieces will be padded to their piece_length, if shorter.
    cache_alignment: u64,

    piece_cache: Arc<Mutex<PieceCacheInfo>>,
    cache_file: Arc<Mutex<TokioFile>>,
    crypto: Option<Arc<Mutex<XSalsa20>>>,

    upstream: P,
}

impl CacheWrapper<()> {
    pub fn build_with_capacity_bytes(cap_bytes: u64) -> Builder {
        Builder {
            cache_alignment: 128 * 1024,
            cache_size_max: cap_bytes,
            crypto: None,
        }
    }
}

pub struct Builder {
    cache_alignment: u64,
    cache_size_max: u64,
    crypto: Option<Arc<Mutex<XSalsa20>>>,
}

impl Builder {
    pub fn set_crypto(&mut self, crypto: XSalsa20) {
        self.crypto = Some(Arc::new(Mutex::new(crypto)));
    }

    pub fn build<P>(self, file: TokioFile, upstream: P) -> CacheWrapper<P>
    where
        P: PieceStorageEngineDumb + Clone + Send + Sync + 'static,
    {
        CacheWrapper {
            cache_alignment: self.cache_alignment,
            cache_file: Arc::new(Mutex::new(file)),
            crypto: self.crypto,
            piece_cache: Arc::new(Mutex::new(PieceCacheInfo {
                cache_size_max: self.cache_size_max,
                cache_file_offset: 0,
                cache_size_cur: 0,
                pieces: Default::default(),
            })),
            upstream,
        }
    }
}

#[derive(Debug)]
struct FileSpan {
    start: u64,
    length: u64,
}

async fn load_from_disk(
    file: Arc<Mutex<TokioFile>>,
    crypto: Option<Arc<Mutex<XSalsa20>>>,
    piece_length: u32,
    _piece_length_nopad: u32,
    file_position: u64,
) -> Result<Bytes, MagnetiteError> {
    let mut piece_data = vec![0; piece_length as usize];

    let mut file = file.lock().await;
    file.seek(SeekFrom::Start(file_position)).await?;
    file.read_exact(&mut piece_data).await?;
    drop(file);

    if let Some(cr) = crypto {
        let mut crlocked = cr.lock().await;
        crlocked.seek(file_position);
        crlocked.apply_keystream(&mut piece_data[..]);
    }

    Ok(Bytes::from(piece_data))
}

#[cfg(not(target_os = "linux"))]
fn cache_cleanup(_cache: &mut PieceCacheInfo, _adding: u64, _batch_size: u64) -> Vec<FileSpan> {
    Vec::new()
}

#[cfg(target_os = "linux")]
fn cache_cleanup(cache: &mut PieceCacheInfo, adding: u64, batch_size: u64) -> Vec<FileSpan> {
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;

    #[derive(Debug)]
    struct HeapEntry {
        last_touched: SystemTime,
        piece_length: u32,
        btree_key: (TorrentID, u32),
    }

    impl Eq for HeapEntry {}

    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> Ordering {
            self.last_touched.cmp(&other.last_touched)
        }
    }

    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl PartialEq for HeapEntry {
        fn eq(&self, other: &Self) -> bool {
            self.last_touched == other.last_touched
        }
    }

    let predicted_used_space = cache.cache_size_cur + adding;
    if predicted_used_space < cache.cache_size_max {
        return Vec::new();
    }

    let mut discard: BinaryHeap<HeapEntry> = BinaryHeap::new();
    let mut discard_credit = batch_size as i64;

    for (k, v) in cache.pieces.iter() {
        while let Some(v) = discard.peek() {
            if discard_credit < 0 {
                discard_credit += i64::from(v.piece_length);
                drop(discard.pop().unwrap());
            } else {
                break;
            }
        }

        if v.position.is_some() {
            discard_credit -= i64::from(v.piece_length);
            discard.push(HeapEntry {
                last_touched: v.last_touched,
                piece_length: v.piece_length,
                btree_key: *k,
            });
        }
    }

    let mut out = Vec::with_capacity(discard.len());

    for h in discard.into_vec() {
        let v = cache.pieces.remove(&h.btree_key).unwrap();

        if let Some(pos) = v.position {
            cache.cache_size_cur -= u64::from(v.piece_length);
            out.push(FileSpan {
                start: pos,
                length: u64::from(v.piece_length),
            });
        }
    }

    out
}

#[cfg(not(target_os = "linux"))]
fn punch_cache(_: &mut TokioFile, _: &[FileSpan]) -> Result<(), nix::Error> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn punch_cache(cache: &mut TokioFile, punch_spans: &[FileSpan]) -> Result<(), nix::Error> {
    let start = std::time::Instant::now();
    let mut byte_acc: u64 = 0;

    for punch in punch_spans {
        use std::os::unix::io::AsRawFd;

        byte_acc += punch.length;
        let flags = FallocateFlags::FALLOC_FL_PUNCH_HOLE | FallocateFlags::FALLOC_FL_KEEP_SIZE;
        fallocate(
            cache.as_raw_fd(),
            flags,
            punch.start as i64,
            punch.length as i64,
        )?;
    }

    if byte_acc > 0 {
        event!(
            Level::INFO,
            "punched {} bytes out with {} calls in {:?}",
            byte_acc,
            punch_spans.len(),
            start.elapsed(),
        );
    }

    Ok(())
}

impl<P> PieceStorageEngineDumb for CacheWrapper<P>
where
    P: PieceStorageEngineDumb + Clone + Send + Sync + 'static,
{
    fn get_piece_dumb(
        &self,
        req: &GetPieceRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>> {
        use std::time::Duration;

        let self_cloned: Self = self.clone();
        let piece_key = (req.content_key, req.piece_index);
        let req: GetPieceRequest = *req;

        async move {
            let mut piece_cache = self_cloned.piece_cache.lock().await;

            let cache_entry = piece_cache
                .pieces
                .entry(piece_key)
                .or_insert_with(|| PieceCacheEntry::new_with_piece_length(req.piece_length));

            cache_entry.last_touched = SystemTime::now();

            if let Some(ref infl) = cache_entry.inflight {
                let completion_fut = infl.clone().complete();
                drop(piece_cache);
                return completion_fut.await;
            }

            let cache_entry_cloned: PieceCacheEntry = cache_entry.clone();

            let (tx, rx) = watch::channel(None);
            let infl = Inflight { finished: rx };
            cache_entry.inflight = Some(infl.clone());

            drop(piece_cache);
            tokio::spawn(async move {
                let (_, piece_length_nopad) = super::utils::compute_offset_length(
                    req.piece_index,
                    req.piece_length,
                    req.total_length,
                );

                event!(
                    Level::DEBUG,
                    "loading request {:?}::{:?}",
                    piece_key,
                    cache_entry_cloned
                );

                // now we determine if we need to fetch it from upstream or from local disk-based cache.
                if let Some(pos) = cache_entry_cloned.position {
                    // it's on disk - load it from there and issue a completion.
                    let disk_load_res = load_from_disk(
                        self_cloned.cache_file.clone(),
                        self_cloned.crypto.clone(),
                        cache_entry_cloned.piece_length,
                        piece_length_nopad,
                        pos,
                    )
                    .await;

                    if let Ok(ref bytes) = disk_load_res {
                        event!(
                            Level::DEBUG,
                            "got piece from cache: len()={:?}",
                            bytes.len()
                        );
                    }

                    if let Err(ref err) = disk_load_res {
                        event!(
                            Level::ERROR,
                            "failed to load piece from disk cache: {}",
                            err
                        );
                    }

                    let _ = tx.broadcast(Some(disk_load_res));
                    drop(tx);

                    // schedule cleanup of inflight marker after a second
                    tokio::time::delay_for(Duration::new(1, 0)).await;

                    let mut piece_cache = self_cloned.piece_cache.lock().await;
                    if let Some(e) = piece_cache.pieces.get_mut(&piece_key) {
                        event!(Level::DEBUG, "clearing memory-cached piece");
                        e.inflight = None;
                    }
                    drop(piece_cache);
                    return;
                }

                event!(Level::DEBUG, "loading request from upstream");

                let mut piece_cache = self_cloned.piece_cache.lock().await;
                let punch_spans = cache_cleanup(
                    &mut *piece_cache,
                    cache_entry_cloned.piece_length as u64,
                    1024 * 1024 * 1024,
                );

                drop(piece_cache);

                let mut res = self_cloned.upstream.get_piece_dumb(&req).await;
                event!(
                    Level::DEBUG,
                    "loading request from upstream: {}",
                    if res.is_ok() { "ok" } else { "err" }
                );

                let mut file = self_cloned.cache_file.lock().await;

                if let Err(err) = punch_cache(&mut file, &punch_spans) {
                    event!(Level::ERROR, "failed to punch hole: {}", err);
                    let _ = tx.broadcast(Some(Err(MagnetiteError::StorageEngineCorruption)));
                    return;
                }

                let mut cache_position = None;
                if let Ok(ref bytes) = res {
                    let mut cache_padded = bytes.len() as u64;
                    if cache_padded % self_cloned.cache_alignment != 0 {
                        cache_padded %= self_cloned.cache_alignment;
                        cache_padded += self_cloned.cache_alignment;

                        event!(
                            Level::DEBUG,
                            "padded piece #{}: {} -> {}",
                            req.piece_index,
                            bytes.len(),
                            cache_padded,
                        );
                    }

                    let mut piece_padded = BytesMut::with_capacity(cache_padded as usize);
                    piece_padded.extend_from_slice(&bytes[..]);
                    piece_padded.resize(cache_padded as usize, 0);

                    let write_res = async {
                        let position = file.seek(SeekFrom::End(0)).await?;
                        assert_eq!(position % self_cloned.cache_alignment, 0);
                        cache_position = Some(position);

                        if let Some(ref cr) = self_cloned.crypto {
                            let mut crlocked = cr.lock().await;
                            crlocked.seek(position);
                            crlocked.apply_keystream(&mut piece_padded[..]);
                        }

                        file.write_all(&piece_padded[..]).await?;

                        Result::<(), MagnetiteError>::Ok(())
                    }
                    .await;

                    if let Err(err) = write_res {
                        res = Err(err);
                    }
                }

                drop(file);

                if let Err(ref err) = res {
                    event!(Level::ERROR, "failed to load piece from upstream: {}", err);
                }

                let _ = tx.broadcast(Some(res));
                drop(tx);

                let mut piece_cache = self_cloned.piece_cache.lock().await;
                if let Some(e) = piece_cache.pieces.get_mut(&piece_key) {
                    event!(
                        Level::DEBUG,
                        "setting local disk cache location to {:?}::{:?}",
                        piece_key,
                        cache_position
                    );
                    e.position = cache_position;
                    piece_cache.cache_size_cur += u64::from(e.piece_length);
                }

                drop(piece_cache);
                // schedule cleanup of inflight marker after a second
                tokio::time::delay_for(Duration::new(10, 0)).await;

                let mut piece_cache = self_cloned.piece_cache.lock().await;

                if let Some(e) = piece_cache.pieces.get_mut(&piece_key) {
                    event!(
                        Level::DEBUG,
                        "clearing memory-cached piece {:?}::{:?}",
                        piece_key,
                        e
                    );
                    e.inflight = None;
                }

                drop(piece_cache);
            });

            return infl.complete().await;
        }
        .boxed()
    }
}
