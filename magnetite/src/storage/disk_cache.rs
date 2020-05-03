use std::collections::BTreeMap;
use std::convert::TryInto;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::{Bytes, BytesMut};
use futures::future::FutureExt;
use metrics::{counter, gauge};
use salsa20::stream_cipher::{SyncStreamCipher, SyncStreamCipherSeek};
use salsa20::XSalsa20;
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::{event, Level};

#[cfg(target_os = "linux")]
use crate::utils::OwnedFd;
#[cfg(target_os = "linux")]
use nix::fcntl::{fallocate, FallocateFlags};

use crate::model::{MagnetiteError, TorrentID};
use crate::storage::{GetPieceRequest, PieceStorageEngineDumb};

#[derive(Clone, Debug)]
struct PieceCacheEntry {
    last_touched: SystemTime,
    piece_length: u32,
    position: u64,
}

struct PieceCacheInfo {
    cache_size_max: u64,
    cache_file_offset: u64,
    cache_size_cur: u64,
    served_bytes: u64,
    fetched_upstream_bytes: u64,
    next_cache_report_print: Instant,
    pieces: BTreeMap<(TorrentID, u32), PieceCacheEntry>,
}

#[derive(Clone)]
pub struct DiskCacheWrapper<P> {
    // pieces and punched holes will always start at a multiple of this alignment
    // pieces will be padded to their piece_length, if shorter.
    cache_alignment: u64,

    piece_cache: Arc<Mutex<PieceCacheInfo>>,
    cache_file: Arc<Mutex<TokioFile>>,
    crypto: Option<Arc<Mutex<XSalsa20>>>,

    upstream: P,
}

impl DiskCacheWrapper<()> {
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

    pub fn build<P>(self, file: TokioFile, upstream: P) -> DiskCacheWrapper<P>
    where
        P: PieceStorageEngineDumb + Clone + Send + Sync + 'static,
    {
        DiskCacheWrapper {
            cache_alignment: self.cache_alignment,
            cache_file: Arc::new(Mutex::new(file)),
            crypto: self.crypto,
            piece_cache: Arc::new(Mutex::new(PieceCacheInfo {
                cache_size_max: self.cache_size_max,
                cache_file_offset: 0,
                cache_size_cur: 0,
                served_bytes: 0,
                fetched_upstream_bytes: 0,
                next_cache_report_print: Instant::now() + Duration::new(60, 0),
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
    piece_length_nopad: u32,
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

    piece_data.truncate(piece_length_nopad as usize);

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

        discard_credit -= i64::from(v.piece_length);
        discard.push(HeapEntry {
            last_touched: v.last_touched,
            piece_length: v.piece_length,
            btree_key: *k,
        });
    }

    let mut out = Vec::with_capacity(discard.len());

    for h in discard.into_vec() {
        let v = cache.pieces.remove(&h.btree_key).unwrap();

        cache.cache_size_cur -= u64::from(v.piece_length);
        out.push(FileSpan {
            start: v.position,
            length: u64::from(v.piece_length),
        });
    }

    out
}

#[cfg(target_os = "linux")]
async fn punch_cache(cache: OwnedFd, punch_spans: Vec<FileSpan>) -> Result<(), nix::Error> {
    use std::os::unix::io::AsRawFd;

    use metrics::timing;

    let start = std::time::Instant::now();
    let mut sched_yield_time = Duration::new(0, 0);
    let mut byte_acc: u64 = 0;

    for punch in &punch_spans {
        let yield_start = std::time::Instant::now();
        tokio::task::yield_now().await;
        sched_yield_time += yield_start.elapsed();

        byte_acc += punch.length;
        let punch_start = std::time::Instant::now();
        let flags = FallocateFlags::FALLOC_FL_PUNCH_HOLE | FallocateFlags::FALLOC_FL_KEEP_SIZE;
        fallocate(
            cache.as_raw_fd(),
            flags,
            punch.start as i64,
            punch.length as i64,
        )?;

        timing!("diskcache.punch_time", punch_start.elapsed());
    }

    counter!(
        "diskcache.punch_span_count",
        punch_spans.len().try_into().unwrap()
    );
    counter!("diskcache.punch_bytes", byte_acc);

    if byte_acc > 0 {
        event!(
            Level::INFO,
            "punched {} bytes out with {} calls in {:?}, {:?} yielded",
            byte_acc,
            punch_spans.len(),
            start.elapsed(),
            sched_yield_time,
        );
    }

    Ok(())
}

impl<P> PieceStorageEngineDumb for DiskCacheWrapper<P>
where
    P: PieceStorageEngineDumb + Clone + Send + Sync + 'static,
{
    fn get_piece_dumb(
        &self,
        req: &GetPieceRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>> {
        let self_cloned: Self = self.clone();
        let piece_key = (req.content_key, req.piece_index);
        let req: GetPieceRequest = *req;

        counter!("diskcache.piece_requests", 1, "torrent" => req.content_key.hex().to_string());

        async move {
            let (_, piece_length_actual) = super::utils::compute_offset_length(
                req.piece_index,
                req.piece_length,
                req.total_length,
            );

            let mut piece_cache = self_cloned.piece_cache.lock().await;
            if let Some(cache_entry) = piece_cache.pieces.get_mut(&piece_key) {
                cache_entry.last_touched = SystemTime::now();
                let cache_entry_cloned = cache_entry.clone();
                drop(piece_cache);

                let disk_load_res = load_from_disk(
                    self_cloned.cache_file.clone(),
                    self_cloned.crypto.clone(),
                    cache_entry_cloned.piece_length,
                    piece_length_actual,
                    cache_entry_cloned.position,
                )
                .await;

                if let Ok(ref bytes) = disk_load_res {
                    event!(
                        Level::DEBUG,
                        "got piece from disk cache: len()={:?}",
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

                if let Ok(ref bytes) = disk_load_res {
                    let mut piece_cache = self_cloned.piece_cache.lock().await;
                    piece_cache.served_bytes += bytes.len() as u64;
                    gauge!(
                        "diskcache.served_bytes",
                        piece_cache.served_bytes.try_into().unwrap()
                    );
                    counter!("diskcache.cache_hit", 1);
                }

                return disk_load_res;
            }

            const PUNCH_SIZE: u64 = 128 * 1024 * 1024;
            let punch_spans =
                cache_cleanup(&mut *piece_cache, piece_length_actual.into(), PUNCH_SIZE);
            gauge!("diskcache.cached_bytes", piece_cache.cache_size_cur as i64,);
            drop(piece_cache);

            if !punch_spans.is_empty() {
                #[cfg(target_os = "linux")]
                async {
                    use std::os::unix::io::AsRawFd;
                    let file = self_cloned.cache_file.lock().await;
                    let fallocate_fd = OwnedFd::dup(file.as_raw_fd()).unwrap();
                    drop(file);

                    if let Err(err) = punch_cache(fallocate_fd, punch_spans).await {
                        event!(Level::ERROR, "failed to punch hole: {}", err);
                        return;
                    }
                }
                .await;
            }

            let mut res = self_cloned.upstream.get_piece_dumb(&req).await;
            if let Ok(ref bytes) = res {
                let mut cache_padded = bytes.len() as u64;

                if cache_padded % self_cloned.cache_alignment != 0 {
                    cache_padded -= cache_padded % self_cloned.cache_alignment;
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
                    let mut file = self_cloned.cache_file.lock().await;
                    let position = file.seek(SeekFrom::End(0)).await?;
                    assert_eq!(position % self_cloned.cache_alignment, 0);

                    if let Some(ref cr) = self_cloned.crypto {
                        let mut crlocked = cr.lock().await;
                        crlocked.seek(position);
                        crlocked.apply_keystream(&mut piece_padded[..]);
                    }

                    assert!(piece_padded.len() as u64 % self_cloned.cache_alignment == 0);
                    file.write_all(&piece_padded[..]).await?;
                    let ending_position = file.seek(SeekFrom::End(0)).await?;
                    assert_eq!(ending_position % self_cloned.cache_alignment, 0);
                    drop(file);

                    event!(
                        Level::TRACE,
                        "setting local disk cache location to {:?}::{:?}",
                        piece_key,
                        position
                    );

                    let mut piece_cache = self_cloned.piece_cache.lock().await;

                    let piece_length: u64 = bytes.len().try_into().unwrap();
                    piece_cache.pieces.insert(
                        piece_key,
                        PieceCacheEntry {
                            last_touched: SystemTime::now(),
                            piece_length: req.piece_length,
                            position,
                        },
                    );

                    piece_cache.cache_size_cur += piece_length;
                    piece_cache.served_bytes += piece_length;
                    piece_cache.fetched_upstream_bytes += piece_length;

                    gauge!("diskcache.cached_bytes", piece_cache.cache_size_cur as i64,);
                    gauge!(
                        "diskcache.served_bytes",
                        piece_cache.served_bytes.try_into().unwrap()
                    );
                    gauge!(
                        "diskcache.fetched_upstream_bytes",
                        piece_cache.fetched_upstream_bytes.try_into().unwrap()
                    );
                    counter!("diskcache.cache_miss", 1);

                    drop(piece_cache);

                    Result::<(), MagnetiteError>::Ok(())
                }
                .await;

                if let Err(err) = write_res {
                    res = Err(err);
                }
            }
            if let Err(ref err) = res {
                event!(Level::ERROR, "failed to load piece from upstream: {}", err);
            }

            return res;
        }
        .boxed()
    }
}
