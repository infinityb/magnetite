use std::collections::BTreeMap;
use std::convert::TryInto;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use bytes::{Bytes, BytesMut};
use futures::future::FutureExt;
use metrics::{counter, gauge, timing};
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

#[cfg(target_os = "linux")]
pub mod cache_cleanup_linux;
#[cfg(target_os = "linux")]
pub use self::cache_cleanup_linux as cache_cleanup;

#[cfg(not(target_os = "linux"))]
pub mod cache_cleanup_stub;
#[cfg(not(target_os = "linux"))]
pub use self::cache_cleanup_stub as cache_cleanup;

use magnetite_common::TorrentId;

use super::CacheMiss;
use crate::model::{MagnetiteError, get_torrent_salsa};
use crate::storage::GetPieceRequest;

mod piece_storage_engine_dumb_adapter;

pub use self::piece_storage_engine_dumb_adapter::DiskCacheWrapper;

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
    pieces: BTreeMap<(TorrentId, u32), PieceCacheEntry>,
}

#[derive(Clone)]
pub struct DiskCacheState {
    // pieces and punched holes will always start at a multiple of this alignment
    // pieces will be padded to their piece_length, if shorter.
    cache_alignment: u64,

    piece_cache: Arc<Mutex<PieceCacheInfo>>,
    cache_file: Arc<Mutex<TokioFile>>,
    crypto: Option<Arc<Mutex<XSalsa20>>>,
}

fn update_logging_and_metrics(res: &Result<Bytes, MagnetiteError>) {
    match res {
        Ok(ref bytes) => {
            counter!("diskcache.cache_read_bytes", bytes.len() as u64);
            counter!("diskcache.cache_hit_count", 1);

            event!(Level::DEBUG, "got piece from disk cache: len()={:?}", bytes.len());
        },
        Err(ref err) => {
            counter!("diskcache.read_error", 1);
            event!(Level::ERROR, "failed to load piece from disk cache: {}", err);
        }
    }
}

const PUNCH_SIZE: u64 = 128 * 1024 * 1024;

impl DiskCacheState {
    fn inject_piece(&self, req: &GetPieceRequest, bytes: Bytes) -> Pin<Box<dyn std::future::Future<Output = Result<(), MagnetiteError>> + Send>> {
        let self_cloned: Self = self.clone();
        let piece_key = (req.content_key, req.piece_index);
        let req: GetPieceRequest = *req;
        let (_, piece_length_actual) = super::utils::compute_offset_length(
            req.piece_index,
            req.piece_length,
            req.total_length,
        );

        async move {
            let cache_inject_start = Instant::now();
            let mut piece_cache = self_cloned.piece_cache.lock().await;
            let punch_spans = cache_cleanup::cache_cleanup(&mut *piece_cache, piece_length_actual.into(), PUNCH_SIZE);
            gauge!("diskcache.cached_bytes", piece_cache.cache_size_cur as i64);
            drop(piece_cache);

            if !punch_spans.is_empty() {
                #[cfg(target_os = "linux")]
                async {
                    use std::os::unix::io::AsRawFd;
                    let file = self_cloned.cache_file.lock().await;
                    let fallocate_fd = OwnedFd::dup(file.as_raw_fd()).unwrap();
                    drop(file);

                    if let Err(err) = cache_cleanup::punch_cache(fallocate_fd, punch_spans).await {
                        event!(Level::ERROR, "failed to punch hole: {}", err);
                        return;
                    }
                }
                .await;
            }

            let mut file = self_cloned.cache_file.lock().await;
            let position = file.seek(SeekFrom::End(0)).await?;
            assert_eq!(position % self_cloned.cache_alignment, 0);

            let mut cache_padded = bytes.len() as u64;
            if cache_padded % self_cloned.cache_alignment != 0 {
                cache_padded -= cache_padded % self_cloned.cache_alignment;
                cache_padded += self_cloned.cache_alignment;
            }

            let mut piece_padded = BytesMut::with_capacity(cache_padded as usize);
            piece_padded.extend_from_slice(&bytes[..]);
            piece_padded.resize(cache_padded as usize, 0);

            if let Some(ref cr) = self_cloned.crypto {
                let mut crlocked = cr.lock().await;
                crlocked.seek(position);
                crlocked.apply_keystream(&mut piece_padded[..]);
            }

            assert!(piece_padded.len() as u64 % self_cloned.cache_alignment == 0);

            let write_start = Instant::now();            
            file.write_all(&piece_padded[..]).await?;
            timing!("diskcache.cache_write_latency", write_start.elapsed());
            counter!("diskcache.cache_write_bytes", piece_padded.len() as u64);

            let ending_position = file.seek(SeekFrom::End(0)).await?;
            assert_eq!(ending_position % self_cloned.cache_alignment, 0);
            drop(file);

            event!(
                Level::TRACE,
                "setting local disk cache location to {:?}::{:?}",
                piece_key,
                position
            );

            let piece_length: u64 = bytes.len().try_into().unwrap();
            let mut piece_cache = self_cloned.piece_cache.lock().await;
            piece_cache.pieces.insert(
                piece_key,
                PieceCacheEntry {
                    last_touched: SystemTime::now(),
                    piece_length: req.piece_length,
                    position,
                },
            );

            piece_cache.cache_size_cur += piece_length;
            gauge!("diskcache.cached_bytes", piece_cache.cache_size_cur as i64);
            counter!("diskcache.cache_miss", 1);
            timing!("diskcache.cache_inject_latency", cache_inject_start.elapsed());

            drop(piece_cache);

            Result::<(), MagnetiteError>::Ok(())
        }.boxed()
    }

    fn fetch_piece(&self, req: &GetPieceRequest) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>> {
        let self_cloned: Self = self.clone();
        let piece_key = (req.content_key, req.piece_index);
        let req: GetPieceRequest = *req;
        let (_, piece_length_actual) = super::utils::compute_offset_length(
            req.piece_index,
            req.piece_length,
            req.total_length,
        );

        counter!("diskcache.piece_requests", 1, "torrent" => req.content_key.hex().to_string());

        async move {
            let mut piece_cache = self_cloned.piece_cache.lock().await;
            let cache_entry = piece_cache.pieces.get_mut(&piece_key)
                .ok_or_else(|| {
                    counter!("diskcache.cache_miss", 1);
                    CacheMiss
                })?;

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

            update_logging_and_metrics(&disk_load_res);

            return disk_load_res;
        }.boxed()
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



