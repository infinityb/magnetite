use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::FutureExt;
use lru::LruCache;
use tokio::sync::{watch, Mutex};
use tracing::{event, Level};

use super::piggyback::Inflight;
use crate::model::{MagnetiteError, TorrentID};
use crate::storage::{GetPieceRequest, PieceStorageEngineDumb};

#[derive(Clone)]
struct PieceCacheEntry {
    inflight: Inflight,
}

impl fmt::Debug for PieceCacheEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PieceCacheEntry").finish()
    }
}

struct MemoryPieceCacheInfo {
    cache_size_max: u64,
    cache_size_cur: u64,
    pieces: LruCache<(TorrentID, u32), PieceCacheEntry>,
}

#[derive(Clone)]
pub struct MemoryCacheWrapper<P> {
    piece_cache: Arc<Mutex<MemoryPieceCacheInfo>>,
    upstream: P,
}

impl MemoryCacheWrapper<()> {
    pub fn build_with_capacity_bytes(cap_bytes: u64) -> Builder {
        Builder {
            cache_size_max: cap_bytes,
        }
    }
}

pub struct Builder {
    cache_size_max: u64,
}

impl Builder {
    pub fn build<P>(self, upstream: P) -> MemoryCacheWrapper<P>
    where
        P: PieceStorageEngineDumb + Clone + Send + Sync + 'static,
    {
        MemoryCacheWrapper {
            piece_cache: Arc::new(Mutex::new(MemoryPieceCacheInfo {
                cache_size_max: self.cache_size_max,
                cache_size_cur: 0,
                // we control the eviction manually by bytes instead of items.
                pieces: LruCache::unbounded(),
            })),
            upstream,
        }
    }
}

fn cache_cleanup(cache: &mut MemoryPieceCacheInfo, adding: u64, batch_size: u64) {
    let cache_next = cache.cache_size_cur + adding;
    if cache_next < cache.cache_size_max {
        return;
    }

    let mut must_pop = batch_size;

    while 0 < must_pop {
        if let Some((k, v)) = cache.pieces.pop_lru() {
            let mut freeing = 0;

            let borrowed = v.inflight.finished.borrow();
            if let Some(Ok(bytes)) = &*borrowed {
                freeing = bytes.len() as u64;
            }
            drop(borrowed);

            must_pop = must_pop.saturating_sub(freeing);
            event!(
                Level::DEBUG,
                "evicted {:?} from memory cache, freeing {}",
                k,
                freeing
            );
        } else {
            break;
        }
    }
}

impl<P> PieceStorageEngineDumb for MemoryCacheWrapper<P>
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

        async move {
            let mut piece_cache = self_cloned.piece_cache.lock().await;

            if let Some(v) = piece_cache.pieces.get(&piece_key) {
                let completion_fut = v.inflight.clone().complete();
                drop(piece_cache);
                return completion_fut.await;
            }

            let (tx, rx) = watch::channel(None);
            piece_cache.pieces.put(
                piece_key,
                PieceCacheEntry {
                    inflight: Inflight {
                        finished: rx.clone(),
                    },
                },
            );
            drop(piece_cache);

            tokio::spawn(async move {
                let res = self_cloned.upstream.get_piece_dumb(&req).await;

                if let Ok(ref bytes) = res {
                    let mut piece_cache = self_cloned.piece_cache.lock().await;
                    piece_cache.cache_size_cur += bytes.len() as u64;
                    drop(piece_cache);
                }

                let _ = tx.broadcast(Some(res));
                drop(tx);
            });

            return Inflight {
                finished: rx.clone(),
            }
            .complete()
            .await;
        }
        .boxed()
    }
}
