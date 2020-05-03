use std::convert::TryInto;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::FutureExt;
use lru::LruCache;
use metrics::{counter, gauge};
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

#[derive(Debug)]
struct MemoryPieceCacheInfo {
    cache_size_max: u64,
    cache_size_cur: u64,
    fetched_bytes: u64,
    fetched_upstream_bytes: u64,
    next_cache_report_print: Instant,
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
                fetched_bytes: 0,
                fetched_upstream_bytes: 0,
                next_cache_report_print: Instant::now() + Duration::new(60, 0),
                // we control the eviction manually by bytes instead of items.
                pieces: LruCache::unbounded(),
            })),
            upstream,
        }
    }
}

fn cache_cleanup(cache: &mut MemoryPieceCacheInfo, adding: u64, batch_size: u64) {
    event!(
        Level::TRACE,
        "cache_cleanup(cache={:?}, adding={}, batch_size={}) called",
        cache,
        adding,
        batch_size
    );
    let cache_next = cache.cache_size_cur + adding;
    if cache_next < cache.cache_size_max {
        return;
    }

    let mut must_pop = std::cmp::max(batch_size, cache_next - cache.cache_size_max);

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

        counter!("mem_cache.piece_requests", 1, "torrent" => req.content_key.hex().to_string());

        async move {
            let mut piece_cache = self_cloned.piece_cache.lock().await;
            cache_cleanup(&mut piece_cache, req.piece_length as u64, 0);
            gauge!("mem_cache.cached_bytes", piece_cache.cache_size_cur as i64,);

            let now = Instant::now();
            if piece_cache.next_cache_report_print < now {
                piece_cache.next_cache_report_print = now + Duration::new(60, 0);
            }

            if let Some(v) = piece_cache.pieces.get(&piece_key) {
                let completion_fut = v.inflight.clone().complete();
                drop(piece_cache);

                let res = completion_fut.await;
                if let Ok(ref bytes) = res {
                    let mut piece_cache = self_cloned.piece_cache.lock().await;
                    piece_cache.fetched_bytes += bytes.len() as u64;
                    gauge!(
                        "mem_cache.served_bytes",
                        piece_cache.fetched_bytes.try_into().unwrap()
                    );
                    counter!("mem_cache.cache_hit", 1);
                }
                return res;
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

            let self_cloned2 = self_cloned.clone();
            tokio::spawn(async move {
                let res = self_cloned2.upstream.get_piece_dumb(&req).await;

                if let Ok(ref bytes) = res {
                    let mut piece_cache = self_cloned2.piece_cache.lock().await;
                    piece_cache.cache_size_cur += bytes.len() as u64;
                    piece_cache.fetched_bytes += bytes.len() as u64;
                    piece_cache.fetched_upstream_bytes += bytes.len() as u64;
                    gauge!(
                        "mem_cache.served_bytes",
                        piece_cache.fetched_bytes.try_into().unwrap()
                    );
                    gauge!(
                        "mem_cache.fetched_bytes",
                        piece_cache.fetched_upstream_bytes.try_into().unwrap()
                    );
                    counter!("mem_cache.cache_miss", 1);
                }

                let _ = tx.broadcast(Some(res));
                drop(tx);
            });

            let completion_fut = Inflight {
                finished: rx.clone(),
            }
            .complete();
            return completion_fut.await;
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryCacheWrapper;
    use crate::storage::test_utils::MockPieceStorageEngineDumb;

    #[test]
    fn eviction_test() {
        use crate::model::TorrentID;
        use crate::storage::{GetPieceRequest, PieceStorageEngineDumb};
        use tokio::runtime::Runtime;

        // use tracing_subscriber::filter::LevelFilter;
        // use tracing_subscriber::FmtSubscriber;
        // tracing::subscriber::set_global_default(
        //     FmtSubscriber::builder()
        //         .with_max_level(LevelFilter::TRACE)
        //         .finish(),
        // )
        // .unwrap();

        let cache_builder = MemoryCacheWrapper::build_with_capacity_bytes(256 * 1024); // 16 cacheable
        let mock = MockPieceStorageEngineDumb::new();
        let storage_engine = cache_builder.build(mock.clone());

        let mut rt = Runtime::new().unwrap();

        for i in 0..17 {
            let res = rt.block_on(storage_engine.get_piece_dumb(&GetPieceRequest {
                content_key: TorrentID::zero(),
                piece_sha: TorrentID::zero(),
                piece_length: 16 * 1024,
                total_length: 0,
                piece_index: i,
            }));

            assert!(res.is_ok());
        }

        // try index 0 again, should hit mock
        let res = rt.block_on(storage_engine.get_piece_dumb(&GetPieceRequest {
            content_key: TorrentID::zero(),
            piece_sha: TorrentID::zero(),
            piece_length: 16 * 1024,
            total_length: 0,
            piece_index: 0,
        }));

        assert!(res.is_ok());

        assert_eq!(18, *mock.request_counts.lock().unwrap());
    }
}
