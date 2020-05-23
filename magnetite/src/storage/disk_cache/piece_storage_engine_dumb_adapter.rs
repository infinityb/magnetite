use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::FutureExt;
use salsa20::XSalsa20;
use tokio::fs::File as TokioFile;
use tokio::sync::Mutex;

use super::{DiskCacheState, PieceCacheEntry, PieceCacheInfo};
use crate::model::MagnetiteError;
use crate::storage::{GetPieceRequest, PieceStorageEngineDumb};

pub struct Builder {
    cache_alignment: u64,
    cache_size_max: u64,
    crypto: Option<Arc<Mutex<XSalsa20>>>,
}

#[derive(Clone)]
pub struct DiskCacheWrapper<P> {
    state: DiskCacheState,
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

impl Builder {
    pub fn set_crypto(&mut self, crypto: XSalsa20) {
        self.crypto = Some(Arc::new(Mutex::new(crypto)));
    }

    pub fn build<P>(self, file: TokioFile, upstream: P) -> DiskCacheWrapper<P>
    where
        P: PieceStorageEngineDumb + Clone + Send + Sync + 'static,
    {
        DiskCacheWrapper {
            state: DiskCacheState {
                cache_alignment: self.cache_alignment,
                cache_file: Arc::new(Mutex::new(file)),
                crypto: self.crypto,
                piece_cache: Arc::new(Mutex::new(PieceCacheInfo {
                    cache_size_max: self.cache_size_max,
                    cache_file_offset: 0,
                    cache_size_cur: 0,
                    pieces: Default::default(),
                })),
            },
            upstream,
        }
    }
}

impl<P> PieceStorageEngineDumb for DiskCacheWrapper<P>
where
    P: PieceStorageEngineDumb + Clone + Send + Sync + 'static,
{
    fn get_piece_dumb(
        &self,
        req: &GetPieceRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send + 'static>>
    {
        let self_cloned: Self = self.clone();
        let piece_key = (req.content_key, req.piece_index);
        let req: GetPieceRequest = *req;

        async move {
            let (_, piece_length_actual) = crate::storage::utils::compute_offset_length(
                req.piece_index,
                req.piece_length,
                req.total_length,
            );

            let mut should_inject = false;
            let piece_bytes = match self_cloned.state.fetch_piece(&req).await {
                Ok(v) => v,
                Err(err) => {
                    if err.is_cache_miss() {
                        should_inject = true;
                        self_cloned.upstream.get_piece_dumb(&req).await?
                    } else {
                        return Err(err);
                    }
                }
            };
            if should_inject {
                self_cloned
                    .state
                    .inject_piece(&req, piece_bytes.clone())
                    .await?;
            }

            Ok(piece_bytes)
        }
        .boxed()
    }
}
