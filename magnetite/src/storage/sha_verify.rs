use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::FutureExt;
use sha1::{Digest, Sha1};
use tokio::sync::Mutex;
use tracing::{event, Level};

use magnetite_common::TorrentId;

use super::{GetPieceRequest, PieceStorageEngineDumb};
use crate::model::{BitField, MagnetiteError, StorageEngineCorruption};

#[derive(Debug)]
pub enum ShaVerifyState {
    Never,
    First { verified: BitField },
    Always,
}

#[derive(Copy, Clone, Debug)]
pub enum ShaVerifyMode {
    Never,
    First,
    Always,
}

#[derive(Clone, Debug)]
pub struct ShaVerify<S: PieceStorageEngineDumb> {
    upstream: S,
    mode: ShaVerifyMode,
    state: Arc<Mutex<BTreeMap<TorrentId, BitField>>>,
}

impl<S> ShaVerify<S>
where
    S: PieceStorageEngineDumb,
{
    pub fn new(upstream: S, mode: ShaVerifyMode) -> ShaVerify<S> {
        ShaVerify {
            upstream,
            mode,
            state: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

fn check_sha(req: &GetPieceRequest, data: &[u8]) -> Result<(), MagnetiteError> {
    let mut hasher = Sha1::new();
    hasher.input(data);
    let sha = hasher.result();

    if &sha[..] != req.piece_sha.as_bytes() {
        return Err(StorageEngineCorruption.into());
    }

    Ok(())
}

impl<S> PieceStorageEngineDumb for ShaVerify<S>
where
    S: PieceStorageEngineDumb + Clone + Send + Sync + 'static,
{
    fn get_piece_dumb(
        &self,
        req: &GetPieceRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>> {
        if let ShaVerifyMode::Never = self.mode {
            return self.upstream.get_piece_dumb(&req);
        }

        let self_cloned: Self = self.clone();
        let req: GetPieceRequest = *req;

        if let ShaVerifyMode::Always = self.mode {
            return async move {
                let piece = self_cloned.upstream.get_piece_dumb(&req).await?;
                if let Err(err) = check_sha(&req, &piece[..]) {
                    event!(
                        Level::ERROR,
                        "piece {} from {} failed sha check",
                        req.piece_index,
                        std::any::type_name::<S>()
                    );

                    return Err(err);
                }
                Ok(piece)
            }
            .boxed();
        }

        async move {
            // We could get into a state where we verify a piece twice since we don't record
            // that we have an inflight verification.
            let mut verify_pieces = self_cloned.state.lock().await;

            let state = verify_pieces.entry(req.content_key).or_insert_with(|| {
                let piece_length = req.piece_length as u64;
                let mut pieces = req.total_length / piece_length;
                if req.total_length % piece_length > 0 {
                    pieces += 1;
                }
                BitField::none(pieces as u32)
            });

            let run_verify = !state.has(req.piece_index);
            drop(verify_pieces);

            let piece = self_cloned.upstream.get_piece_dumb(&req).await?;

            if !run_verify {
                return Ok(piece);
            }

            if let Err(err) = check_sha(&req, &piece[..]) {
                event!(
                    Level::ERROR,
                    "piece {} from {} failed sha check",
                    req.piece_index,
                    std::any::type_name::<S>()
                );
                return Err(err);
            }

            let mut verify_pieces = self_cloned.state.lock().await;
            if let Some(verified) = verify_pieces.get_mut(&req.content_key) {
                verified.set(req.piece_index, true);
            }

            drop(verify_pieces);

            Ok(piece)
        }
        .boxed()
    }
}
