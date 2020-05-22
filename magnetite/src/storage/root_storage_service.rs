use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use salsa20::stream_cipher::{SyncStreamCipher, SyncStreamCipherSeek};
use salsa20::XSalsa20;
use tokio::fs::File as TokioFile;
use tokio::sync::Mutex;
use tokio::sync::{mpsc, oneshot};
use tracing::{event, Level};

use magnetite_common::TorrentId;

use super::{
    utils::piece_file_pread_exact, AddTorrentRequest, GetPieceRequest, GetPieceRequestResolver,
    PieceStorageEngineDumb, TorrentDataSource, OpenFileCache,
};
use crate::model::{CompletionLost, MagnetiteError, ProtocolViolation};
use crate::utils::close_waiter::Done;

#[derive(Clone)]
pub struct ServiceState {
    lockable: Arc<Mutex<ServiceStateLocked>>,
}

struct ServiceStateLocked {
    torrents: BTreeMap<TorrentId, TorrentState>,
}

#[derive(Clone)]
struct TorrentState {
    crypto: Option<Arc<Mutex<XSalsa20>>>,
    piece_file: Arc<Mutex<TokioFile>>,
}

#[derive(Clone)]
pub struct PieceFileStorageHandle {
    tx: mpsc::Sender<GetPieceRequestResolver>,
}

impl PieceStorageEngineDumb for PieceFileStorageHandle {
    fn get_piece_dumb(
        &self,
        req: &GetPieceRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>> {
        let (resolver, response) = oneshot::channel();
        let req = GetPieceRequestResolver {
            request: req.clone(),
            resolver,
        };

        let mut tx = self.tx.clone();
        async move {
            tx.send(req).await.map_err(|_| CompletionLost)?;
            match response.await {
                Ok(v) => v,
                Err(..) => Err(CompletionLost.into()),
            }
        }
        .boxed()
    }
}

async fn piece_fetch_loop(
    init_sig: mpsc::Sender<()>,
    term_sig: Done,
) {
    let files = OpenFileCache::new(64);

    // let sema = Arc::new(tokio::sync::Semaphore::new(10));
    // let mut stop_signal_encountered = false;
    // loop {
    //     let req;
    //     tokio::select! {
    //         _ = Pin::new(&mut term_sig), if !stop_signal_encountered => {
    //             rx.close();
    //             stop_signal_encountered = true;
    //             continue;
    //         }
    //         req_opt = rx.next() => {
    //             req = match req_opt {
    //                 Some(v) => v,
    //                 None => return,
    //             };
    //         }
    //     };

    //     let self_cloned = s.clone();
    //     let sema_acquired = sema.clone().acquire_owned();
    //     let GetPieceRequestResolver {
    //         request: req,
    //         resolver,
    //     } = req;

    //     tokio::spawn(async move {
    //         let resolve_res = resolver.send(
    //             async move {
    //                 let ts: TorrentState = {
    //                     let locked = self_cloned.lockable.lock().await;

    //                     locked
    //                         .torrents
    //                         .get(&req.content_key)
    //                         .ok_or_else(|| ProtocolViolation)?
    //                         .clone()
    //                 };

    //                 let (piece_offset_start, piece_offset_end) = super::utils::compute_offset(
    //                     req.piece_index,
    //                     req.piece_length,
    //                     req.total_length,
    //                 );

    //                 let mut chonker = vec![0; (piece_offset_end - piece_offset_start) as usize];
    //                 piece_file_pread_exact(&*ts.piece_file, piece_offset_start, &mut chonker[..])
    //                     .await?;

    //                 if let Some(ref crypto) = ts.crypto {
    //                     let mut cr = crypto.lock().await;
    //                     cr.seek(piece_offset_start);
    //                     cr.apply_keystream(&mut chonker);
    //                 }

    //                 Ok(Bytes::from(chonker))
    //             }
    //             .await,
    //         );

    //         if resolve_res.is_err() {
    //             event!(Level::WARN, "dropped a response");
    //         }

    //         drop(sema_acquired);
    //     });
    // }
}

pub fn start_piece_storage_engine(init_sig: mpsc::Sender<()>, term_sig: Done) {
    tokio::task::spawn(piece_fetch_loop(init_sig, term_sig));
}
