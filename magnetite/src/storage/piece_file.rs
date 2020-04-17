use std::collections::BTreeMap;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;

use futures::future::FutureExt;
use salsa20::stream_cipher::{SyncStreamCipher, SyncStreamCipherSeek};
use salsa20::XSalsa20;
use sha1::{Digest, Sha1};
use tokio::fs::File as TokioFile;
use tokio::io::AsyncReadExt;

use tokio::sync::{broadcast, Mutex};

use super::{GetPieceRequest, PieceStorageEngineDumb};
use crate::model::{
    BitField, MagnetiteError, ProtocolViolation, StorageEngineCorruption, TorrentID,
};
use crate::storage::CompletionEvent;

#[derive(Debug)]
pub enum PieceFileStorageEngineVerifyState {
    Never,
    First { verified: BitField },
    Always,
}

#[derive(Debug)]
pub enum PieceFileStorageEngineVerifyMode {
    Never,
    First,
    Always,
}

impl PieceFileStorageEngineVerifyState {
    pub fn new(mode: PieceFileStorageEngineVerifyMode, bf_len: u32) -> Self {
        match mode {
            PieceFileStorageEngineVerifyMode::Never => PieceFileStorageEngineVerifyState::Never,
            PieceFileStorageEngineVerifyMode::First => PieceFileStorageEngineVerifyState::First {
                verified: BitField::all(bf_len),
            },
            PieceFileStorageEngineVerifyMode::Always => PieceFileStorageEngineVerifyState::Always,
        }
    }
}

pub struct InProgress {
    // for interior pieces:
    //   self.chunks.len() == piece_length / DOWNLOAD_CHUNK_SIZE
    // for the last piece, we only count existing chunks
    //   self.chunks.len() == ceil(float(total_length % piece_length) / DOWNLOAD_CHUNK_SIZE)
    pub chunks: BitField,
    pub completion: broadcast::Sender<Result<CompletionEvent, MagnetiteError>>,
}

#[derive(Clone)]
struct TorrentState {
    verify_pieces: Arc<Mutex<PieceFileStorageEngineVerifyState>>,
    crypto: Option<Arc<Mutex<XSalsa20>>>,
    piece_file: Arc<Mutex<TokioFile>>,
}

#[derive(Clone)]
pub struct PieceFileStorageEngine {
    torrents: Arc<Mutex<BTreeMap<TorrentID, TorrentState>>>,
}

pub struct Builder {
    torrents: BTreeMap<TorrentID, TorrentState>,
}

pub struct Registration {
    pub piece_count: u32,
    pub verify_mode: PieceFileStorageEngineVerifyMode,
    pub crypto: Option<XSalsa20>,
    pub piece_file: TokioFile,
}

impl Builder {
    pub fn register_info_hash(&mut self, content_key: &TorrentID, reg: Registration) {
        let vstate = PieceFileStorageEngineVerifyState::new(reg.verify_mode, reg.piece_count);

        self.torrents.insert(
            *content_key,
            TorrentState {
                verify_pieces: Arc::new(Mutex::new(vstate)),
                crypto: reg.crypto.map(|x| Arc::new(Mutex::new(x))),
                piece_file: Arc::new(Mutex::new(reg.piece_file)),
            },
        );
    }

    pub fn build(self) -> PieceFileStorageEngine {
        PieceFileStorageEngine {
            torrents: Arc::new(Mutex::new(self.torrents)),
        }
    }
}

impl PieceFileStorageEngine {
    #[inline]
    pub fn builder() -> Builder {
        Builder {
            torrents: Default::default(),
        }
    }
}

// impl PieceStorageEngineMut for PieceFileStorageEngine {
//     fn write_chunk(
//         &self,
//         content_key: &TorrentID,
//         piece_id: u32,
//         chunk_offset: u32,
//         finalize_piece: bool,
//         data: Bytes,
//     ) -> Pin<Box<dyn std::future::Future<Output = Result<WriteChunkResponse, MagnetiteError>> + Send>>
//     {
//         let info_hash = self.info_hash;
//         let lockables = Arc::clone(&self.lockables);
//         let piece_shas = Arc::clone(&self.piece_shas);
//         let total_length = self.total_length;
//         let piece_length = self.piece_length;
//         async move {
//             let mut piece_completed = false;
//             let mut piece_failed_validation = false;

//             if chunk_offset % DOWNLOAD_CHUNK_SIZE != 0 {
//                 return Err(ProtocolViolation.into());
//             }
//             let chunk_id = chunk_offset / DOWNLOAD_CHUNK_SIZE;

//             let (piece_offset_start, piece_offset_end) =
//                 super::utils::compute_offset(piece_id, piece_length, total_length);

//             let chunk_count = {
//                 let true_piece_length = piece_offset_end - piece_offset_start;
//                 let mut chunk_count = true_piece_length / u64::from(DOWNLOAD_CHUNK_SIZE);
//                 if true_piece_length % u64::from(DOWNLOAD_CHUNK_SIZE) > 0 {
//                     chunk_count += 1;
//                 }
//                 chunk_count as u32
//             };

//             let chunk_offset_start = piece_offset_start + u64::from(chunk_offset);
//             let mut chunk_offset_end = chunk_offset_start + u64::from(DOWNLOAD_CHUNK_SIZE);
//             if total_length < chunk_offset_end {
//                 chunk_offset_end = total_length
//             }
//             if piece_offset_end < chunk_offset_end {
//                 return Err(ProtocolViolation.into());
//             }
//             let expect_data_len = (chunk_offset_end - chunk_offset_start) as usize;
//             if data.len() != expect_data_len {
//                 return Err(ProtocolViolation.into());
//             }

//             let mut locked = lockables.lock().await;
//             locked
//                 .piece_file
//                 .seek(SeekFrom::Start(chunk_offset_start))
//                 .await?;

//             let to_write: &[u8];
//             let mut to_write_crypto_owned: BytesMut;
//             if let Some(ref mut cr) = locked.crypto {
//                 to_write_crypto_owned = BytesMut::from(&data[..]);

//                 cr.seek(chunk_offset_start);
//                 cr.apply_keystream(&mut to_write_crypto_owned[..]);

//                 to_write = &to_write_crypto_owned[..];
//             } else {
//                 to_write = &data[..];
//             }

//             locked.piece_file.write_all(to_write).await?;

//             let (finish_tx, finish_rx) = broadcast::channel(1);
//             let prog = locked
//                 .in_progress
//                 .entry(piece_id)
//                 .or_insert_with(|| InProgress {
//                     chunks: BitField::none(chunk_count),
//                     completion: finish_tx,
//                 });

//             prog.chunks.set(chunk_id, true);

//             if prog.chunks.is_filled() {
//                 drop(prog);

//                 let piece_sha: &TorrentID = piece_shas
//                     .get(piece_id as usize)
//                     .ok_or_else(|| ProtocolViolation)?;

//                 locked
//                     .piece_file
//                     .seek(SeekFrom::Start(piece_offset_start))
//                     .await?;

//                 let mut chonker = vec![0; (piece_offset_end - piece_offset_start) as usize];
//                 locked.piece_file.read_exact(&mut chonker).await?;

//                 if let Some(ref mut cr) = locked.crypto {
//                     cr.seek(piece_offset_start);
//                     cr.apply_keystream(&mut chonker);
//                 }

//                 let mut hasher = Sha1::new();
//                 hasher.input(&chonker);
//                 let sha = hasher.result();

//                 // if we fail, the bitfield must be reset, so there's no point in keeping
//                 // the entry anymore.  We'll regenerate it if needed.  If we've succeeded,
//                 // then the piece is no longer in progress.
//                 let in_prog = locked.in_progress.remove(&piece_id).unwrap();
//                 let _ = in_prog.completion.send(Ok(CompletionEvent {
//                     info_hash,
//                     piece_id,
//                 }));

//                 if sha[..] == piece_sha.0[..] {
//                     locked.completion.set(piece_id, true);
//                     piece_completed = true;
//                 } else {
//                     piece_failed_validation = true;
//                 }
//             }

//             drop(locked);

//             Ok(WriteChunkResponse {
//                 piece_completed,
//                 piece_failed_validation,
//                 completion: finish_rx,
//             })
//         }
//         .boxed()
//     }
// }

impl PieceStorageEngineDumb for PieceFileStorageEngine {
    fn get_piece_dumb(
        &self,
        req: &GetPieceRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>> {
        let self_cloned: Self = self.clone();
        let req: GetPieceRequest = req.clone();

        async move {
            let torrents = self_cloned.torrents.lock().await;

            let ts: TorrentState = torrents
                .get(&req.content_key)
                .ok_or_else(|| ProtocolViolation)?
                .clone();

            drop(torrents);

            let (piece_offset_start, piece_offset_end) =
                super::utils::compute_offset(req.piece_index, req.piece_length, req.total_length);

            // We could get into a state where we verify a piece twice since we don't record
            // that we have an inflight verification.
            let mut verify_pieces = ts.verify_pieces.lock().await;
            let run_verify = match &*verify_pieces {
                PieceFileStorageEngineVerifyState::Never => false,
                PieceFileStorageEngineVerifyState::First { ref verified } => {
                    !verified.has(req.piece_index)
                }
                PieceFileStorageEngineVerifyState::Always => true,
            };
            drop(verify_pieces);

            let mut piece_file = ts.piece_file.lock().await;
            piece_file.seek(SeekFrom::Start(piece_offset_start)).await?;

            let mut chonker = vec![0; (piece_offset_end - piece_offset_start) as usize];
            piece_file.read_exact(&mut chonker).await?;

            drop(piece_file);

            if let Some(ref crypto) = ts.crypto {
                let mut cr = crypto.lock().await;
                cr.seek(piece_offset_start);
                cr.apply_keystream(&mut chonker);
            }

            if run_verify {
                let mut hasher = Sha1::new();
                hasher.input(&chonker);
                let sha = hasher.result();
                if sha[..] != req.piece_sha.0[..] {
                    return Err(StorageEngineCorruption.into());
                }

                let mut verify_pieces = ts.verify_pieces.lock().await;
                match &mut *verify_pieces {
                    PieceFileStorageEngineVerifyState::First { ref mut verified } => {
                        verified.set(req.piece_index, true);
                    }
                    _ => (),
                }
                drop(verify_pieces);
            }

            Ok(Bytes::from(chonker))
        }
        .boxed()
    }
}
