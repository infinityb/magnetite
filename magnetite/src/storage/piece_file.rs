use std::collections::HashMap;
use std::fmt;
use std::io::{Read, SeekFrom};
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;

use lru::LruCache;

use futures::future::FutureExt;
use salsa20::stream_cipher::{NewStreamCipher, SyncStreamCipher, SyncStreamCipherSeek};
use salsa20::XSalsa20;
use sha1::{Digest, Sha1};
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use bytes::BytesMut;
use tokio::sync::{Mutex, broadcast, oneshot};

use super::{PieceStorageEngine, PieceStorageEngineMut, WriteChunkResponse};
use crate::model::{
    BitField, MagnetiteError, ProtocolViolation, StorageEngineCorruption, TorrentID,
    TorrentMetaWrapped,
};
use crate::storage::CompletionEvent;

pub const DOWNLOAD_CHUNK_SIZE: u32 = 16 * 1024;

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

pub struct PieceFileStorageEngineLockables {
    piece_file: TokioFile,
    verify_piece: PieceFileStorageEngineVerifyState,
    crypto: Option<XSalsa20>,
    completion: BitField,
    in_progress: HashMap<u32, InProgress>,
}

pub struct InProgress {
    // for interior pieces:
    //   self.chunks.len() == piece_length / DOWNLOAD_CHUNK_SIZE
    // for the last piece, we only count existing chunks
    //   self.chunks.len() == ceil(float(total_length % piece_length) / DOWNLOAD_CHUNK_SIZE)
    pub chunks: BitField,
    pub completion: broadcast::Sender<Result<CompletionEvent, MagnetiteError>>,
}

impl fmt::Debug for PieceFileStorageEngineLockables {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PieceFileStorageEngineLockables")
            .field("piece_file", &self.piece_file)
            .finish()
    }
}

#[derive(Clone)]
pub struct PieceFileStorageEngine {
    info_hash: TorrentID,
    total_length: u64,
    piece_length: u64,
    lockables: Arc<Mutex<PieceFileStorageEngineLockables>>,
    piece_shas: Arc<[TorrentID]>,
}

pub struct PieceFileStorageEngineBuilder {
    info_hash: TorrentID,
    total_length: u64,
    piece_length: u64,
    piece_shas: Vec<TorrentID>,
    crypto: Option<XSalsa20>,
    completion_bitfield: Option<BitField>,
    verify_mode: PieceFileStorageEngineVerifyMode,
}

impl PieceFileStorageEngineBuilder {
    pub fn set_crypto(&mut self, s: XSalsa20) {
        self.crypto = Some(s);
    }

    pub fn set_completion_bitfield(&mut self, s: BitField) {
        self.completion_bitfield = Some(s);
    }

    pub fn set_complete(&mut self) {
        let bf_length = self.piece_shas.len() as u32;
        self.completion_bitfield = Some(BitField::all(bf_length));
    }

    pub fn set_piece_verification_mode(&mut self, vm: PieceFileStorageEngineVerifyMode) {
        self.verify_mode = vm;
    }

    pub fn build(self, piece_file: TokioFile) -> PieceFileStorageEngine {
        let bf_length = self.piece_shas.len() as u32;
        let lockables = PieceFileStorageEngineLockables {
            crypto: self.crypto,
            verify_piece: PieceFileStorageEngineVerifyState::new(self.verify_mode, bf_length),
            piece_file: piece_file,
            completion: BitField::all(bf_length),
            in_progress: Default::default(),
        };

        PieceFileStorageEngine {
            info_hash: self.info_hash,
            total_length: self.total_length,
            piece_length: self.piece_length,
            lockables: Arc::new(Mutex::new(lockables)),
            piece_shas: self.piece_shas.into(),
        }
    }
}

impl PieceFileStorageEngine {
    #[inline]
    pub fn get_piece_length(&self) -> u64 {
        self.piece_length
    }

    pub fn from_torrent_wrapped(wrapped: &TorrentMetaWrapped) -> PieceFileStorageEngineBuilder {
        PieceFileStorageEngineBuilder {
            info_hash: wrapped.info_hash,
            total_length: wrapped.total_length,
            piece_length: wrapped.meta.info.piece_length as u64,
            piece_shas: wrapped.piece_shas.clone().into(),
            crypto: None,
            completion_bitfield: None,
            verify_mode: PieceFileStorageEngineVerifyMode::First,
        }
    }
}

impl fmt::Debug for PieceFileStorageEngine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PieceFileStorageEngine")
            .field("total_length", &self.total_length)
            .field("piece_length", &self.piece_length)
            .finish()
    }
}

fn compute_offset(index: u32, atom_length: u64, total_length: u64) -> (u64, u64) {
    let index = u64::from(index);

    let offset_start = atom_length * index;
    let mut offset_end = atom_length * (index + 1);
    if total_length < offset_end {
        offset_end = total_length;
    }

    (offset_start, offset_end)
}

impl PieceStorageEngineMut for PieceFileStorageEngine {
    fn write_chunk(
        &self,
        content_key: &TorrentID,
        piece_id: u32,
        chunk_offset: u32,
        data: Bytes,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<WriteChunkResponse, MagnetiteError>> + Send>>
    {
        let info_hash = self.info_hash;
        let lockables = Arc::clone(&self.lockables);
        let piece_shas = Arc::clone(&self.piece_shas);
        let total_length = self.total_length;
        let piece_length = self.piece_length;
        async move {
            let mut piece_completed = false;
            let mut piece_failed_validation = false;

            if chunk_offset % DOWNLOAD_CHUNK_SIZE != 0 {
                return Err(ProtocolViolation.into());
            }
            let chunk_id = chunk_offset / DOWNLOAD_CHUNK_SIZE;

            let (piece_offset_start, piece_offset_end) =
                compute_offset(piece_id, piece_length, total_length);

            let chunk_count = {
                let true_piece_length = piece_offset_end - piece_offset_start;
                let mut chunk_count = true_piece_length / u64::from(DOWNLOAD_CHUNK_SIZE);
                if true_piece_length % u64::from(DOWNLOAD_CHUNK_SIZE) > 0 {
                    chunk_count += 1;
                }
                chunk_count as u32
            };

            let chunk_offset_start = piece_offset_start + u64::from(chunk_offset);
            let mut chunk_offset_end = chunk_offset_start + u64::from(DOWNLOAD_CHUNK_SIZE);
            if total_length < chunk_offset_end {
                chunk_offset_end = total_length
            }
            if piece_offset_end < chunk_offset_end {
                return Err(ProtocolViolation.into());
            }
            let expect_data_len = (chunk_offset_end - chunk_offset_start) as usize;
            if data.len() != expect_data_len {
                return Err(ProtocolViolation.into());
            }

            let mut locked = lockables.lock().await;
            locked
                .piece_file
                .seek(SeekFrom::Start(chunk_offset_start))
                .await?;

            let to_write: &[u8];
            let mut to_write_crypto_owned: BytesMut;
            if let Some(ref mut cr) = locked.crypto {
                to_write_crypto_owned = BytesMut::from(&data[..]);

                cr.seek(chunk_offset_start);
                cr.apply_keystream(&mut to_write_crypto_owned[..]);

                to_write = &to_write_crypto_owned[..];
            } else {
                to_write = &data[..];
            }

            locked.piece_file.write_all(to_write).await?;

            let (finish_tx, finish_rx) = broadcast::channel(1);
            let prog = locked
                .in_progress
                .entry(piece_id)
                .or_insert_with(|| InProgress {
                    chunks: BitField::none(chunk_count),
                    completion: finish_tx,
                });

            prog.chunks.set(chunk_id, true);

            if prog.chunks.is_filled() {
                drop(prog);

                let piece_sha: &TorrentID = piece_shas
                    .get(piece_id as usize)
                    .ok_or_else(|| ProtocolViolation)?;

                locked
                    .piece_file
                    .seek(SeekFrom::Start(piece_offset_start))
                    .await?;

                let mut chonker = vec![0; (piece_offset_end - piece_offset_start) as usize];
                locked.piece_file.read_exact(&mut chonker).await?;

                if let Some(ref mut cr) = locked.crypto {
                    cr.seek(piece_offset_start);
                    cr.apply_keystream(&mut chonker);
                }

                let mut hasher = Sha1::new();
                hasher.input(&chonker);
                let sha = hasher.result();

                // if we fail, the bitfield must be reset, so there's no point in keeping
                // the entry anymore.  We'll regenerate it if needed.  If we've succeeded,
                // then the piece is no longer in progress.
                let in_prog = locked.in_progress.remove(&piece_id).unwrap();
                let _ = in_prog.completion.send(Ok(CompletionEvent {
                    info_hash,
                    piece_id,
                }));

                if sha[..] == piece_sha.0[..] {
                    locked.completion.set(piece_id, true);
                    piece_completed = true;
                } else {
                    piece_failed_validation = true;
                }
            }

            drop(locked);

            Ok(WriteChunkResponse {
                piece_completed,
                piece_failed_validation,
                completion: finish_rx,
            })
        }
        .boxed()
    }
}

impl PieceStorageEngine for PieceFileStorageEngine {
    fn get_piece(
        &self,
        content_key: &TorrentID,
        piece_id: u32,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>> {
        let lockables = Arc::clone(&self.lockables);
        let piece_shas = Arc::clone(&self.piece_shas);
        let total_length = self.total_length;
        let piece_length = self.piece_length;

        async move {
            let piece_sha: &TorrentID = piece_shas
                .get(piece_id as usize)
                .ok_or_else(|| ProtocolViolation)?;

            let (piece_offset_start, piece_offset_end) =
                compute_offset(piece_id, piece_length, total_length);

            let mut locked = lockables.lock().await;

            let run_verify = match locked.verify_piece {
                PieceFileStorageEngineVerifyState::Never => false,
                PieceFileStorageEngineVerifyState::First { ref verified } => {
                    !verified.has(piece_id)
                }
                PieceFileStorageEngineVerifyState::Always => true,
            };

            locked
                .piece_file
                .seek(SeekFrom::Start(piece_offset_start))
                .await?;
            let mut chonker = vec![0; (piece_offset_end - piece_offset_start) as usize];
            locked.piece_file.read_exact(&mut chonker).await?;

            if let Some(ref mut cr) = locked.crypto {
                cr.seek(piece_offset_start);
                cr.apply_keystream(&mut chonker);
            }
            if run_verify {
                let mut hasher = Sha1::new();
                hasher.input(&chonker);
                let sha = hasher.result();
                if sha[..] != piece_sha.0[..] {
                    return Err(StorageEngineCorruption.into());
                }

                if let PieceFileStorageEngineVerifyState::First { ref mut verified } =
                    locked.verify_piece
                {
                    verified.set(piece_id, true);
                }
            }

            Ok(Bytes::from(chonker))
        }
        .boxed()
    }
}
