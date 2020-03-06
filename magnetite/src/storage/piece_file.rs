use std::collections::{hash_map, HashMap};
use std::fmt;
use std::fs::File;
use std::io::{Read, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::ops::Range;

use tracing::{event, span, Level};
use clap::{App, Arg, SubCommand};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use bytes::BytesMut;
use iresult::IResult;
use tokio::fs::File as TokioFile;
use salsa20::XSalsa20;
use salsa20::stream_cipher::generic_array::GenericArray;
use salsa20::stream_cipher::{NewStreamCipher, SyncStreamCipher, SyncStreamCipherSeek};
use bytes::Bytes;
use sha1::{Sha1, Digest};
use lru::LruCache;

use crate::model::{
    BitField,
    ProtocolViolation,
    TorrentID,
    StorageEngineCorruption,
};

#[derive(Debug)]
pub enum PieceFileStorageEngineVerifyMode {
    Never,
    First {
        verified: BitField,
    },
    Always,
}

pub struct PieceFileStorageEngineLockables {
    pub piece_file: TokioFile,
    pub verify_piece: PieceFileStorageEngineVerifyMode,
    pub piece_cache: LruCache<u32, Bytes>,
    pub crypto: Option<XSalsa20>,
}

#[derive(Clone)]
pub struct PieceFileStorageEngine {
    pub total_length: u64,
    pub piece_length: u64,
    pub lockables: Arc<Mutex<PieceFileStorageEngineLockables>>,
    pub piece_shas: Arc<Vec<TorrentID>>,
}

impl PieceFileStorageEngine {
    pub fn get_piece(&self, piece_id: u32) -> impl std::future::Future<Output=Result<Bytes, failure::Error>> {
        let lockables = Arc::clone(&self.lockables);
        let piece_shas = Arc::clone(&self.piece_shas);
        let total_length = self.total_length;
        let piece_length = self.piece_length;

        async move {
            let piece_sha: &TorrentID = piece_shas.get(piece_id as usize)
                .ok_or_else(|| ProtocolViolation)?;

            let piece_offset_start = piece_length * u64::from(piece_id);
            let mut piece_offset_end = piece_length * u64::from(piece_id + 1);
            if total_length < piece_offset_end {
                piece_offset_end = total_length;
            }

            
            let mut locked = lockables.lock().await;
            if let Some(cached) = locked.piece_cache.get(&piece_id) {
                return Ok(cached.clone());
            }

            let run_verify = match locked.verify_piece {
                PieceFileStorageEngineVerifyMode::Never => false,
                PieceFileStorageEngineVerifyMode::First { ref verified } => !verified.has(piece_id),
                PieceFileStorageEngineVerifyMode::Always => true,
            };

            locked.piece_file.seek(SeekFrom::Start(piece_offset_start)).await?;
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

                if let PieceFileStorageEngineVerifyMode::First { ref mut verified } = locked.verify_piece {
                    verified.set(piece_id, true);
                }
            }

            let out = Bytes::from(chonker);

            locked.piece_cache.put(piece_id, out.clone());

            Ok(out)
        }
    }
}
