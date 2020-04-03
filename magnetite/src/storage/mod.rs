use std::pin::Pin;
use bytes::Bytes;
use tokio::sync::broadcast;

mod piece_file;
mod remote_magnetite;

use crate::model::TorrentID;
use crate::model::MagnetiteError;
pub use self::piece_file::{
    PieceFileStorageEngine,
    PieceFileStorageEngineLockables,
    PieceFileStorageEngineVerifyMode,
    DOWNLOAD_CHUNK_SIZE,
};

pub trait PieceStorageEngine {
    fn get_piece(
        &self,
        content_key: &TorrentID,
        piece_id: u32,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>>;
}

pub trait PieceStorageEngineMut: PieceStorageEngine {
    fn write_chunk(
        &self,
        content_key: &TorrentID,
        piece_id: u32,
        chunk_offset: u32,
        data: Bytes,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<WriteChunkResponse, MagnetiteError>> + Send>>;
}

pub struct CompletionEvent {
    info_hash: TorrentID,
    piece_id: u32,
}

pub struct WriteChunkResponse {
    piece_completed: bool,
    piece_failed_validation: bool,
    completion: broadcast::Receiver<Result<CompletionEvent, MagnetiteError>>,
}

impl WriteChunkResponse {
    pub fn write_completed_piece(&self) -> bool {
        self.piece_completed
    }

    pub fn piece_failed_validation(&self) -> bool {
        self.piece_failed_validation
    }
}