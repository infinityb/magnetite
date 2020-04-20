use std::pin::Pin;

use bytes::{Bytes, BytesMut};

pub mod disk_cache_layer;
pub mod piece_file;
pub mod piggyback;
pub mod remote_magnetite;
pub mod sha_verify;
pub mod state_wrapper;

pub const DOWNLOAD_CHUNK_SIZE: u32 = 16 * 1024;

pub use self::piece_file::PieceFileStorageEngine;
pub use self::sha_verify::{ShaVerify, ShaVerifyMode};
pub use self::state_wrapper::StateWrapper;

use crate::model::{MagnetiteError, TorrentID};

#[derive(Copy, Clone, Debug)]
pub struct GetPieceRequest {
    pub content_key: TorrentID,
    pub piece_sha: TorrentID,
    pub piece_length: u32,
    pub total_length: u64,
    pub piece_index: u32,
}

pub trait PieceStorageEngine {
    fn get_piece(
        &self,
        content_key: &TorrentID,
        piece_id: u32,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>>;
}

pub trait PieceStorageEngineDumb {
    fn get_piece_dumb(
        &self,
        req: &GetPieceRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>>;
}

// pub trait PieceStorageEngineMut: PieceStorageEngine {
//     fn write_chunk(
//         &self,
//         content_key: &TorrentID,
//         piece_id: u32,
//         chunk_offset: u32,
//         finalize_piece: bool,
//         data: Bytes,
//     ) -> Pin<Box<dyn std::future::Future<Output = Result<WriteChunkResponse, MagnetiteError>> + Send>>;
// }

// pub struct CompletionEvent {
//     info_hash: TorrentID,
//     piece_id: u32,
// }

// pub struct WriteChunkResponse {
//     piece_completed: bool,
//     piece_failed_validation: bool,
//     completion: broadcast::Receiver<Result<CompletionEvent, MagnetiteError>>,
// }

// impl WriteChunkResponse {
//     pub fn write_completed_piece(&self) -> bool {
//         self.piece_completed
//     }

//     pub fn piece_failed_validation(&self) -> bool {
//         self.piece_failed_validation
//     }
// }

pub mod utils {
    #[inline]
    pub fn compute_offset(index: u32, atom_length: u32, total_length: u64) -> (u64, u64) {
        let (start, length) = compute_offset_length(index, atom_length, total_length);
        (start, start + u64::from(length))
    }

    #[inline]
    pub fn compute_offset_length(index: u32, atom_length: u32, total_length: u64) -> (u64, u32) {
        let atom_length = u64::from(atom_length);
        let index = u64::from(index);

        let offset_start = atom_length * index;
        let mut offset_end = atom_length * (index + 1);
        if total_length < offset_end {
            offset_end = total_length;
        }

        (offset_start, (offset_end - offset_start) as u32)
    }
}

pub struct MultiPieceReadRequest<'a> {
    pub content_key: TorrentID,
    pub piece_shas: &'a [TorrentID],
    pub piece_length: u32,
    pub total_length: u64,

    pub torrent_global_offset: u64,
    pub file_offset: u64,
    pub read_length: usize,
}

pub async fn multi_piece_read<S>(
    storage_engine: &S,
    request: &MultiPieceReadRequest<'_>,
) -> Result<Bytes, failure::Error>
where
    S: PieceStorageEngineDumb,
{
    // all this maths seems common, we need utilities for this.
    let piece_length = u64::from(request.piece_length);
    let piece_file_offset_start = request.torrent_global_offset + request.file_offset;
    let piece_file_offset_end = piece_file_offset_start + request.read_length as u64;

    let piece_index = (piece_file_offset_start / piece_length) as u32;
    let read_piece_offset = (piece_file_offset_start % piece_length) as usize;

    let mut piece_index_end = (piece_file_offset_end / piece_length) as u32;
    if piece_file_offset_end % piece_length != 0 {
        piece_index_end += 1;
    }

    let mut out_buf = BytesMut::new();
    for pi in piece_index..piece_index_end {
        let req = GetPieceRequest {
            content_key: request.content_key,
            piece_sha: request.piece_shas.get(pi as usize).unwrap().clone(),
            piece_length: request.piece_length,
            total_length: request.total_length,
            piece_index: pi,
        };

        let p = storage_engine.get_piece_dumb(&req).await?;
        out_buf.extend_from_slice(&p[..]);
    }

    // drop the unwanted part at the start of the first piece.
    drop(out_buf.split_to(read_piece_offset));
    // and drop off the unwanted data at the end of the last piece.
    Ok(out_buf.split_to(request.read_length).freeze())
}
