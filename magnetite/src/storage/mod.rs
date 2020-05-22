use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::future::{Future, FutureExt};
use lru::LruCache;
use tokio::fs::File as TokioFile;
use tokio::sync::Mutex;

use magnetite_common::TorrentId;

pub mod disk_cache;
pub mod memory_cache;
pub mod multi_file;
pub mod piece_file;
pub mod piggyback;
pub mod remote_magnetite;
pub mod sha_verify;
pub mod state_wrapper;

#[cfg(test)]
pub mod test_utils;

pub const DOWNLOAD_CHUNK_SIZE: u32 = 16 * 1024;

pub use self::disk_cache::DiskCacheWrapper;
pub use self::piece_file::PieceFileStorageEngine;
pub use self::sha_verify::{ShaVerify, ShaVerifyMode};
pub use self::state_wrapper::StateWrapper;

use crate::model::MagnetiteError;
use crate::storage::state_wrapper::{ContentInfo, ContentInfoManager};

#[derive(Copy, Clone, Debug)]
pub struct GetPieceRequest {
    pub content_key: TorrentId,
    pub piece_sha: TorrentId,
    pub piece_length: u32,
    pub total_length: u64,
    pub piece_index: u32,
}

pub trait PieceStorageEngine {
    fn get_piece(
        &self,
        content_key: &TorrentId,
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
//         content_key: &TorrentId,
//         piece_id: u32,
//         chunk_offset: u32,
//         finalize_piece: bool,
//         data: Bytes,
//     ) -> Pin<Box<dyn std::future::Future<Output = Result<WriteChunkResponse, MagnetiteError>> + Send>>;
// }

// pub struct CompletionEvent {
//     info_hash: TorrentId,
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
    use std::io::{self, SeekFrom};

    use tokio::fs::File as TokioFile;
    use tokio::io::AsyncReadExt;
    use tokio::sync::Mutex;

    pub async fn piece_file_pread_exact(
        file: &Mutex<TokioFile>,
        offset: u64,
        buf: &mut [u8],
    ) -> io::Result<()> {
        let mut piece_file = file.lock().await;
        piece_file.seek(SeekFrom::Start(offset)).await?;
        piece_file.read_exact(buf).await?;
        Ok(())
    }

    #[inline]
    pub fn compute_piece_index_lb(position: u64, atom_length: u32) -> u32 {
        let atom_length = u64::from(atom_length);
        (position / atom_length) as u32
    }

    #[inline]
    pub fn compute_piece_index_ub(position: u64, atom_length: u32) -> u32 {
        let atom_length = u64::from(atom_length);
        let mut piece_index = (position / atom_length) as u32;
        if position % atom_length != 0 {
            piece_index += 1;
        }
        piece_index
    }

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
    pub content_key: TorrentId,
    pub piece_shas: &'a [TorrentId],
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

    let read_piece_offset = (piece_file_offset_start % piece_length) as usize;
    let piece_index = utils::compute_piece_index_lb(piece_file_offset_start, request.piece_length);
    let piece_index_end =
        utils::compute_piece_index_ub(piece_file_offset_end, request.piece_length);

    let mut out_buf = BytesMut::new();
    for pi in piece_index..piece_index_end {
        let req = GetPieceRequest {
            content_key: request.content_key,
            piece_sha: request.piece_shas[pi as usize],
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

pub async fn get_content_info(
    cim: &Mutex<ContentInfoManager>,
    content_key: &TorrentId,
) -> Option<ContentInfo> {
    // FIXME: a readlock is fine here.
    let c = cim.lock().await;
    let ci = c.data.get(content_key)?;
    Some(ci.clone())
}

impl<T> PieceStorageEngine for Box<T>
where
    T: PieceStorageEngine + ?Sized,
{
    fn get_piece(
        &self,
        content_key: &TorrentId,
        piece_id: u32,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>> {
        PieceStorageEngine::get_piece(&**self, content_key, piece_id)
    }
}

impl<T> PieceStorageEngineDumb for Box<T>
where
    T: PieceStorageEngineDumb + ?Sized,
{
    fn get_piece_dumb(
        &self,
        req: &GetPieceRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>> {
        PieceStorageEngineDumb::get_piece_dumb(&**self, req)
    }
}

impl<T> PieceStorageEngine for Arc<T>
where
    T: PieceStorageEngine + ?Sized,
{
    fn get_piece(
        &self,
        content_key: &TorrentId,
        piece_id: u32,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>> {
        PieceStorageEngine::get_piece(&**self, content_key, piece_id)
    }
}

impl<T> PieceStorageEngineDumb for Arc<T>
where
    T: PieceStorageEngineDumb + ?Sized,
{
    fn get_piece_dumb(
        &self,
        req: &GetPieceRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>> {
        PieceStorageEngineDumb::get_piece_dumb(&**self, req)
    }
}

// --

#[derive(Clone)]
pub struct OpenFileCache {
    open_files: Arc<Mutex<lru::LruCache<PathKey, Arc<Mutex<TokioFile>>>>>,
}

#[derive(Hash, Eq, PartialEq)]
struct PathKey {
    fully_qualified: PathBuf,
}

impl OpenFileCache {
    fn new(size: usize) -> OpenFileCache {
        OpenFileCache {
            open_files: Arc::new(Mutex::new(LruCache::new(size))),
        }
    }

    fn open(
        &self,
        path: &Path,
    ) -> Pin<Box<dyn Future<Output = io::Result<Arc<Mutex<TokioFile>>>> + Send + 'static>> {
        let self_cloned = self.clone();
        let key = PathKey {
            fully_qualified: path.into(),
        };
        async move {
            let mut open_files = self_cloned.open_files.lock().await;
            if let Some(file) = open_files.get(&key) {
                return Ok(file.clone());
            }

            let file = TokioFile::open(&key.fully_qualified).await?;
            let file_arc = Arc::new(Mutex::new(file));
            open_files.put(key, file_arc.clone());

            Ok(file_arc)
        }
        .boxed()
    }
}
