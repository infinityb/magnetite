use std::collections::BTreeMap;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::future::FutureExt;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;
use tracing::{event, Level};

use magnetite_common::TorrentId;

use super::{utils::file_read_buf, GetPieceRequest, OpenFileCache, PieceStorageEngineDumb};
use crate::model::{InternalError, MagnetiteError, ProtocolViolation};

struct TorrentState {
    base_dir: PathBuf,
    piece_file_paths: BTreeMap<u64, FileInfo>,
}

#[derive(Clone)]
pub struct MultiFileStorageEngine {
    file_cache: OpenFileCache,
    torrents: Arc<Mutex<BTreeMap<TorrentId, Arc<TorrentState>>>>,
}

#[derive(Hash, Eq, PartialEq)]
struct PathKey {
    fully_qualified: PathBuf,
}

pub struct Builder {
    torrents: BTreeMap<TorrentId, Arc<TorrentState>>,
}

pub struct Registration {
    pub base_dir: PathBuf,
    pub files: Vec<FileInfo>,
}

pub struct FileInfo {
    pub rel_path: PathBuf,
    pub file_size: u64,
}

impl Builder {
    pub fn register_info_hash(&mut self, content_key: &TorrentId, reg: Registration) {
        let mut piece_file_paths = BTreeMap::new();
        let mut acc = 0;
        for file in reg.files {
            let file_offset = acc;
            acc += file.file_size;
            piece_file_paths.insert(file_offset, file);
        }

        self.torrents.insert(
            *content_key,
            Arc::new(TorrentState {
                base_dir: reg.base_dir,
                piece_file_paths,
            }),
        );
    }

    pub fn build(self) -> MultiFileStorageEngine {
        MultiFileStorageEngine {
            torrents: Arc::new(Mutex::new(self.torrents)),
            file_cache: OpenFileCache::new(32),
        }
    }
}

impl MultiFileStorageEngine {
    #[inline]
    pub fn builder() -> Builder {
        Builder {
            torrents: Default::default(),
        }
    }
}

impl PieceStorageEngineDumb for MultiFileStorageEngine {
    fn get_piece_dumb(
        &self,
        req: &GetPieceRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>> {
        let self_cloned: Self = self.clone();
        let req: GetPieceRequest = *req;

        async move {
            let ts: Arc<TorrentState> = {
                let torrents = self_cloned.torrents.lock().await;

                let torrent_state = torrents
                    .get(&req.content_key)
                    .ok_or_else(|| ProtocolViolation)?;

                Arc::clone(torrent_state)
            };

            let (mut piece_offset_start, piece_offset_end) =
                super::utils::compute_offset(req.piece_index, req.piece_length, req.total_length);

            let mut piece_size_acc = piece_offset_end - piece_offset_start;
            let piece_size = piece_size_acc as usize;
            let mut chonker = BytesMut::with_capacity(piece_size);
            while 0 < piece_size_acc {
                let (offset, file_info) = ts
                    .piece_file_paths
                    .range(..=piece_offset_start)
                    .rev()
                    .next()
                    .ok_or_else(|| InternalError {
                        msg: "failed to find span",
                    })?;

                let file_rel_offset = piece_offset_start - *offset;
                let mut file_remaining = file_info.file_size - file_rel_offset;
                if piece_size_acc < file_remaining {
                    file_remaining = piece_size_acc;
                }
                piece_size_acc -= file_remaining;
                piece_offset_start += file_remaining;

                let path = ts.base_dir.join(&file_info.rel_path);
                let file = self_cloned.file_cache.open(&path).await?;
                let mut file_locked = file.lock().await;
                let mut file_pinned = Pin::new(&mut *file_locked);
                event!(Level::TRACE, path=?path.display(), offset=file_rel_offset);
                file_pinned.seek(SeekFrom::Start(file_rel_offset)).await?;
                file_read_buf(file_pinned.take(file_remaining), &mut chonker).await?;
            }

            Ok(chonker.freeze())
        }
        .boxed()
    }
}
