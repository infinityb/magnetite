use std::collections::BTreeMap;
use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::future::FutureExt;
use lru::LruCache;
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::Mutex;
use tracing::{event, Level};

use magnetite_common::TorrentId;

use super::{GetPieceRequest, PieceStorageEngineDumb};
use crate::model::{InternalError, MagnetiteError, ProtocolViolation};

struct TorrentState {
    base_dir: PathBuf,
    piece_file_paths: BTreeMap<u64, FileInfo>,
}

#[derive(Clone)]
pub struct MultiFileStorageEngine {
    open_files_lru: Arc<Mutex<lru::LruCache<PathKey, Arc<Mutex<TokioFile>>>>>,
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

#[derive(Debug)]
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
            open_files_lru: Arc::new(Mutex::new(LruCache::new(32))),
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
                        msg: "failed to find file",
                    })?;

                let file_rel_offset = piece_offset_start - *offset;
                let mut file_remaining = file_info.file_size - file_rel_offset;
                if piece_size_acc < file_remaining {
                    file_remaining = piece_size_acc;
                }
                piece_size_acc -= file_remaining;
                piece_offset_start += file_remaining;

                let path = ts.base_dir.join(&file_info.rel_path);
                let file = open_helper(&self_cloned, &path).await?;
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

async fn open_helper(m: &MultiFileStorageEngine, path: &Path) -> io::Result<Arc<Mutex<TokioFile>>> {
    let key = PathKey {
        fully_qualified: path.into(),
    };
    let mut open_files = m.open_files_lru.lock().await;
    if let Some(file) = open_files.get(&key) {
        return Ok(file.clone());
    }

    let file = TokioFile::open(&key.fully_qualified).await?;
    let file_arc = Arc::new(Mutex::new(file));
    open_files.put(key, file_arc.clone());

    Ok(file_arc)
}

async fn file_read_buf<R: AsyncRead + Unpin>(mut file: R, out: &mut BytesMut) -> io::Result<()> {
    loop {
        let bytes_read = file.read_buf(out).await?;
        if bytes_read == 0 {
            break;
        }
    }
    Ok(())
}
