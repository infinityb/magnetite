use std::collections::BTreeMap;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::fmt;
use std::fs::OpenOptions;

use bytes::{Bytes, BytesMut};
use futures::future::{Future, FutureExt};
use futures::stream::StreamExt;
use salsa20::stream_cipher::{SyncStreamCipher, SyncStreamCipherSeek};
use salsa20::XSalsa20;
use tokio::io::AsyncReadExt;
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};
use tracing::{event, Level};

use magnetite_common::TorrentId;

use super::{
    utils::file_read_buf, GetPieceRequest, GetPieceRequestResolver, OpenFileCache,
    PieceStorageEngineDumb,
    GetPieceRequestChannel,
    disk_cache::{DiskCacheWrapper},
    memory_cache::{MemoryCacheWrapper},
};
use crate::control::messages::{BusAddTorrent, BusFindPieceFetcher, BusRemoveTorrent};
use crate::control::api::add_torrent_request::BackingFile;
use crate::model::{
    get_torrent_salsa,
    CompletionLost, InternalError, MagnetiteError, ProtocolViolation, TorrentMetaWrapped,
};
use crate::CommonInit;

#[derive(Clone)]
struct SharedState {
    lockable: Arc<Mutex<ServiceStateLocked>>,
}

#[derive(Debug)]
enum BackingStore {
    Tome(BackingStoreTome),
    MultiFile(BackingStoreMultiFile),
}

#[derive(Debug)]
struct BackingStoreTome {
    file_path: PathBuf,
}

#[derive(Debug)]
struct BackingStoreMultiFile {
    base_dir: PathBuf,
    piece_file_paths: BTreeMap<u64, FileInfo>,
}

#[derive(Debug)]
struct FileInfo {
    rel_path: PathBuf,
    file_size: u64,
}

struct ServiceStateLocked {
    torrents: BTreeMap<TorrentId, TorrentState>,
}

#[derive(Clone)]
struct TorrentState {
    crypto: Option<Arc<Mutex<XSalsa20>>>,
    backing_store: Arc<BackingStore>,
}

impl fmt::Debug for TorrentState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TorrentState")
            .field("backing_store", &self.backing_store)
            .finish()
    }
}

struct PieceSpan {
    fully_qualified_path: PathBuf,
    offset: u64,
    length: u64,
}

impl SharedState {
    fn add_torrent(
        &self,
        wrapped: &Arc<TorrentMetaWrapped>,
        backing: &BackingFile,
    ) -> Pin<Box<dyn Future<Output = Result<(), MagnetiteError>> + Send + 'static>> {
        let self_cloned: Self = self.clone();
        let wrapped: Arc<TorrentMetaWrapped> = Arc::clone(wrapped);
        let backing: BackingFile = backing.clone();
        async move {
            event!(
                Level::INFO,
                "SharedState::add_torrent(..., TorrentMetaWrapped {{ info_hash: {:?} }})",
                wrapped.info_hash
            );

            let backing_store = match backing {
                BackingFile::Multifile(ref mf) => {
                    let mut acc = 0;
                    let mut piece_file_paths: BTreeMap<u64, FileInfo> = Default::default();
                    for f in &wrapped.meta.info.files {
                        piece_file_paths.insert(acc, FileInfo {
                            rel_path: f.path.clone(),
                            file_size: f.length,
                        });
                        acc += f.length;
                    }

                    BackingStore::MultiFile(BackingStoreMultiFile {
                        base_dir: PathBuf::from(&mf.base_path),
                        piece_file_paths,
                    })
                }
                BackingFile::Tome(ref mf) => {
                    BackingStore::Tome(BackingStoreTome {
                        file_path: PathBuf::from(&mf.file_path),
                    })
                }
                BackingFile::Remote(ref mf) => {
                    return Err(MagnetiteError::InvalidArgument {
                        msg: format!("remote resources not supported in local mode"),
                    });
                }
            };

            let mut locked = self_cloned.lockable.lock().await;
            locked.torrents.insert(wrapped.info_hash, TorrentState {
                crypto: None,
                backing_store: Arc::new(backing_store),
            });

            event!(Level::INFO, "state = {:?}", locked.torrents);

            Ok(())
        }
        .boxed()
    }

    fn remove_torrent(
        &self,
        info_hash: &TorrentId,
    ) -> Pin<Box<dyn Future<Output = Result<(), MagnetiteError>> + Send + 'static>> {
        let info_hash: TorrentId = *info_hash;
        async move {
            event!(
                Level::INFO,
                "SharedState::remove_torrent(..., {:?})",
                info_hash
            );
            Ok(())
        }
        .boxed()
    }
}

impl BackingStore {
    fn get_piece_file_spans(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<Vec<PieceSpan>, MagnetiteError> {
        match *self {
            BackingStore::Tome(ref tome) => tome.get_piece_file_spans(offset, length),
            BackingStore::MultiFile(ref mf) => mf.get_piece_file_spans(offset, length),
        }
    }
}

impl BackingStoreTome {
    fn get_piece_file_spans(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<Vec<PieceSpan>, MagnetiteError> {
        Ok(vec![PieceSpan {
            fully_qualified_path: self.file_path.clone(),
            offset,
            length,
        }])
    }
}

impl BackingStoreMultiFile {
    fn get_piece_file_spans(
        &self,
        global_offset: u64,
        length: u64,
    ) -> Result<Vec<PieceSpan>, MagnetiteError> {
        let mut global_offset_acc = global_offset;
        let mut out = Vec::new();
        let mut req_size_acc = length;
        while 0 < req_size_acc {
            let (file_global_offset, file_info) = self
                .piece_file_paths
                .range(..=global_offset_acc)
                .rev()
                .next()
                .ok_or_else(|| InternalError {
                    msg: "failed to find span",
                })?;

            let file_rel_offset = global_offset - *file_global_offset;
            let mut file_remaining = file_info.file_size - file_rel_offset;
            if req_size_acc < file_remaining {
                file_remaining = req_size_acc;
            }
            req_size_acc -= file_remaining;
            global_offset_acc += file_remaining;

            let fully_qualified_path = self.base_dir.join(&file_info.rel_path);
            out.push(PieceSpan {
                fully_qualified_path,
                offset: file_rel_offset,
                length: file_remaining,
            });
        }

        Ok(out)
    }
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

pub fn start_piece_storage_engine(
    common: CommonInit,
    opts: RootStorageOpts,
) -> Pin<Box<dyn Future<Output = Result<(), failure::Error>> + Send + 'static>> {
    event!(Level::INFO, "starting piece storage engine");
    let shared_state = SharedState {
        lockable: Arc::new(Mutex::new(ServiceStateLocked {
            torrents: BTreeMap::new(),
        })),
    };

    let (tx, rx) = mpsc::channel(10);

    let control = tokio::task::spawn(piece_fetch_control_loop(
        common.clone(),
        shared_state.clone(),
        tx,
        opts,
    ));

    let data = tokio::task::spawn(piece_fetch_data_loop(common, shared_state, rx));

    async {
        let (a, b) = futures::future::join(control, data).await;

        a??;
        b??;

        event!(Level::INFO, "shut down piece storage engine");

        Ok(())
    }
    .boxed()
}

fn piece_fetch_control_loop(
    common: CommonInit,
    shared: SharedState,
    requests: mpsc::Sender<GetPieceRequestResolver>,
    opts: RootStorageOpts,
) -> Pin<Box<dyn Future<Output = Result<(), failure::Error>> + Send + 'static>> {
    let CommonInit {
        mut ebus,
        mut init_sig,
        mut term_sig,
    } = common;

    let mut ebus_incoming = ebus.subscribe();
    drop(ebus);
    drop(init_sig); // we've finished initializing.

    let gp: GetPieceRequestChannel = requests.into();

    async move {
        let engine_tmp: Box<dyn PieceStorageEngineDumb + Send + Sync + 'static> = Box::new(gp);
        let mut engine: Arc<dyn PieceStorageEngineDumb + Send + Sync + 'static> = engine_tmp.into();
        if opts.disk_cache_size_bytes != 0 {
            let mut engine_disk_builder = DiskCacheWrapper::build_with_capacity_bytes(opts.disk_cache_size_bytes);
            
            if let Some(cr) = get_torrent_salsa(&opts.disk_crypto_secret, &TorrentId::zero()) {
                engine_disk_builder.set_crypto(cr);
            }

            let cache_file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open("magnetite.cache")?;
            let engine_disk = engine_disk_builder.build(cache_file.into(), engine);

            let engine_tmp: Box<dyn PieceStorageEngineDumb + Send + Sync + 'static> = Box::new(engine_disk);
            engine = engine_tmp.into();
        }
        if opts.memory_cache_size_bytes != 0 {
            let engine_mem = MemoryCacheWrapper::build_with_capacity_bytes(opts.memory_cache_size_bytes)
                .build(engine);

            let engine_tmp: Box<dyn PieceStorageEngineDumb + Send + Sync + 'static> = Box::new(engine_mem);
            engine = engine_tmp.into();
        }
    
        loop {
            tokio::select! {
                _ = Pin::new(&mut term_sig) => {
                    drop(ebus_incoming);
                    break;
                }
                control_req = ebus_incoming.recv() => {
                    let creq = match control_req {
                        Ok(v) => v,
                        Err(broadcast::RecvError::Closed) => {
                            break;
                        }
                        Err(broadcast::RecvError::Lagged(..)) => {
                            event!(Level::ERROR, "piece fetch loop lagged - we're dropping requests");
                            continue;
                        },
                    };

                    if let Some(at) = creq.downcast_ref::<BusAddTorrent>() {
                        let mut at: BusAddTorrent = at.clone();
                        let future = shared.add_torrent(&at.torrent, &at.backing_file);
                        tokio::task::spawn(async move {
                            if let Err(err) = at.response.send(future.await).await {
                                event!(Level::ERROR, "dropped BusAddTorrent req");
                            }
                        });
                    }

                    if let Some(rt) = creq.downcast_ref::<BusRemoveTorrent>() {
                        let mut rt: BusRemoveTorrent = rt.clone();
                        let future = shared.remove_torrent(&rt.info_hash);
                        tokio::task::spawn(async move {
                            if let Err(err) = rt.response.send(future.await).await {
                                event!(Level::ERROR, "dropped BusRemoveTorrent req");
                            }
                        });
                    }

                    if let Some(pf) = creq.downcast_ref::<BusFindPieceFetcher>() {
                        let pf: BusFindPieceFetcher = pf.clone();
                        tokio::task::spawn(pf.respond(engine.clone()));
                    }
                },
            };
        }

        event!(Level::INFO, "piece fetcher control system has shut down");

        Ok(())
    }.boxed()
}

pub struct RootStorageOpts {
    pub concurrent_requests: usize,
    pub concurrent_open_files: usize,
    pub memory_cache_size_bytes: u64,
    pub disk_cache_size_bytes: u64,
    pub disk_crypto_secret: String,
}

fn piece_fetch_data_loop(
    common: CommonInit,
    shared: SharedState,
    mut requests: mpsc::Receiver<GetPieceRequestResolver>,
) -> Pin<Box<dyn Future<Output = Result<(), failure::Error>> + Send + 'static>> {
    let CommonInit {
        mut ebus,
        mut init_sig,
        mut term_sig,
    } = common;

    drop(ebus);
    drop(init_sig); // we've finished initializing.

    let files = OpenFileCache::new(64);
    let sema = Arc::new(tokio::sync::Semaphore::new(10));

    async move {
        loop {
            let req;
            tokio::select! {
                _ = Pin::new(&mut term_sig) => {
                    break;
                }
                req_opt = requests.next() => {
                    req = match req_opt {
                        Some(v) => v,
                        None => return Ok(()),
                    };
                }
            };

            let sema_acquired = sema.clone().acquire_owned();
            let shared_clone = shared.clone();
            let files_cloned = files.clone();

            let GetPieceRequestResolver { request, resolver } = req;
            tokio::spawn(async move {
                let resolve_res = resolver.send(
                    async move {
                        let ts: TorrentState = {
                            let locked = shared_clone.lockable.lock().await;

                            locked
                                .torrents
                                .get(&request.content_key)
                                .ok_or_else(|| ProtocolViolation)?
                                .clone()
                        };

                        let (piece_offset_start, piece_length) =
                            super::utils::compute_offset_length(
                                request.piece_index,
                                request.piece_length,
                                request.total_length,
                            );

                        let spans = ts
                            .backing_store
                            .get_piece_file_spans(piece_offset_start, piece_length as u64)?;

                        let mut chonker = BytesMut::with_capacity(piece_length as usize);
                        for entry in &spans {
                            let file = files_cloned.open(&entry.fully_qualified_path).await?;
                            let mut file_locked = file.lock().await;
                            let mut file_pinned = Pin::new(&mut *file_locked);

                            file_pinned.seek(SeekFrom::Start(entry.offset)).await?;
                            file_read_buf(file_pinned.take(entry.length), &mut chonker).await?;
                        }

                        if let Some(ref crypto) = ts.crypto {
                            let mut cr = crypto.lock().await;
                            cr.seek(piece_offset_start);
                            cr.apply_keystream(&mut chonker[..]);
                        }

                        Ok(Bytes::from(chonker))
                    }
                    .await,
                );

                if resolve_res.is_err() {
                    event!(Level::WARN, "dropped a response");
                }

                drop(sema_acquired);
            });
        }

        event!(Level::INFO, "piece fetcher data system has shut down");

        Ok(())
    }
    .boxed()
}
