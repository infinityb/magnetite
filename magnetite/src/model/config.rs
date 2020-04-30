use std::collections::HashMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use salsa20::stream_cipher::generic_array::GenericArray;
use salsa20::stream_cipher::NewStreamCipher;
use salsa20::XSalsa20;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use tracing::{event, Level};

use crate::model::{FileError, InternalError, TorrentID, TorrentMetaWrapped};
use crate::storage::state_wrapper::ContentInfoManager;
use crate::storage::{
    disk_cache::DiskCacheWrapper, memory_cache::MemoryCacheWrapper, piece_file,
    remote_magnetite::RemoteMagnetite, sha_verify::ShaVerifyMode, state_wrapper,
    PieceFileStorageEngine, PieceStorageEngine, PieceStorageEngineDumb, ShaVerify, StateWrapper,
};

#[derive(Serialize, Deserialize)]
pub struct LegacyConfig {
    #[serde(default)]
    pub client_secret: String,
    #[serde(default)]
    pub cache_secret: String,
    #[serde(default)]
    pub seed_bind_addr: String, // "[::]:17862"

    pub torrents: Vec<LegacyTorrentFactory>,
}

#[derive(Serialize, Deserialize)]
pub struct LegacyTorrentFactory {
    pub torrent_file: PathBuf,
    pub source_file: PathBuf,
    pub secret: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    #[serde(default)]
    pub storage_engine: Vec<StorageEngineElement>,
    #[serde(default)]
    pub frontends: Vec<Frontend>,
    #[serde(default)]
    pub torrents: Vec<TorrentFactory>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "name", rename_all = "snake_case")]
pub enum Frontend {
    Host(FrontendHost),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FrontendHost {
    pub bind_address: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StorageEngineElementRemoteMagnetite {
    upstream_server: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StorageEngineElementShaValidate {
    #[serde(with = "validate_mode")]
    mode: ShaVerifyMode,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StorageEngineElementMemoryCache {
    cache_size: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StorageEngineElementDiskCache {
    #[serde(default)]
    secret: String,
    cache_size: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "name", rename_all = "snake_case")]
pub enum StorageEngineElement {
    DiskCache(StorageEngineElementDiskCache),
    MemoryCache(StorageEngineElementMemoryCache),
    MultiFile,
    PieceFile,
    RemoteMagnetite(StorageEngineElementRemoteMagnetite),
    ShaValidate(StorageEngineElementShaValidate),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TorrentFactory {
    torrent_file: PathBuf,
    source_file: PathBuf,
    secret: String,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
mod validate_mode {
    // lets keep this private to the config module, I guess.

    use serde::{Deserialize, Serialize};
    use serde::{Deserializer, Serializer};

    use super::ShaVerifyMode;

    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum Inner {
        First,
        Always,
        Never,
    }

    pub fn serialize<S>(id: &ShaVerifyMode, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match id {
            ShaVerifyMode::First => Inner::First,
            ShaVerifyMode::Always => Inner::Always,
            ShaVerifyMode::Never => Inner::Never,
        }
        .serialize(ser)
    }

    pub fn deserialize<'de, D>(de: D) -> Result<ShaVerifyMode, D::Error>
    where
        D: Deserializer<'de>,
    {
        let inner: Inner = Deserialize::deserialize(de)?;
        Ok(match inner {
            Inner::First => ShaVerifyMode::First,
            Inner::Always => ShaVerifyMode::Always,
            Inner::Never => ShaVerifyMode::Never,
        })
    }
}

fn build_torrent_map(
    config: &Config,
) -> Result<HashMap<PathBuf, Arc<TorrentMetaWrapped>>, failure::Error> {
    let mut path_to_torrent = HashMap::new();

    for s in &config.torrents {
        let mut fi = File::open(&s.torrent_file).map_err(|e| FileError {
            path: s.torrent_file.clone(),
            cause: e,
        })?;
        let mut by = Vec::new();
        fi.read_to_end(&mut by).map_err(|e| FileError {
            path: s.torrent_file.clone(),
            cause: e,
        })?;

        let torrent = TorrentMetaWrapped::from_bytes(&by)?;
        path_to_torrent.insert(s.torrent_file.clone(), Arc::new(torrent));
    }

    Ok(path_to_torrent)
}

pub fn get_torrent_salsa(crypto_secret: &str, info_hash: &TorrentID) -> Option<XSalsa20> {
    if !crypto_secret.is_empty() {
        let mut nonce_data = [0; 24];
        nonce_data[4..].copy_from_slice(info_hash.as_bytes());
        let nonce = GenericArray::from_slice(&nonce_data[..]);
        let key = GenericArray::from_slice(crypto_secret.as_bytes());
        Some(XSalsa20::new(&key, &nonce))
    } else {
        None
    }
}

fn piece_file(
    config: &Config,
    path_to_torrent: &HashMap<PathBuf, Arc<TorrentMetaWrapped>>,
) -> Result<Arc<dyn PieceStorageEngineDumb + Send + Sync + 'static>, failure::Error> {
    event!(Level::INFO, "configuring PieceFile backend");
    let mut pf_builder = PieceFileStorageEngine::builder();

    for s in &config.torrents {
        let pf = File::open(&s.source_file).map_err(|e| FileError {
            path: s.torrent_file.clone(),
            cause: e,
        })?;

        let metainfo = path_to_torrent
            .get(&s.torrent_file)
            .ok_or_else(|| InternalError {
                msg: "failed to find torrent metainfo from path",
            })?;

        let mut crypto = None;
        if let Some(salsa) = get_torrent_salsa(&s.secret, &metainfo.info_hash) {
            crypto = Some(salsa);
        }
        pf_builder.register_info_hash(
            &metainfo.info_hash,
            piece_file::Registration {
                piece_count: metainfo.piece_shas.len() as u32,
                crypto,
                piece_file: pf.into(),
            },
        );
    }

    let boxed: Box<dyn PieceStorageEngineDumb + Send + Sync + 'static> =
        Box::new(pf_builder.build());
    Ok(boxed.into())
}

fn remote_magnetite(
    runtime: &mut Runtime,
    rm: &StorageEngineElementRemoteMagnetite,
) -> Result<Arc<dyn PieceStorageEngineDumb + Send + Sync + 'static>, failure::Error> {
    event!(
        Level::INFO,
        "configuring RemoteMagnetite backend - {:?}",
        rm
    );
    let upstream = rm.upstream_server.clone();

    let storage_backend = runtime.block_on(async move { RemoteMagnetite::connected(&upstream) });

    let boxed: Box<dyn PieceStorageEngineDumb + Send + Sync + 'static> = Box::new(storage_backend);
    Ok(boxed.into())
}

fn build_storage_engine_dumb_helper(
    runtime: &mut Runtime,
    config: &Config,
    path_to_torrent: &HashMap<PathBuf, Arc<TorrentMetaWrapped>>,
) -> Result<Arc<dyn PieceStorageEngineDumb + Send + Sync + 'static>, failure::Error> {
    let mut se_iter = config.storage_engine.iter();
    let first_engine = se_iter.next().ok_or_else(|| InternalError {
        msg: "must define at least one storage engine",
    })?;

    let mut current_engine: Arc<dyn PieceStorageEngineDumb + Send + Sync + 'static> =
        match first_engine {
            StorageEngineElement::DiskCache(..) => {
                return Err(InvalidRootStorage(InvalidStorage { name: "disk cache" }).into());
            }
            StorageEngineElement::MemoryCache(..) => {
                return Err(InvalidRootStorage(InvalidStorage { name: "memory cache" }).into());
            }
            StorageEngineElement::MultiFile => {
                unimplemented!();
            }
            StorageEngineElement::PieceFile => piece_file(config, path_to_torrent)?,
            StorageEngineElement::RemoteMagnetite(ref rm) => remote_magnetite(runtime, rm)?,
            StorageEngineElement::ShaValidate(..) => {
                return Err(InvalidRootStorage(InvalidStorage {
                    name: "sha-validate",
                })
                .into());
            }
        };

    for engine_config in se_iter {
        match engine_config {
            StorageEngineElement::MultiFile => {
                return Err(InvalidChainedStorage(InvalidStorage { name: "multi-file" }).into());
            }
            StorageEngineElement::PieceFile => {
                return Err(InvalidChainedStorage(InvalidStorage { name: "piece-file" }).into());
            }
            StorageEngineElement::RemoteMagnetite(..) => {
                return Err(InvalidChainedStorage(InvalidStorage {
                    name: "remote-magnetite",
                })
                .into());
            }
            StorageEngineElement::MemoryCache(ref mc) => {
                event!(Level::INFO, "configuring MemoryCache backend - {:?}", mc);
                let cache = MemoryCacheWrapper::build_with_capacity_bytes(mc.cache_size);

                let boxed: Box<dyn PieceStorageEngineDumb + Send + Sync + 'static> =
                    Box::new(cache.build(current_engine));
                current_engine = boxed.into();
            }
            StorageEngineElement::DiskCache(ref dc) => {
                event!(Level::INFO, "configuring DiskCache backend - {:?}", dc);
                let cache_file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open("magnetite.cache")?;

                let mut cache = DiskCacheWrapper::build_with_capacity_bytes(dc.cache_size);

                let zero_iv = TorrentID::zero();
                if let Some(salsa) = get_torrent_salsa(&dc.secret, &zero_iv) {
                    cache.set_crypto(salsa);
                }

                let boxed: Box<dyn PieceStorageEngineDumb + Send + Sync + 'static> =
                    Box::new(cache.build(cache_file.into(), current_engine));
                current_engine = boxed.into();
            }
            StorageEngineElement::ShaValidate(ref sv) => {
                event!(Level::INFO, "configuring ShaValidate backend - {:?}", sv);
                let boxed: Box<dyn PieceStorageEngineDumb + Send + Sync + 'static> =
                    Box::new(ShaVerify::new(current_engine, sv.mode));
                current_engine = boxed.into();
            }
        }
    }

    Ok(current_engine)
}

fn build_storage_engine_helper(
    _config: &Config,
) -> Result<Arc<Box<dyn PieceStorageEngine + Send + Sync + 'static>>, failure::Error> {
    unimplemented!();
}

pub fn build_storage_engine_dumb(
    runtime: &mut Runtime,
    config: &Config,
) -> Result<Arc<dyn PieceStorageEngineDumb + Send + Sync + 'static>, failure::Error> {
    let path_to_torrent = build_torrent_map(config)?;
    build_storage_engine_dumb_helper(runtime, config, &path_to_torrent)
}

pub fn build_storage_engine(
    runtime: &mut Runtime,
    config: &Config,
) -> Result<Arc<Box<dyn PieceStorageEngine + Send + Sync + 'static>>, failure::Error> {
    let path_to_torrent = build_torrent_map(config)?;
    let dumb = build_storage_engine_dumb_helper(runtime, config, &path_to_torrent)?;

    let mut state_builder = StateWrapper::builder();

    for s in &config.torrents {
        let metainfo = path_to_torrent
            .get(&s.torrent_file)
            .ok_or_else(|| InternalError {
                msg: "failed to find torrent metainfo from path",
            })?;

        state_builder.register_info_hash(
            &metainfo.info_hash,
            state_wrapper::Registration {
                total_length: metainfo.total_length,
                piece_length: metainfo.meta.info.piece_length,
                piece_shas: metainfo.piece_shas.clone(),
            },
        );
    }

    let boxed: Box<dyn PieceStorageEngine + Send + Sync + 'static> =
        Box::new(state_builder.build(dumb));

    Ok(boxed.into())
}

pub struct BuiltStates {
    pub storage_engine: Arc<dyn PieceStorageEngineDumb + Send + Sync + 'static>,
    pub content_info_manager: ContentInfoManager,
    pub path_to_torrent: HashMap<PathBuf, Arc<TorrentMetaWrapped>>,
}

pub fn build_storage_engine_states(
    runtime: &mut Runtime,
    config: &Config,
) -> Result<BuiltStates, failure::Error> {
    let path_to_torrent = build_torrent_map(config)?;
    let storage_engine = build_storage_engine_dumb_helper(runtime, config, &path_to_torrent)?;

    let mut state_builder = StateWrapper::builder();

    for s in &config.torrents {
        let metainfo = path_to_torrent
            .get(&s.torrent_file)
            .ok_or_else(|| InternalError {
                msg: "failed to find torrent metainfo from path",
            })?;

        state_builder.register_info_hash(
            &metainfo.info_hash,
            state_wrapper::Registration {
                total_length: metainfo.total_length,
                piece_length: metainfo.meta.info.piece_length,
                piece_shas: metainfo.piece_shas.clone(),
            },
        );
    }

    let content_info_manager = state_builder.build_content_info_manager();

    Ok(BuiltStates {
        storage_engine,
        content_info_manager,
        path_to_torrent,
    })
}

// --

#[derive(Debug)]
struct InvalidStorage {
    name: &'static str,
}

// --

#[derive(Debug)]
struct InvalidChainedStorage(InvalidStorage);

impl fmt::Display for InvalidChainedStorage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} it not a valid chained storage engine", self.0.name)
    }
}

impl std::error::Error for InvalidChainedStorage {}

// --

#[derive(Debug)]
struct InvalidRootStorage(InvalidStorage);

impl fmt::Display for InvalidRootStorage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} is not a valid root storage engine", self.0.name)
    }
}

impl std::error::Error for InvalidRootStorage {}
