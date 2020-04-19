use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::storage::sha_verify::ShaVerifyMode;

#[derive(Serialize, Deserialize)]
pub struct LegacyConfig {
    #[serde(default)]
    pub client_secret: String,
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


#[derive(Serialize, Deserialize)]
pub struct Config {
    storage_engine: Vec<StorageEngineElement>,
    torrents: Vec<TorrentFactory>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StorageEngineElement {
    RemoteMagnetite {
        upstream_server: String,
    },
    PieceFile,
    MultiFile,
    ShaValidate {
        #[serde(with = "validate_mode")]
        mode: ShaVerifyMode,
    },
    DiskCache {
        #[serde(default)]
        secret: String,
        cache_size: u64,
    },
}

#[derive(Serialize, Deserialize)]
pub struct TorrentFactory {
    torrent_file: PathBuf,
    source_file: PathBuf,
    secret: String,
}

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
