use crate::model::config::{
    Config,
    StorageEngineElement,
    TorrentFactory,
};

fn storage_engine_from_config_helper(config: &Config) -> Result<Arc<PieceStorageEngine>, failure::Error> {
    let dumb_storage_engine = storage_engine_dumb_from_config(config)?;

    let mut state_builder = StateWrapper::builder();

    for t in &config.torrents {
        state_builder.register_info_hash(
            &tm.info_hash,
            state_wrapper::Registration {
                total_length: tm.total_length,
                piece_length: tm.meta.info.piece_length,
                piece_shas: tm.piece_shas.clone().into(),
            },
        );
    }

    Ok(state_builder.build(dumb_storage_engine))
}

pub fn storage_engine_from_config_helper(config: &Config) -> Result<Arc<PieceStorageEngine>, failure::Error> {

}

pub fn storage_engine_dumb_from_config(config: &Config) -> Result<Arc<PieceStorageEngineDumb>, failure::Error> {

}