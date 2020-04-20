use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::FutureExt;
use tokio::sync::Mutex;

use crate::model::{MagnetiteError, ProtocolViolation, TorrentID};
use crate::storage::get_content_info;
use crate::storage::{GetPieceRequest, PieceStorageEngine, PieceStorageEngineDumb};

#[derive(Clone)]
pub struct StateWrapper<P> {
    dumb: P,
    state: Arc<Mutex<ContentInfoManager>>,
}

#[derive(Default, Debug, Clone)]
pub struct ContentInfoManager {
    pub data: BTreeMap<TorrentID, ContentInfo>,
}

pub struct Builder {
    state: ContentInfoManager,
}

#[derive(Clone, Debug)]
pub struct ContentInfo {
    pub total_length: u64,
    pub piece_length: u32,
    pub piece_shas: Arc<[TorrentID]>,
}

#[derive(Clone)]
pub struct Registration {
    pub total_length: u64,
    pub piece_length: u32,
    pub piece_shas: Vec<TorrentID>,
}

impl StateWrapper<()> {
    pub fn builder() -> Builder {
        Builder {
            state: Default::default(),
        }
    }
}

impl ContentInfoManager {
    pub fn get_content_info(&self, content_key: &TorrentID) -> Option<ContentInfo> {
        self.data.get(content_key).map(Clone::clone)
    }
}

impl<P> PieceStorageEngine for StateWrapper<P>
where
    P: PieceStorageEngineDumb + Clone + Send + Sync + 'static,
{
    fn get_piece(
        &self,
        content_key: &TorrentID,
        piece_id: u32,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>> {
        let piece_key = (*content_key, piece_id);
        let self_cloned: Self = self.clone();

        async move {
            let content_info = get_content_info(&*self_cloned.state, &piece_key.0)
                .await
                .ok_or(ProtocolViolation)?;

            let piece_sha = content_info
                .piece_shas
                .get(piece_id as usize)
                .ok_or_else(|| ProtocolViolation)?
                .clone();

            let preq = GetPieceRequest {
                content_key: piece_key.0,
                piece_sha: piece_sha,
                piece_length: content_info.piece_length,
                total_length: content_info.total_length,
                piece_index: piece_key.1,
            };

            self_cloned.dumb.get_piece_dumb(&preq).await
        }
        .boxed()
    }
}

impl Builder {
    pub fn register_info_hash(&mut self, info_hash: &TorrentID, ci: Registration) {
        self.state.data.insert(
            *info_hash,
            ContentInfo {
                total_length: ci.total_length,
                piece_length: ci.piece_length,
                piece_shas: ci.piece_shas.clone().into(),
            },
        );
    }

    pub fn build<P>(self, wrapped: P) -> StateWrapper<P>
    where
        P: PieceStorageEngineDumb + Clone,
    {
        StateWrapper {
            dumb: wrapped,
            state: Arc::new(Mutex::new(self.state)),
        }
    }

    pub fn build_content_info_manager(self) -> ContentInfoManager {
        self.state
    }
}
