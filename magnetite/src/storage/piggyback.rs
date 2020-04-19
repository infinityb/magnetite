use bytes::Bytes;
use tokio::sync::watch;

use crate::model::{CompletionLost, MagnetiteError};

// we also need this in the PieceFile storage engine. lets generalize and use composition, if we can.
#[derive(Clone)]
pub struct Inflight {
    // starts off as None and is eventually resolved exactly once.
    pub finished: watch::Receiver<Option<Result<Bytes, MagnetiteError>>>,
}

impl Inflight {
    pub fn complete(
        mut self,
    ) -> impl std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send {
        async move {
            loop {
                match self.finished.recv().await {
                    Some(Some(v)) => return v,
                    Some(None) => continue,
                    None => return Err(CompletionLost.into()),
                }
            }
        }
    }
}
