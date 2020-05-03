use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use futures::future::FutureExt;

use crate::model::MagnetiteError;
use crate::storage::{GetPieceRequest, PieceStorageEngineDumb};

#[derive(Clone)]
pub struct MockPieceStorageEngineDumb {
    pub request_counts: Arc<Mutex<u64>>,
}

impl MockPieceStorageEngineDumb {
    pub fn new() -> MockPieceStorageEngineDumb {
        MockPieceStorageEngineDumb {
            request_counts: Arc::new(Mutex::new(0)),
        }
    }
}

impl PieceStorageEngineDumb for MockPieceStorageEngineDumb {
    fn get_piece_dumb(
        &self,
        req: &GetPieceRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, MagnetiteError>> + Send>> {
        let mut request_counts = self.request_counts.lock().unwrap();
        *request_counts += 1;
        drop(request_counts);

        let data = vec![0; req.piece_length as usize];
        async { Ok(data.into()) }.boxed()
    }
}
