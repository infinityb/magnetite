use super::{PieceCacheInfo, FileSpan};

pub(super) fn cache_cleanup(_cache: &mut PieceCacheInfo, _adding: u64, _batch_size: u64) -> Vec<FileSpan> {
    Vec::new()
}