use std::cmp::Ordering;
use std::time::{SystemTime, Duration};
use std::collections::BinaryHeap;

use tracing::{event, Level};
use nix::fcntl::{fallocate, FallocateFlags};
use metrics::{counter};

use magnetite_common::TorrentId;

use super::{PieceCacheInfo, FileSpan};
use crate::utils::OwnedFd;

#[derive(Debug)]
struct HeapEntry {
    last_touched: SystemTime,
    piece_length: u32,
    btree_key: (TorrentId, u32),
}

impl Eq for HeapEntry {}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.last_touched.cmp(&other.last_touched)
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.last_touched == other.last_touched
    }
}

#[cfg(target_os = "linux")]
pub(super) fn cache_cleanup(cache: &mut PieceCacheInfo, adding: u64, batch_size: u64) -> Vec<FileSpan> {
    let predicted_used_space = cache.cache_size_cur + adding;
    if predicted_used_space < cache.cache_size_max {
        return Vec::new();
    }

    let mut discard: BinaryHeap<HeapEntry> = BinaryHeap::new();
    let mut discard_credit = batch_size as i64;

    for (k, v) in cache.pieces.iter() {
        while let Some(v) = discard.peek() {
            if discard_credit < 0 {
                discard_credit += i64::from(v.piece_length);
                drop(discard.pop().unwrap());
            } else {
                break;
            }
        }

        discard_credit -= i64::from(v.piece_length);
        discard.push(HeapEntry {
            last_touched: v.last_touched,
            piece_length: v.piece_length,
            btree_key: *k,
        });
    }

    let mut out = Vec::with_capacity(discard.len());

    for h in discard.into_vec() {
        let v = cache.pieces.remove(&h.btree_key).unwrap();

        cache.cache_size_cur -= u64::from(v.piece_length);
        out.push(FileSpan {
            start: v.position,
            length: u64::from(v.piece_length),
        });
    }

    out
}

#[cfg(target_os = "linux")]
pub(super) async fn punch_cache(cache: OwnedFd, punch_spans: Vec<FileSpan>) -> Result<(), nix::Error> {
    use std::os::unix::io::AsRawFd;

    use metrics::timing;

    let start = std::time::Instant::now();
    let mut sched_yield_time = Duration::new(0, 0);
    let mut byte_acc: u64 = 0;

    for punch in &punch_spans {
        let yield_start = std::time::Instant::now();
        tokio::task::yield_now().await;
        sched_yield_time += yield_start.elapsed();

        byte_acc += punch.length;
        let punch_start = std::time::Instant::now();
        let flags = FallocateFlags::FALLOC_FL_PUNCH_HOLE | FallocateFlags::FALLOC_FL_KEEP_SIZE;
        fallocate(
            cache.as_raw_fd(),
            flags,
            punch.start as i64,
            punch.length as i64,
        )?;

        timing!("diskcache.punch_time", punch_start.elapsed());
    }

    let punch_span_count = punch_spans.len() as u64;
    counter!("diskcache.punch_span_count", punch_span_count);
    counter!("diskcache.punch_bytes", byte_acc);

    if byte_acc > 0 {
        event!(
            Level::INFO,
            "punched {} bytes out with {} calls in {:?}, {:?} yielded",
            byte_acc,
            punch_spans.len(),
            start.elapsed(),
            sched_yield_time,
        );
    }

    Ok(())
}
