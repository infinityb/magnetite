use std::cmp::Ordering;
use std::collections::BinaryHeap;

pub struct GeneralHeapEntry<K, T>
where
    K: Ord,
{
    pub dist_key: K,
    pub value: T,
}

pub fn general_heap_entry_push_or_replace<K, T>(
    heap: &mut BinaryHeap<GeneralHeapEntry<K, T>>,
    max_length: usize,
    entry: GeneralHeapEntry<K, T>,
) where
    K: Ord,
{
    if heap.len() < max_length {
        heap.push(entry);
    } else {
        // current entry is better than the worst node, in our heap
        // replace the worst node with this new better node.
        if entry.dist_key < heap.peek().unwrap().dist_key {
            heap.pop().unwrap();
            heap.push(entry);
        }
    }
}

impl<K, T> Eq for GeneralHeapEntry<K, T> where K: Ord {}

impl<K, T> Ord for GeneralHeapEntry<K, T>
where
    K: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.dist_key.cmp(&other.dist_key).reverse()
    }
}

impl<K, T> PartialOrd for GeneralHeapEntry<K, T>
where
    K: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, T> PartialEq for GeneralHeapEntry<K, T>
where
    K: Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.dist_key == other.dist_key
    }
}
