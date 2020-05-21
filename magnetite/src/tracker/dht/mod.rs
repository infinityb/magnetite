use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime};

use smallvec::SmallVec;

use magnetite_common::TorrentId;

mod wire;
mod tracker;
mod search;

// Please make this a power of two, since Vec::with_capacity aligns to
// powers of two anyway.  We're paying the cost of the memory anyway.
const BUCKET_SIZE: usize = 32;

const MAX_UNRESPONDED_QUERIES: i32 = 5;

const QUESTIONABLE_THRESH: Duration = Duration::from_secs(15 * 60);

// This prevents bucket prefix length overflows, which would lead to an attempt
// at out-of-bounds array access, panicking.  80 was picked arbitrarily.
const MAX_BUCKET_PREFIX_LENGTH: u32 = 80;

#[derive(Copy, Clone)]
enum AddressFamily {
    Ipv4,
    Ipv6,
}

struct Node {
    peer_id: TorrentId,
    peer_addr: SocketAddr,

    // time of last correct reply received
    last_message_time: Instant,
    // time of last correct reply received
    last_correct_reply_time: Instant,
    // time of last request
    last_request_sent_time: Instant,
    // how many requests we sent since last reply
    pinged: i32,
}

#[derive(Debug, Eq, PartialEq)]
enum NodeQuality {
    Good,
    Questionable,
    Bad,
}

impl NodeQuality {
    pub fn is_good(&self) -> bool {
        match *self {
            NodeQuality::Good => true,
            _ => false,
        }
    }

    pub fn is_questionable(&self) -> bool {
        match *self {
            NodeQuality::Questionable => true,
            _ => false,
        }
    }

    pub fn is_bad(&self) -> bool {
        match *self {
            NodeQuality::Bad => true,
            _ => false,
        }
    }
}

struct NodeEnvironment {
    now: Instant,
}

impl Node {
    fn new(peer_id: TorrentId, peer_addr: SocketAddr, env: &NodeEnvironment) -> Node {
        Node {
            peer_id,
            peer_addr,
            last_message_time: env.now,
            last_correct_reply_time: env.now,
            last_request_sent_time: env.now,
            pinged: 0,
        }
    }

    fn with_confirm(mut self, confirm: ConfirmLevel, env: &NodeEnvironment) -> Node {
        self.confirm(confirm, env);
        self
    }

    fn confirm(&mut self, confirm: ConfirmLevel, env: &NodeEnvironment) {
        if !confirm.is_empty() {
            self.last_message_time = env.now;
        }
        if confirm.is_responding() {
            self.last_correct_reply_time = env.now;
        }
    }

    fn quality(&self, env: &NodeEnvironment) -> NodeQuality {
        if (env.now - self.last_correct_reply_time) < QUESTIONABLE_THRESH {
            return NodeQuality::Good;
        }
        if MAX_UNRESPONDED_QUERIES <= self.pinged {
            return NodeQuality::Bad;
        }
        NodeQuality::Questionable
    }

    fn bump_activity(&mut self, env: &NodeEnvironment) {
        self.last_correct_reply_time = env.now;
    }

    fn bump_failure_count(&mut self) {
        self.pinged += 1;
    }
}

struct BucketManager {
    self_peer_id: TorrentId,
    buckets: BTreeMap<TorrentId, Bucket>,
}

impl BucketManager {
    /// may panic if limit is 0.
    fn find_close_nodes(
        &self,
        target: &TorrentId,
        limit: usize,
        env: &NodeEnvironment,
    ) -> Vec<&Node> {
        // chooseing an inefficient but obviously correct implemenation for now.

        struct HeapEntry<'a> {
            dist_key: TorrentId,
            node: &'a Node,
        }

        impl<'a> Eq for HeapEntry<'a> {}

        impl<'a> Ord for HeapEntry<'a> {
            fn cmp(&self, other: &Self) -> Ordering {
                self.dist_key.cmp(&other.dist_key)
            }
        }

        impl<'a> PartialOrd for HeapEntry<'a> {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl<'a> PartialEq for HeapEntry<'a> {
            fn eq(&self, other: &Self) -> bool {
                self.dist_key == other.dist_key
            }
        }

        let mut heap = BinaryHeap::with_capacity(limit);

        for b in self.buckets.values() {
            for n in &b.nodes {
                if !n.quality(env).is_good() {
                    continue;
                }

                let entry = HeapEntry {
                    dist_key: n.peer_id ^ *target,
                    node: n,
                };
                if limit <= heap.len() {
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
        }

        let mut out = Vec::with_capacity(heap.len());
        for entry in heap.drain() {
            out.push(entry.node);
        }

        out
    }

    fn find_bucket_mut_for_id<'man>(&'man mut self, id: &TorrentId) -> &'man mut Bucket {
        let mut bucket_counter = 0;
        let mut target_bucket = None;
        for b in self.buckets.values_mut() {
            if b.contains(id) {
                bucket_counter += 1;
                target_bucket = Some(b)
            }
        }

        assert_eq!(bucket_counter, 1);
        target_bucket.unwrap()
    }

    fn add_node(&mut self, node: Node, confirm: ConfirmLevel, env: &NodeEnvironment) {
        let self_peer_id = self.self_peer_id;
        let mut b = self.find_bucket_mut_for_id(&node.peer_id);

        while b.contains(&self_peer_id)
            && b.is_full()
            && b.prefix.prefix_len < MAX_BUCKET_PREFIX_LENGTH
        {
            // home bucket full - let's split.
            let new_bucket = b.split();
            drop(b);
            self.buckets.insert(new_bucket.prefix.base, new_bucket);
            b = self.find_bucket_mut_for_id(&node.peer_id);
        }

        b.add_node(node, confirm, env)
    }
}

struct Bucket {
    af: AddressFamily,
    prefix: TorrentIdPrefix,

    nodes: SmallVec<[Node; BUCKET_SIZE]>,
    last_touched_time: SystemTime,
    cached: (),
}

#[derive(Copy, Clone)]
struct TorrentIdPrefix {
    base: TorrentId,
    prefix_len: u32,
}

impl TorrentIdPrefix {
    fn contains(&self, id: &TorrentId) -> bool {
        self.prefix_len <= (self.base ^ *id).leading_zeros()
    }

    fn split(&mut self) -> TorrentIdPrefix {
        let (bytes, bits) = (self.prefix_len / 8, self.prefix_len % 8);
        self.prefix_len += 1;

        let mut new_base = self.base;
        let slice = new_base.as_mut_bytes();
        slice[bytes as usize] |= 0x80 >> bits;
        TorrentIdPrefix {
            base: new_base,
            prefix_len: self.prefix_len,
        }
    }
}

impl Bucket {
    fn new(af: AddressFamily) -> Bucket {
        Bucket {
            af: af,
            prefix: TorrentIdPrefix {
                base: TorrentId::zero(),
                prefix_len: 0,
            },
            nodes: Default::default(),
            last_touched_time: std::time::UNIX_EPOCH,
            cached: (),
        }
    }

    fn contains(&self, id: &TorrentId) -> bool {
        self.prefix.contains(id)
    }

    fn touch(&mut self) {
        self.last_touched_time = SystemTime::now();
    }

    fn is_full(&self) -> bool {
        assert!(self.nodes.len() <= BUCKET_SIZE);
        BUCKET_SIZE <= self.nodes.len()
    }

    fn new_node(
        &mut self,
        id: TorrentId,
        ss: SocketAddr,
        confirm: ConfirmLevel,
        env: &NodeEnvironment,
    ) {
        self.add_node(Node::new(id, ss, env), confirm, env)
    }

    fn add_node(&mut self, node: Node, confirm: ConfirmLevel, env: &NodeEnvironment) {
        assert!(self.contains(&node.peer_id));

        self.touch();

        if self.is_full() {
            let mut bad_node = None;
            for n in self.nodes.iter_mut() {
                if n.quality(env).is_bad() {
                    bad_node = Some(n);
                    break;
                }
            }
            if let Some(n) = bad_node {
                *n = node.with_confirm(confirm, env);
            }
        } else {
            self.nodes.push(node.with_confirm(confirm, env));
        }
    }

    fn split(&mut self) -> Bucket {
        let mut self_new_prefix = self.prefix;
        let other_prefix = self_new_prefix.split();

        let mut self_nodes: SmallVec<[Node; BUCKET_SIZE]> = Default::default();
        let mut new_bucket_nodes: SmallVec<[Node; BUCKET_SIZE]> = Default::default();

        for node in self.nodes.drain(..) {
            if self_new_prefix.contains(&node.peer_id) {
                self_nodes.push(node);
            } else {
                assert!(other_prefix.contains(&node.peer_id));
                new_bucket_nodes.push(node);
            }
        }

        self.prefix = self_new_prefix;
        for node in self_nodes.drain(..) {
            self.nodes.push(node);
        }

        Bucket {
            af: self.af,
            prefix: other_prefix,
            last_touched_time: self.last_touched_time,
            nodes: new_bucket_nodes,
            cached: (),
        }
    }
}

#[derive(Clone, Copy)]
enum ConfirmLevel {
    /// Haven't had any response
    Empty,

    /// Got a message from the node
    GotMessage,

    /// The node responded to one of our requests
    Responding,
}

impl ConfirmLevel {
    fn is_empty(self) -> bool {
        match self {
            ConfirmLevel::Empty => true,
            ConfirmLevel::GotMessage => false,
            ConfirmLevel::Responding => false,
        }
    }

    fn is_responding(self) -> bool {
        match self {
            ConfirmLevel::Empty => false,
            ConfirmLevel::GotMessage => false,
            ConfirmLevel::Responding => true,
        }
    }
}

fn start_service() -> DhtHandle {
    let mut high_p_queue = VecDeque::new();
    let mut low_p_queue = VecDeque::new();
    
    let mut running_searches: HashMap<TorrentId, RunningSearch> = Default::default();
    let mut tracker: HashMap<TorrentId, TrackerTorrentState> = Default::default();

    //
    
}

#[cfg(test)]
mod tests {
    use super::{TorrentId, TorrentIdPrefix};

    #[test]
    fn test_id_split() {
        let mut prefix = TorrentIdPrefix {
            base: TorrentId::zero(),
            prefix_len: 0,
        };

        let split_one = prefix.split();
        assert_eq!(
            split_one.base,
            TorrentId([
                0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ])
        );

        let mut split_two = prefix.split();
        assert_eq!(
            split_two.base,
            TorrentId([
                0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ])
        );

        let split_three = split_two.split();
        assert_eq!(
            split_three.base,
            TorrentId([
                0x60, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ])
        );
    }
}
