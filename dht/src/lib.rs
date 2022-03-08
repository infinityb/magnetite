use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::{BTreeMap, BinaryHeap};
use std::future::Future;
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime};
use std::collections::hash_map::RandomState;
use std::hash::{Hasher, BuildHasher};

use rand::rngs::OsRng;
use rand::RngCore;
use smallvec::SmallVec;

use magnetite_common::TorrentId;

mod search;
mod tracker;
pub mod wire;

// Please make this a power of two, since Vec::with_capacity aligns to
// powers of two so we're paying the cost of the memory anyway.
pub const BUCKET_SIZE: usize = 1 << 3;

pub const RECURSION_CACHED_NODE_COUNT: usize = BUCKET_SIZE;

pub const TRANSACTION_SIZE_BYTES: usize = 8;

const BUCKET_EXCLUSION_COUNT: usize = 2;

const MAX_UNRESPONDED_QUERIES: i32 = 5;

const QUESTIONABLE_THRESH: Duration = Duration::from_secs(15 * 60);

#[derive(Copy, Clone)]
enum AddressFamily {
    Ipv4,
    Ipv6,
}

pub struct RecursionState {
    nodes: BinaryHeap<TorrentIdHeapEntry<Node>>,
}

impl RecursionState {
    pub fn new(target: TorrentId, bm: &BucketManager, env: &NodeEnvironment) -> RecursionState {
        type HeapEntry = TorrentIdHeapEntry<Node>;

        let mut heap = BinaryHeap::with_capacity(RECURSION_CACHED_NODE_COUNT);

        for b in &bm.buckets {
            for n in &b.nodes {
                if !n.quality(env).is_good() {
                    continue;
                }

                let entry = HeapEntry {
                    dist_key: n.peer_id ^ target,
                    value: n.clone(),
                };

                if heap.len() < RECURSION_CACHED_NODE_COUNT {
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

        // self.searches.insert(target, heap);

        unimplemented!();
    }

    pub fn get_work(&mut self) -> () {
        //
    }

    pub fn start_recursion(&self, target: TorrentId, bm: &BucketManager, env: &NodeEnvironment) {
        
    }

    pub fn add_node(&mut self, x: ()) {
       
    }
}

struct TorrentIdHeapEntry<T> {
    dist_key: TorrentId,
    value: T,
}

impl<T> Eq for TorrentIdHeapEntry<T> {}

impl<T> Ord for TorrentIdHeapEntry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.dist_key.cmp(&other.dist_key)
    }
}

impl<T> PartialOrd for TorrentIdHeapEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq for TorrentIdHeapEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.dist_key == other.dist_key
    }
}


#[derive(Debug, Clone)]
pub struct Node {
    pub peer_id: TorrentId,
    pub peer_addr: SocketAddr,

    // time of last correct reply received
    pub last_message_time: Instant,
    // time of last correct reply received
    pub last_correct_reply_time: Instant,
    // time of last request
    pub last_request_sent_time: Instant,
    // how many requests we sent since last reply
    pub pinged: i32,
}

impl Node {
    pub fn debug(&self) -> NodeDebug {
        NodeDebug { node: self }
    }
}

pub struct NodeDebug<'a> {
    node: &'a Node,
}

impl<'a> std::fmt::Debug for NodeDebug<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}@{:?}", self.node.peer_id.hex(), self.node.peer_addr)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum NodeQuality {
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

pub struct NodeEnvironment {
    pub now: Instant,
    pub is_reply: bool,
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

    pub fn quality(&self, env: &NodeEnvironment) -> NodeQuality {
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

#[derive(Debug)]
pub struct BucketManager {
    pub self_peer_id: TorrentId,
    pub buckets: Vec<Bucket>,
    pub recent_dead_hosts: (),

    pub token_rs_next_incr: Instant,
    pub token_rs_current: RandomState,
    pub token_rs_previous: RandomState,
}

impl BucketManager {
    pub fn new(self_peer_id: TorrentId) -> BucketManager {
        let mut buckets = Vec::new();
        let mut now = Instant::now();

        buckets.push(Bucket {
            prefix: TorrentIdPrefix {
                base: TorrentId::zero(),
                prefix_len: 0,
            },
            exclusions: Default::default(),
            nodes: SmallVec::new(),
            last_touched_time: now,
        });

        let mut rs = RandomState::new();
        BucketManager {
            self_peer_id,
            buckets,
            recent_dead_hosts: (),
            token_rs_next_incr: now + Duration::new(300, 0),
            token_rs_current: rs.clone(),
            token_rs_previous: rs,
        }
    }

    pub fn tick(&mut self, env: &NodeEnvironment) {
        if env.now < self.token_rs_next_incr {
            self.token_rs_previous = self.token_rs_current.clone();
            self.token_rs_current = RandomState::new();
            self.token_rs_next_incr = env.now + Duration::new(300, 0);
        }
    }

    pub fn generate_token(&self, client_addr: &SocketAddr) -> Vec<u8> {
        let ipv4_buf;
        let ipv6_buf;
        let ip_buf;

        match client_addr {
            SocketAddr::V4(v4) => {
                ipv4_buf = v4.ip().octets();
                ip_buf = &ipv4_buf[..];
            }
            SocketAddr::V6(v6) => {
                ipv6_buf = v6.ip().octets();
                ip_buf = &ipv6_buf[..];
            }
        }

        let mut hasher = self.token_rs_current.build_hasher();
        hasher.write(&ip_buf[..]);
        hasher.finish().to_be_bytes().to_vec()
    }

    pub fn check_token(&self, token: &[u8], client_addr: &SocketAddr) -> bool {
        let ipv4_buf;
        let ipv6_buf;
        let ip_buf;

        match client_addr {
            SocketAddr::V4(v4) => {
                ipv4_buf = v4.ip().octets();
                ip_buf = &ipv4_buf[..];
            }
            SocketAddr::V6(v6) => {
                ipv6_buf = v6.ip().octets();
                ip_buf = &ipv6_buf[..];
            }
        }

        let mut hasher = self.token_rs_current.build_hasher();
        hasher.write(&ip_buf[..]);
        let token_candidate = hasher.finish().to_be_bytes();
        if token == &token_candidate[..] {
            return true
        }

        let mut hasher = self.token_rs_previous.build_hasher();
        hasher.write(&ip_buf[..]);
        let token_candidate = hasher.finish().to_be_bytes();
        token == &token_candidate[..]
    }

    /// may panic if limit is 0.
    pub fn find_close_nodes(
        &self,
        target: &TorrentId,
        limit: usize,
        env: &NodeEnvironment,
    ) -> Vec<&Node> {
        // choosing an inefficient but obviously correct implementation for now.

        type HeapEntry<'a> = TorrentIdHeapEntry<&'a Node>;

        let mut heap = BinaryHeap::with_capacity(limit);

        for b in self.buckets.iter() {
            for n in &b.nodes {
                if !n.quality(env).is_good() {
                    continue;
                }

                let entry = HeapEntry {
                    dist_key: n.peer_id ^ *target,
                    value: n,
                };
                if heap.len() < limit {
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
            out.push(entry.value);
        }

        out
    }

    fn find_bucket_mut_for_id<'man>(&'man mut self, id: &TorrentId) -> &'man mut Bucket {
        let mut bucket_counter = 0;

        let mut target_bucket = None;
        for b in self.buckets.iter_mut() {
            if b.contains(id) {
                bucket_counter += 1;
                target_bucket = Some(b)
            }
        }

        assert_eq!(
            bucket_counter, 1,
            "must have found one bucket, we always cover the entire keyspace"
        );
        target_bucket.unwrap()
    }

    pub fn add_node(&mut self, node: Node, confirm: ConfirmLevel, env: &NodeEnvironment) {
        let self_peer_id = self.self_peer_id;
        let mut b = self.find_bucket_mut_for_id(&node.peer_id);

        while b.contains(&self_peer_id) && b.would_split(&node, env) {
            // home bucket full - let's split.
            let new_bucket = b.split();

            drop(b); // unborrow self.buckets for insertion

            self.buckets.push(new_bucket);
            self.buckets.sort_by_key(|bm| bm.prefix.base);
            b = self.find_bucket_mut_for_id(&node.peer_id);
        }

        b.add_node(node, confirm, env)
    }
}

#[derive(Debug)]
pub struct Bucket {
    // af: AddressFamily,
    pub prefix: TorrentIdPrefix,
    // these prefixes are held elsewhere, potentially because of running
    // recursions.
    pub exclusions: SmallVec<[TorrentIdPrefix; BUCKET_EXCLUSION_COUNT]>,
    pub nodes: SmallVec<[Node; BUCKET_SIZE]>,
    pub last_touched_time: Instant,
}

#[derive(Copy, Clone, Debug)]
pub struct TorrentIdPrefix {
    pub base: TorrentId,
    pub prefix_len: u32,
}


#[test]
fn foobar() {
    let first_half = TorrentIdPrefix {
        base: TorrentId::zero(),
        prefix_len: 1,
    };

    let whole_set = TorrentIdPrefix {
        base: TorrentId::zero(),
        prefix_len: 0,
    };

    assert!(first_half.is_proper_subset_of(&whole_set));
    assert_eq!(false, whole_set.is_proper_subset_of(&first_half));
    assert_eq!(false, whole_set.is_proper_subset_of(&whole_set));
}


impl TorrentIdPrefix {
    fn contains(&self, id: &TorrentId) -> bool {
        self.prefix_len <= (self.base ^ *id).leading_zeros()
    }

    fn is_proper_subset_of(&self, other: &TorrentIdPrefix) -> bool {
        if other.prefix_len <= self.prefix_len {
            return false;
        }

        unimplemented!();
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
    fn new(/*af: AddressFamily*/) -> Bucket {
        Bucket {
            // af: af,
            prefix: TorrentIdPrefix {
                base: TorrentId::zero(),
                prefix_len: 0,
            },
            exclusions: Default::default(),
            nodes: Default::default(),
            last_touched_time: Instant::now(),
        }
    }

    fn contains(&self, id: &TorrentId) -> bool {
        self.prefix.contains(id)
    }

    fn touch(&mut self, env: &NodeEnvironment) {
        self.last_touched_time = env.now;
    }

    fn is_full(&self) -> bool {
        assert!(self.nodes.len() <= BUCKET_SIZE);
        BUCKET_SIZE <= self.nodes.len()
    }

    fn would_split(&self, node: &Node, env: &NodeEnvironment) -> bool {
        if !self.is_full() {
            return false;
        }

        unimplemented!();
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
        for e in &self.exclusions {
            assert!(!e.contains(&node.peer_id));
        }

        self.touch(env);

        for n in self.nodes.iter_mut() {
            if n.peer_id == node.peer_id && n.peer_addr == node.peer_addr {
                n.last_message_time = node.last_message_time;
                if env.is_reply {
                    n.last_correct_reply_time = node.last_message_time;
                    n.pinged = 0;
                }
                return;
            } else if n.peer_id == node.peer_id {
                // FIXME: drop incorrect source address.
                return;
            }
        }

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
        let mut new_bucket_exclusions: SmallVec<[TorrentIdPrefix; BUCKET_EXCLUSION_COUNT]> = Default::default();

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
            // af: self.af,
            prefix: other_prefix,
            exclusions: Default::default(),
            last_touched_time: self.last_touched_time,
            nodes: new_bucket_nodes,
        }
    }
}

#[derive(Clone, Copy)]
pub enum ConfirmLevel {
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

// fn start_service() -> DhtHandle {
//     let mut high_p_queue = VecDeque::new();
//     let mut low_p_queue = VecDeque::new();
//     let mut running_searches: HashMap<TorrentId, RunningSearch> = Default::default();
//     let mut tracker: HashMap<TorrentId, TrackerTorrentState> = Default::default();
//     //
// }

pub struct DhtCommandAddPeer {
    pub addr: std::net::SocketAddrV4,
}

pub enum DhtCommand {
    AddPeer(DhtCommandAddPeer),
    GetPeers(),
}

pub fn start_service(
    mut rx: tokio::sync::mpsc::Receiver<DhtCommand>,
) -> impl Future<Output = Result<(), anyhow::Error>> {
    // 172.105.96.16:3019

    let mut tid = TorrentId::zero();
    OsRng.fill_bytes(&mut tid.0[..]);
    let mut buckets = BucketManager::new(tid);

    // let mut high_p_queue = VecDeque::new();
    // let mut low_p_queue = VecDeque::new();

    // let mut buf = [0; 4096];

    async move {
        // while let Some(cmd) = rx.recv().await {
        //     match cmd {
        //         DhtCommand::AddPeer(..) => {
        //             //
        //         },
        //         DhtCommand::GetPeers(..) => {
        //             //
        //         },
        //     }
        // }
        loop {}
        Ok(())
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
struct BinStr<'a>(pub &'a [u8]);

impl std::fmt::Debug for BinStr<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "b\"")?;
        for &b in self.0 {
            match b {
                b'\0' => write!(f, "\\0")?,
                b'\n' => write!(f, "\\n")?,
                b'\r' => write!(f, "\\r")?,
                b'\t' => write!(f, "\\t")?,
                b'\\' => write!(f, "\\\\")?,
                b'"' => write!(f, "\\\"")?,
                _ if 0x20 <= b && b < 0x7F => write!(f, "{}", b as char)?,
                _ => write!(f, "\\x{:02x}", b)?,
            }
        }
        write!(f, "\"")?;
        Ok(())
    }
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
