use std::fmt;
use std::cmp::Ordering;
use std::collections::btree_map::{Entry, VacantEntry};
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::hash::{BuildHasher, Hasher};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use futures::channel::oneshot;
use rand::{thread_rng, Rng};
use smallvec::SmallVec;
use tracing::{event, Level};

use magnetite_common::TorrentId;

mod search;

use crate::tracker::Tracker;
use crate::wire::{DhtMessage, DhtMessageData, DhtMessageQuery, DhtMessageQueryGetPeers};

pub mod tracker;
pub mod wire;

// Please make this a power of two, since Vec::with_capacity aligns to
// powers of two so we're paying the cost of the memory anyway.
pub const BUCKET_SIZE: usize = 1 << 3;

pub const RECURSION_CACHED_NODE_COUNT: usize = BUCKET_SIZE << 2;

pub const TRANSACTION_SIZE_BYTES: usize = 8;

const BUCKET_EXCLUSION_COUNT: usize = 2;

const MAX_UNRESPONDED_QUERIES: i32 = 5;

const QUESTIONABLE_THRESH: Duration = Duration::from_secs(15 * 60);

#[derive(Copy, Clone)]
enum AddressFamily {
    Ipv4,
    Ipv6,
}

#[derive(Debug)]
pub struct ThinNode {
    pub id: TorrentId,
    pub saddr: SocketAddr,
}

pub struct RecursionState {
    self_id: TorrentId,
    target_info_hash: TorrentId,
    nodes: VecDeque<ThinNode>,
}

impl RecursionState {
    pub fn new(target: TorrentId, bm: &BucketManager, env: &NodeEnvironment) -> RecursionState {
        type HeapEntry = TorrentIdHeapEntry<ThinNode>;

        let mut heap = BinaryHeap::with_capacity(RECURSION_CACHED_NODE_COUNT);

        for b in &bm.buckets {
            for n in &b.nodes {
                if !n.quality(env).is_good() {
                    continue;
                }

                general_heap_entry_push_or_replace(
                    &mut heap,
                    RECURSION_CACHED_NODE_COUNT,
                    HeapEntry {
                        dist_key: !(n.peer_id ^ target),
                        value: ThinNode {
                            id: n.peer_id,
                            saddr: n.peer_addr,
                        },
                    },
                );
            }
        }

        RecursionState {
            self_id: bm.self_peer_id,
            target_info_hash: target,
            nodes: heap.into_iter().map(|e| e.value).collect(),
        }
    }

    pub fn get_work(&mut self) -> Option<(ThinNode, DhtMessage)> {
        let node = self.nodes.pop_front()?;
        let msg = DhtMessage {
            transaction: Vec::new(),
            data: DhtMessageData::Query(DhtMessageQuery::GetPeers(DhtMessageQueryGetPeers {
                id: self.self_id,
                info_hash: self.target_info_hash,
            })),
        };
        Some((node, msg))
    }

    pub fn add_candidate(&mut self, node: ThinNode) {
        self.nodes.push_back(node);
    }
}

type TorrentIdHeapEntry<T> = GeneralHeapEntry<TorrentId, T>;

#[derive(Debug, Clone)]
pub struct Node {
    pub peer_id: TorrentId,
    pub peer_addr: SocketAddr,

    // time of last valid message received
    pub last_message_time: Instant,
    // none if we've never queried this node.
    pub reply_stats: Option<NodeReplyStats>,
}

#[derive(Debug, Clone)]
pub struct NodeReplyStats {
    // time of last sent request
    pub last_request_sent_time: Instant,
    // time of last correct reply received
    pub last_correct_reply_time: Option<Instant>,
    // how many requests we sent since last reply
    pub pinged: i32,
}

pub enum NodeActivityCommand {
    ReceiveRequest,
    ReceiveResponse,
    SendRequest,
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

#[derive(Debug)]
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
            reply_stats: None,
        }
    }

    pub fn quality(&self, env: &NodeEnvironment) -> NodeQuality {
        if let Some(ref stats) = self.reply_stats {
            if MAX_UNRESPONDED_QUERIES <= stats.pinged {
                return NodeQuality::Bad;
            }
            if let Some(lcrt) = stats.last_correct_reply_time {
                if env.now - lcrt < QUESTIONABLE_THRESH {
                    return NodeQuality::Good;
                }
            }
        }
        NodeQuality::Questionable
    }

    pub fn apply_activity_send_request(&mut self, env: &NodeEnvironment) {
        if self.reply_stats.is_none() {
            self.reply_stats = Some(NodeReplyStats {
                last_request_sent_time: env.now,
                last_correct_reply_time: None,
                pinged: 0,
            });
        }
        let rs = self.reply_stats.as_mut().unwrap();
        rs.last_request_sent_time = env.now;
        rs.pinged += 1;
    }

    pub fn apply_activity_receive_request(&mut self, env: &NodeEnvironment) {
        self.last_message_time = env.now;
    }

    pub fn apply_activity_receive_response(&mut self, env: &NodeEnvironment) {
        self.last_message_time = env.now;
        if let Some(ref mut rs) = self.reply_stats {
            rs.last_correct_reply_time = Some(env.now);
            rs.pinged = 0;
        } else {
            // received a valid response to an unknown node, so we fake the sent time.
            // this is needed because we don't have a node record before we do the initial
            // query for an ID (node record requires the ID)
            self.reply_stats = Some(NodeReplyStats {
                last_request_sent_time: env.now,
                last_correct_reply_time: Some(env.now),
                pinged: 0,
            });
        }
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
    pub get_peers_search: RandomState,
    pub transactions: BTreeMap<u64, Box<TransactionState>>,
    pub tracker: Tracker,
}

pub struct TransactionState {
    pub expiration: Instant,
    pub query: wire::DhtMessage,
    pub from_expected_peer: SocketAddr,
    pub completion_port: oneshot::Sender<Box<TransactionCompletion>>,
}

impl TransactionState {
    fn is_expired(&self, now: &Instant) -> bool {
        self.expiration <= *now
    }
}

impl std::fmt::Debug for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        struct Ellipses;

        impl std::fmt::Debug for Ellipses {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "...")
            }
        }

        f.debug_struct("TransactionState")
            .field("query", &self.query)
            .field("from_expected_peer", &self.from_expected_peer)
            .field("completion_port", &Ellipses)
            .finish()
    }
}

#[derive(Debug)]
pub struct TransactionCompletion {
    pub peer_saddr: SocketAddr,
    pub query: wire::DhtMessage,
    pub response: Result<wire::DhtMessageResponse, ()>,
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
            get_peers_search: RandomState::new(),
            transactions: BTreeMap::new(),
            tracker: Tracker::new(),
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
            return true;
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
        type HeapEntry<'a> = TorrentIdHeapEntry<&'a Node>;

        let mut heap = BinaryHeap::with_capacity(limit);

        for b in self.buckets.iter() {
            for n in &b.nodes {
                if !n.quality(env).is_good() {
                    continue;
                }
                general_heap_entry_push_or_replace(
                    &mut heap,
                    limit,
                    HeapEntry {
                        // max-heap so get the bitwise not of the xor-distance
                        dist_key: !(n.peer_id ^ *target),
                        value: n,
                    },
                );
            }
        }

        let mut out = Vec::with_capacity(heap.len());
        for entry in heap.drain() {
            out.push(entry.value);
        }

        out
    }

    fn find_bucket_mut_for_insertion_by_id<'man>(
        &'man mut self,
        id: &TorrentId,
        env: &NodeEnvironment,
    ) -> &'man mut Bucket {
        // because these are ordered by longest prefix first, we can find the best
        // match by finding the first match and then breaking, relooping for saturated
        // buckets.

        let mut idx = 0;
        let mut found = false;
        for (i, b) in self.buckets.iter().enumerate() {
            if b.prefix.contains(id) && !b.is_saturated(env) {
                idx = i;
                found = true;
            }
        }
        if !found {
            for (i, b) in self.buckets.iter().enumerate() {
                if b.prefix.contains(id) {
                    idx = i;
                    found = true;
                }
            }
        }

        if !found {
            unreachable!("bucket manager not covering entire keyspace");
        }

        &mut self.buckets[idx]
    }

    pub fn node_seen(&mut self, node: &ThinNode, env: &NodeEnvironment) {
        event!(Level::TRACE, "BucketManager::node_seen(..., {:?}, {:?})", node, env);
        let self_peer_id = self.self_peer_id;
        let b = self.find_bucket_mut_for_insertion_by_id(&node.id, env);

        let mut new_buckets = Vec::new();
        let mut new_bucket_slot_ref = None;
        if b.prefix.contains(&self_peer_id) {
            new_bucket_slot_ref = Some(&mut new_buckets);
        }

        b.node_seen(&self_peer_id, node, env, new_bucket_slot_ref);

        if !new_buckets.is_empty() {
            self.buckets.extend(new_buckets.into_iter());

            // ordered by base and then longest prefix (smallest interval) descending.
            // this way, the first match is our best match.
            self.buckets
                .sort_by_key(|bm| (bm.prefix.base, !0 - bm.prefix.prefix_len));
        }
    }

    pub fn clean_expired_transactions(&mut self, now: &Instant) {
        let mut remove_keys: SmallVec<[u64; 20]> = Default::default();
        for (k, v) in &self.transactions {
            if v.is_expired(now) {
                remove_keys.push(*k);
            }
        }
        for k in &remove_keys {
            if let Some(tx) = self.transactions.remove(k) {
                let res = tx.completion_port.send(Box::new(TransactionCompletion {
                    peer_saddr: tx.from_expected_peer,
                    query: tx.query,
                    response: Err(()),
                }));
                if let Err(err) = res {
                    event!(Level::INFO, "failed to send on completion port: {:?}", err);
                }
            }
        }
        remove_keys.clear();
    }

    pub fn clean_expired_transaction(&mut self, txid: u64) {
        if let Some(tx) = self.transactions.remove(&txid) {
            let res = tx.completion_port.send(Box::new(TransactionCompletion {
                peer_saddr: tx.from_expected_peer,
                query: tx.query,
                response: Err(()),
            }));
            if let Err(err) = res {
                event!(Level::INFO, "failed to send on completion port: {:?}", err);
            }
        }
    }

    pub fn handle_incoming_packet(&mut self, message: &wire::DhtMessage, from: SocketAddr) -> bool {
        if message.transaction.len() != 8 {
            return false;
        }

        let resp;
        if let wire::DhtMessageData::Response(ref resp_tmp) = message.data {
            resp = resp_tmp;
        } else {
            return false;
        }

        let mut buf = [0; 8];
        buf.copy_from_slice(&message.transaction);
        let txid = u64::from_be_bytes(buf);

        if let Some(tx) = self.transactions.remove(&txid) {
            if from != tx.from_expected_peer {
                eprintln!(
                    "TX-{:016x} peer invalid: {} != {}",
                    txid, tx.from_expected_peer, from
                );
            }

            let res = tx.completion_port.send(Box::new(TransactionCompletion {
                peer_saddr: tx.from_expected_peer,
                query: tx.query,
                response: Ok(resp.clone()),
            }));

            if let Err(err) = res {
                event!(Level::INFO, "failed to send on completion port: {:?}", err);
            }

            true
        } else {
            false
        }
    }

    pub fn acquire_transaction_slot<'a>(&'a mut self) -> TransactionSlot<'a> {
        let mut rng = thread_rng();
        let txid;
        loop {
            let txid_candidate: u64 = rng.gen();
            if self.transactions.get(&txid_candidate).is_none() {
                txid = txid_candidate;
                break;
            }
        }
        match self.transactions.entry(txid) {
            Entry::Vacant(v) => TransactionSlot { entry: v },
            _ => unreachable!(),
        }
    }
}

pub struct TransactionSlot<'a> {
    entry: VacantEntry<'a, u64, Box<TransactionState>>,
}

impl<'a> TransactionSlot<'a> {
    pub fn key(&self) -> u64 {
        *self.entry.key()
    }

    pub fn assign(
        self,
        message: &mut wire::DhtMessage,
        to: SocketAddr,
        now: &Instant,
    ) -> oneshot::Receiver<Box<TransactionCompletion>> {
        let (tx, rx) = oneshot::channel();
        message.transaction = self.entry.key().to_be_bytes().to_vec();
        self.entry.insert(Box::new(TransactionState {
            expiration: *now + Duration::new(60, 0),
            query: message.clone(),
            from_expected_peer: to,
            completion_port: tx,
        }));

        rx
    }
}

pub struct Bucket {
    // af: AddressFamily,
    pub prefix: TorrentIdPrefix,
    pub nodes: SmallVec<[Node; BUCKET_SIZE]>,
    pub last_touched_time: Instant,
}

impl fmt::Debug for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let age = self.last_touched_time.elapsed();
        f.debug_struct("Bucket")
            .field("prefix", &self.prefix)
            .field("node_count", &self.nodes.len())
            .field("last_touched_age", &age)
            .field("nodes", &self.nodes)
            .finish()
    }
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

#[test]
fn split_foo() {
    let move_zig = TorrentIdPrefix { base: TorrentId(*b"\xe8D\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), prefix_len: 14 };
    let move_zig_ch_left = TorrentIdPrefix { base: TorrentId(*b"\xe8D\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), prefix_len: 15 };
    let move_zig_ch_right = TorrentIdPrefix { base: TorrentId(*b"\xe8F\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), prefix_len: 15 };
    let test = TorrentId(*b"\xe8F4T\x1e\x98Q\xbduF$d\0\0\0\0\0\0\0\0");
    assert!(!move_zig_ch_left.contains(&test));
    assert!(move_zig_ch_right.contains(&test));
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

    fn split(&self) -> (TorrentIdPrefix, TorrentIdPrefix) {
        let (bytes, bits) = (self.prefix_len / 8, self.prefix_len % 8);
        let mut new_base = self.base;
        let slice = new_base.as_mut_bytes();
        slice[bytes as usize] |= 0x80 >> bits;

        (
            TorrentIdPrefix {
                base: self.base,
                prefix_len: self.prefix_len + 1,
            },
            TorrentIdPrefix {
                base: new_base,
                prefix_len: self.prefix_len + 1,
            },
        )
    }

    fn split_swap(&self, target: &TorrentId) -> (TorrentIdPrefix, TorrentIdPrefix) {
        assert!(self.contains(target));
        let (left, right) = self.split();
        if left.contains(target) {
            (left, right)
        } else {
            (right, left)
        }
    }

    // fn split(&mut self) -> TorrentIdPrefix {
    //     let (bytes, bits) = (self.prefix_len / 8, self.prefix_len % 8);
    //     self.prefix_len += 1;
    //     let mut new_base = self.base;
    //     let slice = new_base.as_mut_bytes();
    //     slice[bytes as usize] |= 0x80 >> bits;
    //     TorrentIdPrefix {
    //         base: new_base,
    //         prefix_len: self.prefix_len,
    //     }
    // }
}

impl Bucket {
    fn touch(&mut self, env: &NodeEnvironment) {
        self.last_touched_time = env.now;
    }

    fn is_full(&self) -> bool {
        assert!(self.nodes.len() <= BUCKET_SIZE);
        BUCKET_SIZE <= self.nodes.len()
    }

    /// Iff we're full of good nodes, we're saturated.
    fn is_saturated(&self, env: &NodeEnvironment) -> bool {
        if !self.is_full() {
            return false;
        }

        for n in &self.nodes {
            if !n.quality(env).is_good() {
                return false;
            }
        }

        true
    }

    fn split_bucket_and_add(
        &mut self,
        peer_self_id: &TorrentId,
        seen_node: &ThinNode,
        env: &NodeEnvironment,
        // is Some if this is our home bucket.
        new_bucket: &mut Vec<Bucket>,
    ) {
        let mut nn = Node {
            peer_id: seen_node.id,
            peer_addr: seen_node.saddr,
            last_message_time: env.now,
            reply_stats: None,
        };
        if env.is_reply {
            nn.apply_activity_receive_response(env);
        } else {
            nn.apply_activity_receive_request(env);
        }

        while self.is_saturated(env) && self.prefix.contains(&seen_node.id) {
            let (new_self_prefix, other_prefix) = self.prefix.split_swap(peer_self_id);
            // event!(Level::TRACE, "ITERATION -- {:?} => {:?} + {:?}", self.prefix, new_self_prefix, other_prefix);
            let mut new_self_nodes: SmallVec<[Node; BUCKET_SIZE]> = Default::default();
            let mut other_bucket_nodes: SmallVec<[Node; BUCKET_SIZE]> = Default::default();

            for node in self.nodes.drain(..) {
                // event!(Level::TRACE, "    Checking node: {:?}", node.peer_id);
                // event!(Level::TRACE, "        -? within {:?} :: {}", new_self_prefix, new_self_prefix.contains(&node.peer_id));
                // event!(Level::TRACE, "        -? within {:?} :: {}", other_prefix, other_prefix.contains(&node.peer_id));

                if new_self_prefix.contains(&node.peer_id) {
                    new_self_nodes.push(node);
                } else {
                    assert!(other_prefix.contains(&node.peer_id));
                    other_bucket_nodes.push(node);
                }
            }

            self.prefix = new_self_prefix;
            self.nodes = new_self_nodes;

            if other_prefix.contains(&seen_node.id) {
                // other_prefix is definitely full of good nodes otherwise we wouldn't
                // have been trying to split, so we can just discard the node if we are
                // full.
                if other_bucket_nodes.len() < 8 {
                    other_bucket_nodes.push(nn);
                }

                new_bucket.push(Bucket {
                    prefix: other_prefix,
                    last_touched_time: self.last_touched_time,
                    nodes: other_bucket_nodes,
                });

                return;
            } else {
                new_bucket.push(Bucket {
                    prefix: other_prefix,
                    last_touched_time: self.last_touched_time,
                    nodes: other_bucket_nodes,
                });
            }
        }

        assert!(self.prefix.contains(&nn.peer_id));
        self.nodes.push(nn);
    }

    fn node_seen(
        &mut self,
        peer_self_id: &TorrentId,
        seen_node: &ThinNode,
        env: &NodeEnvironment,
        // is Some if this is our home bucket.
        new_bucket: Option<&mut Vec<Bucket>>,
    ) {
        assert!(self.prefix.contains(&seen_node.id));
        self.touch(env);

        for n in self.nodes.iter_mut() {
            if n.peer_id == seen_node.id && n.peer_addr == seen_node.saddr {
                if env.is_reply {
                    n.apply_activity_receive_response(env);
                } else {
                    n.apply_activity_receive_request(env);
                }
                return;
            } else if n.peer_id == seen_node.id {
                // FIXME: drop incorrect source address.
                return;
            }
        }

        if self.is_saturated(env) && new_bucket.is_some() {
            let new_bucket = new_bucket.unwrap();
            self.split_bucket_and_add(peer_self_id, seen_node, env, new_bucket);
        } else if !self.is_saturated(env) {
            // not saturated so we can replace a bad node or add.
            let mut nn = Node {
                peer_id: seen_node.id,
                peer_addr: seen_node.saddr,
                last_message_time: env.now,
                reply_stats: None,
            };
            if env.is_reply {
                nn.apply_activity_receive_response(env);
            } else {
                nn.apply_activity_receive_request(env);
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
                    *n = nn;
                }
            } else {
                self.nodes.push(nn);
            }
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

pub struct DhtCommandAddPeer {
    pub addr: std::net::SocketAddrV4,
}

pub enum DhtCommand {
    AddPeer(DhtCommandAddPeer),
    GetPeers(),
}

struct GeneralHeapEntry<K, T>
where
    K: Ord,
{
    dist_key: K,
    value: T,
}

fn general_heap_entry_push_or_replace<K, T>(
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

        let (new_pref, split_one) = prefix.split();
        prefix = new_pref;
        assert_eq!(
            split_one.base,
            TorrentId([
                0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ])
        );

        let (new_pref, split_two) = prefix.split();
        prefix = new_pref;
        assert_eq!(
            split_two.base,
            TorrentId([
                0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ])
        );

        let (new_pref, split_three) = split_two.split();
        prefix = new_pref;
        assert_eq!(
            split_three.base,
            TorrentId([
                0x60, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ])
        );
    }
}
