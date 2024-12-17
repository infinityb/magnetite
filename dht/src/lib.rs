use std::fmt;
use std::cmp::Ordering;
use std::collections::hash_map::HashMap;
use std::collections::btree_map::{self, Entry, VacantEntry};
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::hash::{BuildHasher, Hasher};
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};
use std::collections::BTreeSet;

use anyhow::anyhow;
use futures::channel::oneshot;
use rand::{thread_rng, Rng};
use smallvec::SmallVec;
use tracing::{event, Level};

use magnetite_common::{TorrentId, TorrentIdPrefix};
use magnetite_tracker_lib::Tracker;
use heap_dist_key::{general_heap_entry_push_or_replace, GeneralHeapEntry};

use crate::wire::{DhtMessage, DhtMessageData, DhtMessageQuery, DhtMessageQueryGetPeers};

pub mod wire;

// Please make this a power of two, since Vec::with_capacity aligns to
// powers of two so we're paying the cost of the memory anyway.
pub const BUCKET_SIZE: usize = 1 << 3;

pub const RECURSION_CACHED_NODE_COUNT: usize = BUCKET_SIZE << 2;

pub const TRANSACTION_SIZE_BYTES: usize = 8;

const BUCKET_EXCLUSION_COUNT: usize = 2;

const MAX_UNRESPONDED_QUERIES_BAD: i32 = 5;
const MAX_UNRESPONDED_QUERIES_QUESTIONABLE: i32 = 5;

const QUESTIONABLE_THRESH: Duration = Duration::from_secs(15 * 60);

#[derive(Copy, Clone)]
enum AddressFamily {
    Ipv4,
    Ipv6,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Copy)]
pub struct ThinNode {
    pub id: TorrentId,
    pub saddr: SocketAddr,
}


pub struct RecursionState {
    target_info_hash: TorrentId,
    cached_nodes_max: usize,
    search_eye: TorrentIdPrefix,
    // anti-spamming measure.
    visited_nodes: BTreeSet<SocketAddr>,
    // sorted by distance from target
    pub nodes: BTreeMap<TorrentId, ThinNode>,
}

impl RecursionState {
    pub fn new(
        target: TorrentId,
        bm: &BucketManager2,
        env: &GeneralEnvironment,
        cached_nodes_max: usize,
    ) -> Option<RecursionState> {
        type HeapEntry = TorrentIdHeapEntry<ThinNode>;

        let mut heap = BinaryHeap::with_capacity(cached_nodes_max);

        let mut found_one = false;
        for (_, n) in &bm.nodes {
            found_one = true;
            if !n.quality(env).is_good() {
                continue;
            }

            general_heap_entry_push_or_replace(
                &mut heap,
                cached_nodes_max,
                HeapEntry {
                    dist_key: !(n.thin.id ^ target),
                    value: ThinNode {
                        id: n.thin.id,
                        saddr: n.thin.saddr,
                    },
                },
            );
        }

        if !found_one {
            return None;
        }

        Some(RecursionState {
            target_info_hash: target,
            cached_nodes_max,
            search_eye: TorrentIdPrefix::zero(),
            nodes: heap.into_iter().map(|e| {
                (e.dist_key, e.value)
            }).collect(),
            visited_nodes: Default::default(),
        })
    }

    pub fn has_work(&self) -> bool {
        !self.nodes.is_empty()
    }

    pub fn get_work(&mut self) -> Option<ThinNode> {
        let (k, _) = self.nodes.range(..).next()?;
        let k2 = k.clone();
        drop(k);
        self.nodes.remove(&k2)
    }

    pub fn search_eye(&self) -> TorrentIdPrefix {
        TorrentIdPrefix::new(
            self.target_info_hash,
            self.search_eye.prefix_len)
    }

    pub fn add_candidate(&mut self, node: ThinNode) -> bool {
        assert!(self.cached_nodes_max > 0);
        if self.visited_nodes.contains(&node.saddr) {
            event!(Level::INFO, "node already visited");
            return false;
        }

        let dist_val = self.target_info_hash ^ node.id;
        if !self.search_eye.contains(&dist_val) {
            return false;
        }
        if self.nodes.len() < self.cached_nodes_max {
            self.nodes.insert(dist_val, node);
            return true;
        } else {
            let (k, _) = self.nodes.range(..).rev().next().expect("unreachable unless self.cached_nodes_max is 0");
            let k2 = k.clone();
            drop(k);

            let e;
            if let btree_map::Entry::Occupied(e_tmp) = self.nodes.entry(k2) {
                e = e_tmp;
            } else {
                unreachable!("key was fetched from btree but doesn't exist within it.");
            }

            let ekey = *e.key();
            let mut added = false;
            if dist_val < ekey {
                e.remove();
                self.nodes.insert(dist_val, node);
                added = true;

                // we're at the `cached_nodes_max` limit, so see if we can reduce our
                // eye.  If we need longer-lived/wider searches, the thing to tune is
                // `cached_nodes_max`.
                while let Some(longer) = self.search_eye.longer() {
                    if longer.contains(&dist_val) {
                        self.search_eye = longer;
                    } else {
                        break;
                    }
                }
            }

            return added;
        }
    }
}

type TorrentIdHeapEntry<T> = GeneralHeapEntry<TorrentId, T>;

#[derive(Debug, Clone)]
pub struct Node {
    pub thin: ThinNode,
    // if a persisted node, this will be set to the bucket it belongs to.
    pub in_bucket: bool,
    // none if we've never queried this node.
    pub node_stats: Option<NodeReplyStats>,
    // how many times we've timed out
    pub timeouts: i32,
    // record outgoing ping counter, saturating
    pub sent_pings: i8,
    // when we can send the next exploratory ping/find-nodes
    pub next_allowed_ping: Instant,
    // used for expiration when not in bucket, bumped to now when:
    // * inserted into the pool
    // * responds to a query
    pub last_touch_time: Instant,
    // used for expiration when not in bucket, bumped to now when:
    // * inserted into the pool
    // * responds to a query
    // * we see a request from it
    pub last_touch_time_weak: Instant,
}

#[derive(Debug, Clone)]
pub struct NodeReplyStats {
    // time of last valid message received
    pub last_message_time: Instant,
    // time of last sent request
    pub last_request_sent_time: Option<Instant>,
    // time of last correct reply received, none if no valid response has ever been
    // recorded.
    pub last_correct_reply_time: Option<Instant>,
}

pub enum NodeActivityCommand {
    ReceiveRequest,
    ReceiveResponse,
    SendRequest,
}

pub struct NodeDebug<'a> {
    node: &'a Node,
}

impl<'a> std::fmt::Debug for NodeDebug<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // TODO: belongs in NodeThin.
        write!(f, "{}@{:?}", self.node.thin.id.hex(), self.node.thin.saddr)
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

#[derive(Debug, Clone, Copy)]
pub struct GeneralEnvironment {
    pub now: Instant,
}

#[derive(Debug)]
pub struct RequestEnvironment {
    pub gen: GeneralEnvironment,
    pub is_reply: bool,
    pub addr: SocketAddr,
}

impl Node {
    fn new(peer_id: TorrentId, peer_addr: SocketAddr, env: &GeneralEnvironment) -> Node {
        Node {
            thin: ThinNode {
                id: peer_id,
                saddr: peer_addr,
            },
            in_bucket: false,
            node_stats: None,
            timeouts: 0,
            sent_pings: 0,
            next_allowed_ping: env.now,
            last_touch_time: env.now,
            last_touch_time_weak: env.now,
        }
    }

    pub fn debug(&self) -> NodeDebug {
        NodeDebug { node: self }
    }

    #[deprecated]  // use self.last_touch_time
    pub fn get_last_message_age(&self, env: &GeneralEnvironment) -> Option<Duration> {
        if let Some(ref v) = self.node_stats {
            return Some(env.now - v.last_message_time);
        }
        None
    }

    pub fn quality(&self, genv: &GeneralEnvironment) -> NodeQuality {
        if MAX_UNRESPONDED_QUERIES_BAD <= self.timeouts {
            return NodeQuality::Bad;
        }
        if MAX_UNRESPONDED_QUERIES_QUESTIONABLE <= self.timeouts {
            return NodeQuality::Questionable;
        }
        if let Some(ref stats) = self.node_stats {
            if let Some(lcrt) = stats.last_correct_reply_time {
                if genv.now - lcrt < QUESTIONABLE_THRESH {
                    return NodeQuality::Good;
                }
            }
        }
        NodeQuality::Questionable
    }

    pub fn apply_activity_receive_request(&mut self, env: &GeneralEnvironment) {
        self.last_touch_time_weak = env.now;
        self.sent_pings = self.sent_pings.saturating_add(1);
        if let Some(ref mut rs) = self.node_stats {
            rs.last_message_time = env.now;
        } else {
            self.node_stats = Some(NodeReplyStats {
                last_message_time: env.now,
                last_request_sent_time: None,
                last_correct_reply_time: None,
            });
        }
    }

    pub fn apply_activity_receive_response(&mut self, env: &GeneralEnvironment) {
        self.last_touch_time = env.now;
        self.last_touch_time_weak = env.now;
        self.next_allowed_ping = env.now + Duration::from_secs(120);
        self.timeouts = 0;
        if let Some(ref mut rs) = self.node_stats {
            rs.last_message_time = env.now;
            rs.last_correct_reply_time = Some(env.now);
        } else {
            self.node_stats = Some(NodeReplyStats {
                last_message_time: env.now,
                last_request_sent_time: None,
                last_correct_reply_time: Some(env.now),
            });
        }
    }
}


#[derive(Debug)]
pub struct BucketManager {
    pub self_peer_id: TorrentId,
    pub buckets: Vec<Bucket>,
    pub recent_dead_hosts: (),
    pub bootstrap_hostnames: Vec<String>,
    pub host_remove_dups: HashMap<IpAddr, TorrentId>,

    pub token_rs_next_incr: Instant,
    pub token_rs_current: RandomState,
    pub token_rs_previous: RandomState,
    pub get_peers_search: RandomState,
    pub transactions: BTreeMap<u64, Box<TransactionState>>,
    pub tracker: Tracker,
}

impl BucketManager {
    pub fn find_mut_oldest_bucket<'a>(&'a mut self) -> &'a mut Bucket {
        assert!(self.buckets.len() > 0);
        self.buckets.iter_mut()
            .min_by_key(|b| b.last_touched_time)
            .expect("must have at least one bucket")
    }

    pub fn find_mut_worst_bucket<'a>(&'a mut self, genv: &GeneralEnvironment) -> &'a mut Bucket {
        assert!(self.buckets.len() > 0);
        let mut lowest_node_bucket_node_score = usize::max_value();
        let mut lowest_node_bucket = None;
        for b in &mut self.buckets {
            let score = b.nodes.iter().map(|n| match n.quality(genv) {
                NodeQuality::Good => 16,
                NodeQuality::Questionable => 4,
                NodeQuality::Bad => 1,
            }).sum();
            if score < lowest_node_bucket_node_score && !b.nodes.is_empty() {
                lowest_node_bucket_node_score = score;
                lowest_node_bucket = Some(b);
            }
        }
        lowest_node_bucket.unwrap()
    }

    pub fn node_count(&self) -> usize {
        let mut acc = 0;
        for b in &self.buckets {
            acc += b.nodes.len()
        }
        acc
    }

    pub fn format_buckets<'a>(&'a self) -> BucketGroupFormatter<'a> {
        BucketGroupFormatter { bb: &self.buckets }
    }
}

pub struct BucketGroupFormatter<'a> {
    pub bb: &'a [Bucket],
}

impl<'a> fmt::Display for BucketGroupFormatter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let genv = GeneralEnvironment {
            now: Instant::now(),
        };
        write!(f, "buckets count={}\n", self.bb.len())?;
        for bucket in self.bb {
            write!(f, "{}", BucketFormatter {
                bb: bucket,
                genv: &genv,
            })?;
        }
        Ok(())
    }
}

pub struct BucketFormatter<'a> {
    pub bb: &'a Bucket,
    pub genv: &'a GeneralEnvironment,
}

impl<'a> fmt::Display for BucketFormatter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "    bucket {}/{} age={:?}    nodes={}\n",
            self.bb.prefix.base.hex(),
            self.bb.prefix.prefix_len,
            self.genv.now - self.bb.last_touched_time,
            self.bb.nodes.len(),
        )?;
        for node in &self.bb.nodes {
            let quality = match node.quality(self.genv) {
                NodeQuality::Good => "🔥",
                NodeQuality::Questionable => "❓",
                NodeQuality::Bad => "🧊",
            };

            write!(f, "        {} {} {:21} age={:?} timeouts={}\n",
                quality,
                node.thin.id.hex(),
                node.thin.saddr,
                self.genv.now - node.last_touch_time,
                node.timeouts,
            )?;
        }
        Ok(())
    }
}

pub struct TransactionState {
    pub saddr: SocketAddr,
    pub queried_peer_id: Option<TorrentId>,
    pub query: wire::DhtMessage,
    pub completion_port: oneshot::Sender<Box<TransactionCompletion>>,
}

impl TransactionState {
    pub fn queried_thin_node(&self) -> Option<ThinNode> {
        let id = self.queried_peer_id?;
        Some(ThinNode {
            id,
            saddr: self.saddr,
        })
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
            .field("from_expected_peer", &self.saddr)
            .field("completion_port", &Ellipses)
            .finish()
    }
}

#[derive(Debug)]
pub struct TransactionCompletion {
    pub queried_node: SocketAddr,
    pub queried_peer_id: Option<TorrentId>,
    pub query: wire::DhtMessage,
    pub response: Result<wire::DhtMessageResponse, ()>,
}

impl TransactionCompletion {
    pub fn queried_thin_node(&self) -> Option<ThinNode> {
        let id = self.queried_peer_id?;
        Some(ThinNode {
            id,
            saddr: self.queried_node,
        })
    }
}

impl BucketManager {
    pub fn new(self_peer_id: &TorrentId) -> BucketManager {
        let mut buckets = Vec::new();
        let now = Instant::now();

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
            self_peer_id: *self_peer_id,
            buckets,
            recent_dead_hosts: (),
            host_remove_dups: HashMap::new(),
            token_rs_next_incr: now + Duration::new(300, 0),
            token_rs_current: rs.clone(),
            token_rs_previous: rs,
            get_peers_search: RandomState::new(),
            transactions: BTreeMap::new(),
            tracker: Tracker::new(),
            bootstrap_hostnames: Vec::new(),
        }
    }

    pub fn tick(&mut self, env: &GeneralEnvironment) {
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
        env: &GeneralEnvironment,
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
                        // max-heap so get the reversed value of the bitwise xor
                        dist_key: !(n.thin.id ^ *target),
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

    fn find_node_mut_by_id<'man>(
        &'man mut self,
        id: &TorrentId,
    ) -> Option<&'man mut Node> {
        let mut idx = 0;
        let mut found = false;
        for (i, b) in self.buckets.iter().enumerate() {
            if b.prefix.contains(id) {
                idx = i;
                found = true;
            }
        }
        if !found {
            unreachable!("bucket manager not covering entire keyspace");
        }
        for n in &mut self.buckets[idx].nodes {
            if n.thin.id == *id {
                return Some(n);
            }
        }
        None
    }

    fn find_bucket_mut_for_insertion_by_id<'man>(
        &'man mut self,
        id: &TorrentId,
        env: &GeneralEnvironment,
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

    pub fn node_seen(&mut self, node: &ThinNode, env: &RequestEnvironment) {
        let host_ip = env.addr.ip();
        if let Some(v) = self.host_remove_dups.get(&host_ip) {
            if *v != node.id {
                return;
            }
        }
        self.host_remove_dups.insert(host_ip, node.id);

        let self_peer_id = self.self_peer_id;
        if node.id == self_peer_id {
            return;
        }
        let b = self.find_bucket_mut_for_insertion_by_id(&node.id, &env.gen);

        let mut new_buckets = Vec::new();
        let mut new_bucket_slot_ref = None;
        if b.prefix.contains(&self_peer_id) {
            new_bucket_slot_ref = Some(&mut new_buckets);
        }

        b.node_seen(&self_peer_id, node, env, new_bucket_slot_ref);

        if !new_buckets.is_empty() {
            self.buckets.extend(new_buckets.into_iter());
            self.buckets.sort_by_key(|bm| bm.prefix.base);
        }
    }


    pub fn handle_incoming_packet(
        &mut self,
        message: &wire::DhtMessage,
        from: SocketAddr,
        env: &GeneralEnvironment,
    ) -> bool {
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
            if from != tx.saddr {
                event!(Level::INFO,
                    kind="bad-peer-invalid-source",
                    from=?from, from_expected=?tx.saddr,
                    "TX-{:016x} peer invalid: bad response source",
                    txid,
                );
            }
            if let Some(peer_id) = tx.queried_peer_id {
                if resp.data.id != peer_id {
                    event!(Level::INFO,
                        kind="bad-peer-invalid-id",
                        from=?from,
                        peer_id=%resp.data.id.hex(),
                        peer_id_expected=%peer_id.hex(),
                        "TX-{:016x} peer invalid: bad response peer-id",
                        txid,
                    );
                }
            }
            let res = tx.completion_port.send(Box::new(TransactionCompletion {
                queried_node: tx.saddr,
                queried_peer_id: Some(resp.data.id),
                query: tx.query,
                response: Ok(resp.clone()),
            }));

            if let Err(err) = res {
                // these are fine - just means the message was fire-and-forget.
                event!(Level::TRACE, "failed to send on completion port: {:?}", err);
            }

            true
        } else {
            false
        }
    }


    pub fn clean_expired_transaction(&mut self, txid: u64, genv: &GeneralEnvironment) {
        if let Some(tx) = self.transactions.remove(&txid) {
            let target_node = tx.queried_thin_node();
            let res = tx.completion_port.send(Box::new(TransactionCompletion {
                queried_node: tx.saddr,
                queried_peer_id: tx.queried_peer_id,
                query: tx.query,
                response: Err(()),
            }));
            if let Some(tn) = target_node {
                if let Some(node) = self.find_node_mut_by_id(&tn.id) {
                    node.timeouts += 1;
                    node.next_allowed_ping = genv.now + Duration::from_secs(120);
                }
            }
            if let Err(err) = res {
                // these are fine - just means the message was fire-and-forget.
                event!(Level::TRACE, "failed to send on completion port: {:?}", err);
            }
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
        queried_peer_id: Option<TorrentId>,
    ) -> oneshot::Receiver<Box<TransactionCompletion>> {
        let (tx, rx) = oneshot::channel();
        message.transaction = self.entry.key().to_be_bytes().to_vec();
        self.entry.insert(Box::new(TransactionState {
            query: message.clone(),
            queried_peer_id,
            saddr: to,
            completion_port: tx,
        }));

        rx
    }
}

pub struct Bucket {
    // af: AddressFamily,
    pub prefix: TorrentIdPrefix,
    pub last_touched_time: Instant,
    pub nodes: SmallVec<[Node; BUCKET_SIZE]>,
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

// #[test]
// fn foobar() {
//     let first_half = TorrentIdPrefix {
//         base: TorrentId::zero(),
//         prefix_len: 1,
//     };

//     let whole_set = TorrentIdPrefix {
//         base: TorrentId::zero(),
//         prefix_len: 0,
//     };

//     assert!(first_half.is_proper_subset_of(&whole_set));
//     assert_eq!(false, whole_set.is_proper_subset_of(&first_half));
//     assert_eq!(false, whole_set.is_proper_subset_of(&whole_set));
// }

impl Bucket {
    fn touch(&mut self, env: &GeneralEnvironment) {
        self.last_touched_time = env.now;
    }

    fn is_full(&self) -> bool {
        assert!(self.nodes.len() <= BUCKET_SIZE);
        BUCKET_SIZE <= self.nodes.len()
    }

    /// Iff we're full of good nodes, we're saturated.
    pub fn is_saturated(&self, env: &GeneralEnvironment) -> bool {
        if !self.is_full() {
            return false;
        }

        for n in &self.nodes {
            if !n.quality(&env).is_good() {
                return false;
            }
        }

        true
    }

    fn split_bucket_and_add(
        &mut self,
        peer_self_id: &TorrentId,
        seen_node: &ThinNode,
        env: &RequestEnvironment,
        new_bucket: &mut Vec<Bucket>,
    ) {
        let mut nn = Node {
            thin: *seen_node,
            node_stats: None,
            in_bucket: false,
            timeouts: 0,
            sent_pings: 0,
            next_allowed_ping: env.gen.now,
            last_touch_time: env.gen.now,
            last_touch_time_weak: env.gen.now,
        };
        if env.is_reply {
            nn.apply_activity_receive_response(&env.gen);
        } else {
            nn.apply_activity_receive_request(&env.gen);
        }

        while self.is_saturated(&env.gen) && self.prefix.contains(&seen_node.id) {
            let (new_self_prefix, other_prefix) = self.prefix.split_swap(peer_self_id);
            // event!(Level::TRACE, "ITERATION -- {:?} => {:?} + {:?}", self.prefix, new_self_prefix, other_prefix);
            let mut new_self_nodes: SmallVec<[Node; BUCKET_SIZE]> = Default::default();
            let mut other_bucket_nodes: SmallVec<[Node; BUCKET_SIZE]> = Default::default();

            for node in self.nodes.drain(..) {
                // event!(Level::TRACE, "    Checking node: {:?}", node.peer_id);
                // event!(Level::TRACE, "        -? within {:?} :: {}", new_self_prefix, new_self_prefix.contains(&node.peer_id));
                // event!(Level::TRACE, "        -? within {:?} :: {}", other_prefix, other_prefix.contains(&node.peer_id));

                if new_self_prefix.contains(&node.thin.id) {
                    new_self_nodes.push(node);
                } else {
                    assert!(other_prefix.contains(&node.thin.id));
                    other_bucket_nodes.push(node);
                }
            }

            self.prefix = new_self_prefix;
            self.nodes = new_self_nodes;

            if other_prefix.contains(&seen_node.id) {
                // other_prefix is full of good nodes (otherwise we wouldn't have
                // been trying to split), so we can just discard the node.
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

        assert!(self.prefix.contains(&nn.thin.id));
        self.nodes.push(nn);
    }

    fn node_seen(
        &mut self,
        peer_self_id: &TorrentId,
        seen_node: &ThinNode,
        env: &RequestEnvironment,
        // is Some if this is our home bucket.
        new_bucket: Option<&mut Vec<Bucket>>,
    ) {
        assert!(self.prefix.contains(&seen_node.id));
        self.touch(&env.gen);

        for n in self.nodes.iter_mut() {
            if n.thin == *seen_node {
                event!(Level::DEBUG,
                    seen_node=?seen_node,
                    old_age=?n.get_last_message_age(&env.gen),
                    "node-seen");
                if env.is_reply {
                    n.apply_activity_receive_response(&env.gen);
                } else {
                    n.apply_activity_receive_request(&env.gen);
                }
                return;
            } else if n.thin.id == seen_node.id && env.is_reply {
                event!(Level::DEBUG,
                    seen_node=?seen_node,
                    seen_node_exp=?n.thin,
                    age=?n.get_last_message_age(&env.gen),
                    "node-seen-replaced-src-addr");
                n.thin = *seen_node;
                return;
            } else if n.thin.id == seen_node.id && !env.is_reply {
                event!(Level::DEBUG,
                    seen_node=?seen_node,
                    seen_node_exp=?n.thin,
                    age=?n.get_last_message_age(&env.gen),
                    "node-seen-invalid-src-addr");
                // FIXME: drop incorrect source address???
                return;
            }
        }
        event!(Level::DEBUG,
            seen_node=?seen_node,
            "node-seen-new");

        if self.is_saturated(&env.gen) && new_bucket.is_some() {
            let new_bucket = new_bucket.unwrap();
            self.split_bucket_and_add(peer_self_id, seen_node, env, new_bucket);
        } else if !self.is_saturated(&env.gen) {
            // not saturated so we can replace a bad node or add.
            let mut nn = Node {
                thin: *seen_node,
                node_stats: None,
                in_bucket: false,
                timeouts: 0,
                sent_pings: 0,
                next_allowed_ping: env.gen.now,
                last_touch_time: env.gen.now,
                last_touch_time_weak: env.gen.now,
            };
            if env.is_reply {
                nn.apply_activity_receive_response(&env.gen);
            } else {
                nn.apply_activity_receive_request(&env.gen);
            }

            self.last_touched_time = env.gen.now;
            if self.is_full() {
                let mut bad_node = None;
                for n in self.nodes.iter_mut() {
                    if n.quality(&env.gen).is_bad() {
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

    pub fn find_mut_node_for_maintenance_ping<'a>(&'a mut self, env: &GeneralEnvironment) -> Option<&'a mut Node> {
        // borrowck....
        let has_first_branch_node = self.nodes.iter()
            .filter(|n| n.next_allowed_ping < env.now)
            .filter(|n| n.sent_pings == 0)
            .filter(|n| n.node_stats.is_none())
            .next()
            .is_some();

        if has_first_branch_node {
            self.nodes.iter_mut()
                .filter(|n| n.next_allowed_ping < env.now)
                .filter(|n| n.sent_pings == 0)
                .filter(|n| n.node_stats.is_none())
                .next()
        } else {
            self.nodes.iter_mut()
                .filter(|n| n.next_allowed_ping < env.now)
                .min_by_key(|n| n.node_stats.as_ref().map(|s| s.last_message_time))
        }
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

///////////// -----

#[derive(Copy, Clone, Debug)]
pub struct BucketInfo {
    pub prefix: TorrentIdPrefix,
    pub last_touched_time: Instant,
}

#[derive(Debug)]
pub struct BucketManager2 {
    pub self_peer_id: TorrentId,
    pub bootstrap_hostnames: Vec<String>,

    pub buckets: BTreeMap<TorrentId, BucketInfo>,

    // for node in nodes: host_ip_to_node_bind[node.thin.id] == node.thing.addr
    // for (addr, id) in host_ip_to_node_bind: nodes[id].thin.addr == ip
    // drop packet if either constraint violated.
    pub host_ip_to_node_bind: HashMap<IpAddr, TorrentId>,
    pub nodes: BTreeMap<TorrentId, Node>,
    // tracks inflight requests
    pub transactions: BTreeMap<u64, Box<TransactionState>>,
    pub tracker: Tracker,
    pub get_peers_search: RandomState,
    // clocks and hashers relating to validating response packets.
    pub token_rs_next_incr: Instant,
    pub token_rs_current: RandomState,
    pub token_rs_previous: RandomState,
}

#[cfg(test)]
mod tests_BucketManager2 {
    use rand::{thread_rng, Rng, RngCore};

    use super::{TorrentId, TorrentIdPrefix, BucketManager2};

    #[test]
    fn test_find_bucket_key() -> anyhow::Result<()> {
        let mut rng = thread_rng();

        let mut self_id = TorrentId::zero();
        rng.fill_bytes(&mut self_id.0[..]);
        let bm = BucketManager2::new(&self_id);

        assert_eq!(bm.find_bucket_prefix(&TorrentId::zero()).base, TorrentId::zero());
        for i in 0..1024 {
            let mut id = TorrentId::zero();
            rng.fill_bytes(&mut id.0[..]);
            assert_eq!(bm.find_bucket_prefix(&id).base, TorrentId::zero());
        }
        // println!("{:#?}", bm);
        // panic!();
        Ok(())
    }

    #[test]
    fn test_split_buckets() -> anyhow::Result<()> {
        let mut rng = thread_rng();

        let mut self_id = TorrentId::zero();
        rng.fill_bytes(&mut self_id.0[..]);
        let mut bm = BucketManager2::new(&self_id);

        assert_eq!(bm.find_bucket_prefix(&TorrentId::zero()).base, TorrentId::zero());

        for i in 0..1024 {
            let mut id = TorrentId::zero();
            rng.fill_bytes(&mut id.0[..]);
            if bm.deepest_bucket_prefix().contains(&id) {
                bm.split_bucket(&id)?;
            }

            // must be able to find again, ensuring the entire keyspace
            // is covered.
            bm.find_bucket_prefix(&id);
        }
        // println!("{:#?}", bm);
        // panic!();
        Ok(())
    }
}

impl BucketManager2 {
    pub fn new(tid: &TorrentId) -> BucketManager2 {
        let mut buckets = BTreeMap::new();
        let now = Instant::now();
        buckets.insert(TorrentId::zero(), BucketInfo {
            prefix: TorrentIdPrefix::zero(),
            last_touched_time: now - Duration::from_secs(900),
        });

        BucketManager2 {
            self_peer_id: *tid,
            buckets,
            host_ip_to_node_bind: HashMap::new(),
            nodes: BTreeMap::new(),

            transactions: BTreeMap::new(),
            tracker: Tracker::new(),
            bootstrap_hostnames: Vec::new(),

            token_rs_next_incr: now + Duration::new(300, 0),
            token_rs_current: RandomState::new(),
            token_rs_previous: RandomState::new(),
            get_peers_search: RandomState::new(),
        }
    }

    pub fn node_seen(&mut self, node_: &ThinNode, env: &RequestEnvironment) {
        let node = self.nodes.entry(node_.id).or_insert_with(|| {
            Node::new(node_.id, node_.saddr, &env.gen)
        });
        if env.is_reply {
            node.timeouts = 0;
            if let Some(ref mut v) = node.node_stats {
                v.last_message_time = env.gen.now;
            }
        }

        let bucket_prefix = self.find_bucket_prefix(&node_.id);
        if let Some(v) = self.buckets.get_mut(&bucket_prefix.base) {
            v.last_touched_time = env.gen.now;
        }
    }

    fn find_node_mut_by_id<'a>(&'a mut self, id: &TorrentId) -> Option<&'a mut Node> {
        self.nodes.get_mut(id)
    }

    fn find_bucket_prefix(&self, id: &TorrentId) -> TorrentIdPrefix {
        let (k, v) = self.buckets.range(..=id).next_back().unwrap();
        assert!(v.prefix.contains(id));
        assert_eq!(v.prefix.base, *k);
        v.prefix
    }

    pub fn deepest_bucket_prefix(&self) -> TorrentIdPrefix {
        self.find_bucket_prefix(&self.self_peer_id)
    }

    pub fn split_bucket(&mut self, id: &TorrentId) -> anyhow::Result<()> {
        let base = self.find_bucket_prefix(id).base;
        let bucket: &mut BucketInfo = self.buckets.get_mut(&base).unwrap();

        if 128 < bucket.prefix.prefix_len {
            return Err(anyhow!("excessively partitioned"));
        }

        let mut bucket_copy = *bucket;
        let (self_new, other_new) = bucket.prefix.split();
        bucket.prefix = self_new;
        bucket_copy.prefix = other_new;
        self.buckets.insert(other_new.base, bucket_copy);
        Ok(())
    }

    pub fn adding_to_bucket_creates_split(&self, id: &TorrentId, genv: &GeneralEnvironment) -> bool {
        let prefix = self.find_bucket_prefix(id);
        let mut found_good_nodes = 0;
        for (k, v) in self.nodes.range(prefix.to_range()) {
            assert!(prefix.contains(k), "{:?} not in {:?}, prefix mask is {:?}", k, prefix.to_range(), prefix.mask());
            if v.quality(genv).is_good() {
                found_good_nodes += 1;
            }
        }
        8 < found_good_nodes
    }

    pub fn nodes_for_bucket_containing_id<'a>(&'a self, id: &TorrentId) -> std::collections::btree_map::Range<'a, TorrentId, Node> {
        let prefix = self.find_bucket_prefix(id);
        self.nodes.range(prefix.to_range())
    }

    pub fn find_oldest_bucket<'a>(&'a self) -> &'a BucketInfo {
        assert!(self.buckets.len() > 0);
        let (_, b) = self.buckets.iter()
            .min_by_key(|(_, b)| b.last_touched_time)
            .expect("must have at least one bucket");

        b
    }

    pub fn find_mut_oldest_bucket<'a>(&'a mut self) -> &'a mut BucketInfo {
        assert!(self.buckets.len() > 0);
        let (_, b) = self.buckets.iter_mut()
            .min_by_key(|(_, b)| b.last_touched_time)
            .expect("must have at least one bucket");

        b
    }

    pub fn find_worst_bucket<'a>(&'a self, genv: &GeneralEnvironment) -> &'a BucketInfo {
        let mut lowest_node_bucket_node_score = usize::max_value();
        let mut lowest_node_bucket = None;

        for (bucket_key, bucket_info) in &self.buckets {
            let mut node_count = 0;
            let mut score = 0;
            for (_, node) in self.nodes.range(bucket_info.prefix.to_range()) {
                node_count += 1;
                score += match node.quality(genv) {
                    NodeQuality::Good => 4,
                    NodeQuality::Questionable => 2,
                    NodeQuality::Bad => 1,
                }
            }

            if score < lowest_node_bucket_node_score {
                lowest_node_bucket_node_score = score;
                lowest_node_bucket = Some(*bucket_key);
            }
        }

        let best_key = lowest_node_bucket.unwrap();
        self.buckets.get(&best_key).unwrap()
    }

    pub fn find_mut_worst_bucket<'a>(&'a mut self, genv: &GeneralEnvironment) -> &'a mut BucketInfo {
        let mut lowest_node_bucket_node_score = usize::max_value();
        let mut lowest_node_bucket = None;

        for (bucket_key, bucket_info) in &self.buckets {
            let mut node_count = 0;
            let mut score = 0;
            for (_, node) in self.nodes.range(bucket_info.prefix.to_range()) {
                node_count += 1;
                score += match node.quality(genv) {
                    NodeQuality::Good => 4,
                    NodeQuality::Questionable => 2,
                    NodeQuality::Bad => 1,
                }
            }

            if score < lowest_node_bucket_node_score && node_count != 0 {
                lowest_node_bucket_node_score = score;
                lowest_node_bucket = Some(*bucket_key);
            }
        }

        let best_key = lowest_node_bucket.unwrap();
        self.buckets.get_mut(&best_key).unwrap()
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn format_buckets<'a>(&'a self) -> BucketManager2Formatter<'a> {
        BucketManager2Formatter { parent: &self }
    }

    pub fn find_close_nodes(
        &self,
        target: &TorrentId,
        limit: usize,
        env: &GeneralEnvironment,
    ) -> Vec<&Node> {
        type HeapEntry<'a> = TorrentIdHeapEntry<&'a Node>;

        let mut heap = BinaryHeap::with_capacity(limit);

        let mut inspected = 0;
        for (_, n) in self.nodes.range(target..) {
            if !n.quality(env).is_good() {
                continue;
            }
            if limit < inspected {
                break;
            }
            inspected += 1;
            general_heap_entry_push_or_replace(
                &mut heap,
                limit,
                HeapEntry {
                    // max-heap so get the reversed value of the bitwise xor
                    dist_key: !(n.thin.id ^ *target),
                    value: n,
                },
            );
        }
        for (_, n) in self.nodes.range(..target).rev() {
            if !n.quality(env).is_good() {
                continue;
            }
            if limit < inspected {
                break;
            }
            inspected += 1;
            general_heap_entry_push_or_replace(
                &mut heap,
                limit,
                HeapEntry {
                    // max-heap so get the reversed value of the bitwise xor
                    dist_key: !(n.thin.id ^ *target),
                    value: n,
                },
            );
        }


        let mut out = Vec::with_capacity(heap.len());
        for entry in heap.drain() {
            out.push(entry.value);
        }

        out
    }

    pub fn clean_expired_transaction(&mut self, txid: u64, genv: &GeneralEnvironment) {
        if let Some(tx) = self.transactions.remove(&txid) {
            let target_node = tx.queried_thin_node();
            let res = tx.completion_port.send(Box::new(TransactionCompletion {
                queried_node: tx.saddr,
                queried_peer_id: tx.queried_peer_id,
                query: tx.query,
                response: Err(()),
            }));
            if let Some(tn) = target_node {
                if let Some(node) = self.find_node_mut_by_id(&tn.id) {
                    node.timeouts += 1;
                    node.next_allowed_ping = genv.now + Duration::from_secs(120);
                }
            }
            if let Err(err) = res {
                // these are fine - just means the message was fire-and-forget.
                event!(Level::TRACE, "failed to send on completion port: {:?}", err);
            }
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

    pub fn handle_incoming_packet(
        &mut self,
        message: &wire::DhtMessage,
        from: SocketAddr,
        env: &GeneralEnvironment,
    ) -> bool {

        if message.transaction.len() != 8 {
            return false;
        }

        let mut buf = [0; 8];
        buf.copy_from_slice(&message.transaction);
        let txid = u64::from_be_bytes(buf);

        event!(Level::TRACE, tx=txid, "handle-incoming-packet");

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
            if from != tx.saddr {
                event!(Level::INFO,
                    kind="bad-peer-invalid-source",
                    from=?from, from_expected=?tx.saddr,
                    "TX-{:016x} peer invalid: bad response source",
                    txid,
                );
            }
            if let Some(peer_id) = tx.queried_peer_id {
                if resp.data.id != peer_id {
                    event!(Level::INFO,
                        kind="bad-peer-invalid-id",
                        from=?from,
                        peer_id=%resp.data.id.hex(),
                        peer_id_expected=%peer_id.hex(),
                        "TX-{:016x} peer invalid: bad response peer-id",
                        txid,
                    );
                }
            }
            let res = tx.completion_port.send(Box::new(TransactionCompletion {
                queried_node: tx.saddr,
                queried_peer_id: Some(resp.data.id),
                query: tx.query,
                response: Ok(resp.clone()),
            }));

            if let Err(err) = res {
                // these are fine - just means the message was fire-and-forget.
                event!(Level::TRACE, "failed to send on completion port: {:?}", err);
            }

            true
        } else {
            false
        }
    }

    pub fn find_mut_node_for_maintenance_ping<'a>(&'a mut self, env: &GeneralEnvironment) -> Option<&'a mut Node> {
        fn elligible_for_maintenance_ping(n: &Node, env: &GeneralEnvironment) -> bool {
            n.next_allowed_ping < env.now && n.in_bucket
        }

        // borrowck....
        let has_first_branch_node = self.nodes.iter()
            .filter(|(_, n)| elligible_for_maintenance_ping(n, env))
            .filter(|(_, n)| n.sent_pings == 0)
            .filter(|(_, n)| n.node_stats.is_none())
            .next()
            .is_some();

        if has_first_branch_node {
            self.nodes.iter_mut()
                .filter(|(_, n)| elligible_for_maintenance_ping(n, env))
                .filter(|(_, n)| n.sent_pings == 0)
                .filter(|(_, n)| n.node_stats.is_none())
                .next()
                .map(|(_, n)| n)
        } else {
            self.nodes.iter_mut()
                .filter(|(_, n)| elligible_for_maintenance_ping(n, env))
                .min_by_key(|(_, n)| n.node_stats.as_ref().map(|s| s.last_message_time))
                .map(|(_, n)| n)
        }
    }
}

pub struct BucketManager2Formatter<'a> {
    parent: &'a BucketManager2,
}

pub struct BucketInfoFormatter<'a> {
    pub bb: &'a BucketInfo,
    pub nodes: std::collections::btree_map::Range<'a, TorrentId, Node>,
    pub genv: &'a GeneralEnvironment,
}

impl<'a> fmt::Display for BucketManager2Formatter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let genv = GeneralEnvironment {
            now: Instant::now(),
        };
        write!(f, "buckets count={} nodes={}, nodes_persisted={}\n",
            self.parent.buckets.len(),
            self.parent.nodes.len(),
            self.parent.nodes.iter().filter(|(_, n)| n.in_bucket).map(|_| 1).sum::<i64>())?;
        for bucket in self.parent.buckets.values() {
            write!(f, "{}", BucketInfoFormatter {
                bb: bucket,
                nodes: self.parent.nodes.range(bucket.prefix.to_range()),
                genv: &genv,
            })?;
        }
        Ok(())
    }
}

impl<'a> fmt::Display for BucketInfoFormatter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let node_count: u32 = self.nodes.clone().map(|_| 1).sum();
        write!(f, "    bucket {}/{} age={:4.2}s nodes={}\n",
            self.bb.prefix.base.hex(),
            self.bb.prefix.prefix_len,
            (self.genv.now - self.bb.last_touched_time).as_secs_f64(),
            node_count,
        )?;
        for (_nid, node) in self.nodes.clone() {
            let quality = match node.quality(self.genv) {
                NodeQuality::Good => "🔥",
                NodeQuality::Questionable => "❓",
                NodeQuality::Bad => "🧊",
            };
            let in_bucket = match node.in_bucket {
                true => "🪣",
                false => "🕳️",
            };
            write!(f, "        {}{} {} {:21} age={:4.2}s timeouts={}\n",
                quality,
                in_bucket,
                node.thin.id.hex(),
                node.thin.saddr,
                (self.genv.now - node.last_touch_time).as_secs_f64(),
                node.timeouts,
            )?;
        }
        Ok(())
    }
}
