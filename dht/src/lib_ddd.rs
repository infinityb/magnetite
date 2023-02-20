#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Copy)]
enum NodeKey {
    SocketAddr(SocketAddr),
}

pub struct BucketManager {
    buckets: BTreeMap<TorrentId, Bucket>,
    
    nodes: BTreeMap<TorrentId, Node>,
    nodes_by_key: BTreeMap<NodeKey, TorrentId>,
}

impl BucketManager {
    fn split_bucket(&mut self, base: &TorrentId) {
        let bucket = &mut self.buckets[base];
        let original_prefix = bucket.prefix;
        let bucket_copy = *bucket;

        assert!(bucket.prefix.prefix_len < 160);
        let (self_new, other_new) = bucket.prefix.split();
        bucket.prefix = self_new;
        bucket_copy.prefix = other_new;
        self.buckets.insert(other_new.base, bucket_copy);

        for (_key, node) in self.nodes.range(original_prefix.base..) {
            if !original_prefix.contains(&node.id) {
                // we're out of the bucket's range, so we've visited all relevant nodes.
                break;
            }

            let new_base = if self_new.contains(&node.bucket_id) {
                self_new.base
            } else {
                other_new.base
            };

            let mut ns = node.state.borrow_mut();
            ns.bucket_id = new_base
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Copy)]
pub struct ThinNode {
    pub id: TorrentId,
    pub saddr: SocketAddr,
}

pub struct Node {
    thin: ThinNode,
    state: RefCell<NodeState>,
}

pub struct NodeState {
    // optional??
    bucket_id: TorrentId,
    // none if we've never queried this node.
    pub node_stats: Option<NodeReplyStats>,
    // how many times we've timed out
    pub timeouts: i32,
    // record outgoing ping counter, saturating
    pub sent_pings: i8,
    // when we can send the next exploratory ping/find-nodes
    pub next_allowed_ping: Instant,
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

#[derive(Copy, Clone)]
pub struct Bucket {
    pub prefix: TorrentIdPrefix,
    pub last_touched_time: Instant,
}
