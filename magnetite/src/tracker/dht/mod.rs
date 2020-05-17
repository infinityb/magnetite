use std::collections::BTreeMap;
use std::mem;
use std::net::SocketAddr;
use std::time::SystemTime;

use smallvec::SmallVec:
use serde::{Deserialize, Serialize};

use magnetite_common::TorrentId;

// Please make this a power of two, since Vec::with_capacity aligns to
// powers of two anyway.  We're paying the cost of the memory anyway.
const BUCKET_SIZE: usize = 32;
const MAX_UNRESPONDED_QUERIES: u32 = 5;
const QUESTIONABLE_THRESH: Duration = Duration::from_secs(15 * 60);

#[derive(Copy, Clone)]
enum AddressFamily {
    Ipv4,
    Ipv6,
}

struct Node {
    peer_id: TorrentId,
    peer_addr: SocketAddr,
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

impl Node {
    fn new(base_instant: Instant, peer_id: TorrentId, peer_addr: SocketAddr) -> Node {
        Node {
            peer_id,
            peer_addr,
            last_correct_reply_time: base_instant,
            last_ping_sent_time: base_instant,
            pinged: 0,
        }
    }

    fn with_confirm(mut self, confirm: ConfirmLevel) -> Node {
        self.confirm(confirm);
        self
    }

    fn confirm(&mut self, when: Instant, confirm: ConfirmLevel) {
        if !confirm.is_empty() {
            self.last_correct_reply_time = when;
        }
        if confirm.is_responding() {
            self.last_correct_reply_time = when;
        }
    }

    fn quality(&self, when: Instant) -> NodeQuality {
        if (when - self.last_correct_reply_time) < QUESTIONABLE_THRESH {
            return NodeQuality::Good;
        }
        if MAX_UNRESPONDED_QUERIES <= self.pinged {
            return NodeQuality::Bad;
        }
        NodeQuality::Questionable
    }

    fn bump_activity(&mut self, when: Instant) -> {
        self.last_correct_reply_time = when;
    }

    fn bump_failure_count(&mut self) {
        self.pinged += 1;
    }
}

struct BucketManager {
    self_peer_id: TorrentId,
    buckets: BTreeMap<TorrentId, Bucket>,
}



struct Bucket {
    af: AddressFamily,
    base: TorrentId,
    prefix_len: u32,

    nodes: SmallVec<[Node; BUCKET_SIZE]>,
    last_touched_time: SystemTime,
    cached: (),
}

impl Bucket {
    fn new(af: AddressFamily) -> Bucket {
        Bucket {
            af: af,
            base: TorrentId::zero(),
            prefix_len: 0,

            nodes: Default::default(),
            last_touched_time: std::time::UNIX_EPOCH,
            cached: (),
        }
    }

    fn contains(&self, id: &TorrentId) -> bool {
        self.prefix_len <= (self.base ^ *id).leading_zeros()
    }

    fn touch(&mut self) {
        self.last_touched_time = SystemTime::now();
    }

    fn is_full(&self) -> bool {
        assert!(self.nodes.len() <= BUCKET_SIZE);
        BUCKET_SIZE <= self.nodes.len()
    }

    fn new_node(&mut self, id: TorrentId, ss: SocketAddr, confirm: ConfirmLevel) {
        self.add_node(Node::new(id, ss), confirm)
    }

    fn add_node(&mut self, node: Node, confirm: ConfirmLevel) {
        assert!(self.contains(&id));
        
        self.touch();

        if self.is_full() {
            // find oldest bad node or drop.
        } else {
            self.nodes.push(node.with_confirm(confirm));
        }
        // let now = SystemTime::now();

        // for n in self.nodes.as_mut_slice().iter_mut() {
        //     if node.id == n.id {
        //         if !confirm.is_empty() || n.time.sec < now.sec - 15 * 60 {
        //             n.ss = node.ss;
        //             n.confirm(confirm);
        //         }
        //     }
        // }
    }

    fn split(&mut self) -> Bucket {
        let mut new_bucket_nodes = Vec::new();

        let new_base_id = split_torrent_id_top(&self.base, self.prefix_len);
        self.prefix_len += 1;

        for node in mem::replace(&mut self.nodes, Vec::with_capacity(BUCKET_SIZE)) {
            if self.contains(&node.peer_id) {
                self.nodes.push(node);
            } else {
                new_bucket_nodes.push(node);
            }
        }

        Bucket {
            af: self.af,
            base: new_base_id,
            prefix_len: self.prefix_len,
            last_touched_time: self.last_touched_time,
            nodes: new_bucket_nodes,
            cached: (),
        }
    }
}

fn split_torrent_id_top(t: &TorrentId, old_prefix_len: u32) -> TorrentId {
    let (bytes, bits) = (old_prefix_len / 8, old_prefix_len % 8);
    let mut out = *t;
    let slice = out.as_mut_bytes();
    slice[bytes as usize] |= 0x80 >> bits;
    out
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

// fn start_service() -> DhtHandle {
//     let high_p_queue = VecDeque::new();
//     let low_p_queue = VecDeque::new();

//     //
// }

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
struct DhtMessage {
    #[serde(rename = "t", with = "serde_bytes")]
    transaction: Vec<u8>,
    #[serde(flatten)]
    data: DhtMessageData,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(tag = "y")]
enum DhtMessageData {
    #[serde(rename = "q")]
    Query(DhtMessageQuery),
    #[serde(rename = "r")]
    Response(DhtMessageResponse),
    #[serde(rename = "e")]
    Error,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(tag = "q", content = "a")]
enum DhtMessageQuery {
    #[serde(rename = "ping")]
    Ping { id: TorrentId },
    #[serde(rename = "find_node")]
    FindNode { id: TorrentId, target: TorrentId },
    #[serde(rename = "get_peers")]
    GetPeers(DhtMessageQueryGetPeers),
    #[serde(rename = "announce_peers")]
    AnnouncePeers(DhtMessageQueryAnnouncePeers),
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
struct DhtMessageQueryAnnouncePeers {
    id: TorrentId,
    info_hash: TorrentId,
    port: u16,
    #[serde(with = "serde_bytes")]
    token: Vec<u8>,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
struct DhtMessageQueryGetPeers {
    id: TorrentId,
    info_hash: TorrentId,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
struct DhtMessageResponse {
    #[serde(rename = "r")]
    response: DhtMessageResponseData,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum DhtMessageResponseData {
    GetPeers(DhtMessageResponseGetPeers),
    FindNode(DhtMessageResponseFindNode),
    /// includes "ping" and "announce_peer" responses.
    GeneralId {
        id: TorrentId,
    },
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
struct DhtMessageResponseFindNode {
    id: TorrentId,
    #[serde(with = "serde_bytes")]
    nodes: Vec<u8>,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
struct DhtMessageResponseGetPeers {
    id: TorrentId,
    #[serde(with = "serde_bytes")]
    token: Vec<u8>,
    #[serde(flatten)]
    data: DhtMessageResponseGetPeersData,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum DhtMessageResponseGetPeersData {
    Peers {
        values: Vec<serde_bytes::ByteBuf>,
    },
    CloseNodes {
        #[serde(with = "serde_bytes")]
        nodes: Vec<u8>,
    },
}

#[cfg(test)]
mod tests {
    use super::{
        split_torrent_id_top, DhtMessage, DhtMessageData, DhtMessageQuery,
        DhtMessageQueryAnnouncePeers, DhtMessageQueryGetPeers, DhtMessageResponse,
        DhtMessageResponseData, DhtMessageResponseFindNode, DhtMessageResponseGetPeers,
        DhtMessageResponseGetPeersData, TorrentId,
    };
    use serde_bytes::ByteBuf;

    fn dht_message_get_table() -> Vec<(&'static str, &'static [u8], DhtMessage)> {
        return vec![
            (
                "ping_query",
                b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t1:01:y1:qe",
                DhtMessage {
                    transaction: vec![48],
                    data: DhtMessageData::Query(DhtMessageQuery::Ping {
                        id: TorrentId([
                            0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x30, 0x31,
                            0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
                        ])
                    })
                },
            ),
            (
                "ping_response",
                b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t1:01:y1:re",
                DhtMessage {
                    transaction: vec![48],
                    data: DhtMessageData::Response(DhtMessageResponse {
                        response: DhtMessageResponseData::GeneralId {
                            id: TorrentId([
                                0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78,
                                0x79, 0x7a, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36,
                            ]),
                        }
                    }),
                }
            ),
            (
                "find_node_query",
                b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t1:01:y1:qe",
                DhtMessage {
                    transaction: vec![48],
                    data: DhtMessageData::Query(DhtMessageQuery::FindNode {
                        id: TorrentId([
                            0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x30, 0x31,
                            0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
                        ]),
                        target: TorrentId([
                            0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78,
                            0x79, 0x7a, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36,
                        ]),
                    })
                }
            ),
            (
                "find_node_response",
                b"d1:rd2:id20:0123456789abcdefghij5:nodes9:def456...e1:t1:01:y1:re",
                DhtMessage {
                    transaction: vec![48],
                    data: DhtMessageData::Response(DhtMessageResponse {
                        response: DhtMessageResponseData::FindNode(DhtMessageResponseFindNode {
                            id: TorrentId([
                                0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62,
                                0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a,
                            ]),
                            nodes: b"def456...".to_vec(),
                        })
                    }),
                }
            ),
            (
                "get_peer_query",
                b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t1:01:y1:qe",
                DhtMessage {
                    transaction: vec![48],
                    data: DhtMessageData::Query(DhtMessageQuery::GetPeers(DhtMessageQueryGetPeers {
                        id: TorrentId([
                            0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x30, 0x31,
                            0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
                        ]),
                        info_hash: TorrentId([
                            0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78,
                            0x79, 0x7a, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36,
                        ]),
                    })),
                }
            ),
            (
                "get_peer_response_peers",
                b"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl15:axje.uidhtnmbrlee1:t1:01:y1:re",
                DhtMessage {
                    transaction: vec![48],
                    data: DhtMessageData::Response(DhtMessageResponse {
                        response: DhtMessageResponseData::GetPeers(DhtMessageResponseGetPeers {
                            id: TorrentId([
                                0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x30, 0x31,
                                0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
                            ]),
                            token: b"aoeusnth".to_vec(),
                            data: DhtMessageResponseGetPeersData::Peers {
                                values: vec![
                                    ByteBuf::from(b"axje.uidhtnmbrl".to_vec()),
                                ],
                            }
                        })
                    })
                }
            ),
            (
                "get_peer_response_close_nodes",
                b"d1:rd2:id20:abcdefghij01234567895:nodes9:def456...5:token8:aoeusnthe1:t1:01:y1:re",
                DhtMessage {
                    transaction: vec![48],
                    data: DhtMessageData::Response(DhtMessageResponse {
                        response: DhtMessageResponseData::GetPeers(DhtMessageResponseGetPeers {
                            id: TorrentId([
                                0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x30, 0x31,
                                0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
                            ]),
                            token: b"aoeusnth".to_vec(),
                            data: DhtMessageResponseGetPeersData::CloseNodes {
                                nodes: b"def456...".to_vec(),
                            }
                        })
                    })
                }
            ),
            (
                "announce_peer_query",
                b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q14:announce_peers1:t1:01:y1:qe",
                DhtMessage {
                    transaction: vec![48],
                    data: DhtMessageData::Query(DhtMessageQuery::AnnouncePeers(DhtMessageQueryAnnouncePeers {
                        id: TorrentId([
                            0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x30, 0x31,
                            0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
                        ]),
                        info_hash: TorrentId([
                            0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78,
                            0x79, 0x7a, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36,
                        ]),
                        port: 6881,
                        token: b"aoeusnth".to_vec(),
                    })),
                }
            ),
            // announce_peer_response would be the same as the ping response, so not tested.
        ];
    }

    #[test]
    fn dht_message_decodes() {
        for (name, encoded_bytes, expected_decode) in dht_message_get_table().iter() {
            println!("execution test case: decode {:?}", name);
            let decoded: DhtMessage = bencode::from_bytes(encoded_bytes).unwrap();
            assert_eq!(expected_decode, &decoded, "{} failed", name);
        }
    }

    #[test]
    fn dht_message_encodes() {
        for (name, expected_encode, message) in dht_message_get_table().iter() {
            println!("execution test case: encode {:?}", name);
            let encoded = bencode::to_bytes(message).unwrap();
            assert_eq!(*expected_encode, &encoded[..], "{} failed", name);
        }
    }

    #[test]
    fn test_id_split() {
        let zero = TorrentId::zero();
        assert_eq!(
            split_torrent_id_top(&zero, 0),
            TorrentId([
                0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ])
        );
        assert_eq!(
            split_torrent_id_top(&zero, 1),
            TorrentId([
                0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ])
        );
        assert_eq!(
            split_torrent_id_top(&split_torrent_id_top(&zero, 1), 2),
            TorrentId([
                0x60, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ])
        );
    }
}
