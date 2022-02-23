use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use magnetite_common::TorrentId;

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct DhtMessage {
    #[serde(rename = "t", with = "serde_bytes")]
    transaction: Vec<u8>,
    #[serde(flatten)]
    data: DhtMessageData,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(tag = "y")]
pub enum DhtMessageData {
    #[serde(rename = "q")]
    Query(DhtMessageQuery),
    #[serde(rename = "r")]
    Response(DhtMessageResponse),
    #[serde(rename = "e")]
    Error(DhtErrorResponse),
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(tag = "q", content = "a")]
pub enum DhtMessageQuery {
    #[serde(rename = "ping")]
    Ping { id: TorrentId },
    #[serde(rename = "find_node")]
    FindNode { id: TorrentId, target: TorrentId },
    #[serde(rename = "get_peers")]
    GetPeers(DhtMessageQueryGetPeers),
    #[serde(rename = "announce_peer")]
    AnnouncePeers(DhtMessageQueryAnnouncePeer),
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct DhtMessageQueryAnnouncePeer {
    id: TorrentId,
    info_hash: TorrentId,
    port: u16,
    #[serde(with = "serde_bytes")]
    token: Vec<u8>,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct DhtMessageQueryGetPeers {
    id: TorrentId,
    info_hash: TorrentId,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct DhtErrorResponse {
    // 201 Generic Error
    // 202 Server Error
    // 203 Protocol Error, such as a malformed packet, invalid arguments, or bad token
    // 204 Method Unknown
    #[serde(rename = "e")]
    error: (i64, String),
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct DhtMessageResponse {
    #[serde(rename = "r")]
    response: DhtMessageResponseData,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DhtMessageResponseData {
    GetPeers(DhtMessageResponseGetPeers),
    FindNode(DhtMessageResponseFindNode),
    /// includes "ping" and "announce_peer" responses.
    GeneralId {
        id: TorrentId,
    },
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct DhtMessageResponseFindNode {
    id: TorrentId,
    #[serde(with = "serde_bytes")]
    nodes: Vec<u8>,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct DhtMessageResponseGetPeers {
    id: TorrentId,
    #[serde(with = "serde_bytes")]
    token: Vec<u8>,
    #[serde(flatten)]
    data: DhtMessageResponseGetPeersData,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DhtMessageResponseGetPeersData {
    Peers {
        values: Vec<serde_bytes::ByteBuf>,
    },
    CloseNodes {
        #[serde(with = "serde_bytes")]
        nodes: Vec<u8>,
    },
    CloseNodesIPv6 {
        #[serde(with = "serde_bytes")]
        nodes6: Vec<u8>,
    },
}

#[cfg(test)]
mod tests {
    use serde_bytes::ByteBuf;
    use std::fmt::Write;
    use std::{fmt, str};

    use super::{
        DhtErrorResponse,DhtMessage,DhtMessageData,DhtMessageQuery,DhtMessageQueryAnnouncePeer,
        DhtMessageQueryGetPeers,DhtMessageResponse,DhtMessageResponseData,
        DhtMessageResponseFindNode,DhtMessageResponseGetPeers,DhtMessageResponseGetPeersData,
        TorrentId,
    };

    fn dht_message_get_table() -> Vec<(&'static str, &'static [u8], DhtMessage)> {
        return vec![
            (
                "ping_query",
                b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t1:01:y1:qe",
                DhtMessage {
                    transaction: vec![0x30],
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
                    transaction: vec![0x30],
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
                    transaction: vec![0x30],
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
                    transaction: vec![0x30],
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
                    transaction: vec![0x30],
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
                    transaction: vec![0x30],
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
                    transaction: vec![0x30],
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
                b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t1:01:y1:qe",
                DhtMessage {
                    transaction: vec![0x30],
                    data: DhtMessageData::Query(DhtMessageQuery::AnnouncePeers(DhtMessageQueryAnnouncePeer {
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
            (
                "error",
                b"d1:eli201e23:A Generic Error Ocurrede1:t1:01:y1:ee",
                DhtMessage {
                    transaction: vec![0x30],
                    data: DhtMessageData::Error(DhtErrorResponse {
                        error: (201, "A Generic Error Ocurred".to_string()),
                    }),
                }
            ),
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
    fn dht_message_observed_crashes() {
        const SEEN: &[&[u8]] = &[
            b"d1:ad2:id20:n\x89\xa1(\xc0\x9e@Ce\x89\xedJ \xfdN$\x83L\xb7\xef6:target20:\x88\xc8\x9by\xc90ll\xaa\xfc\xc6\xa3\xa1>!\x0f\"\xef\xac!5:token8:\xdc\x9d\xa8}\x14w\x7f\xc04:votei0ee1:q4:vote1:t4:\x08\x1e\x82\x941:v4:UT\xab\x141:y1:qe",
        ];

        for v in SEEN {
            println!("execution test case: decode {:?}", v);
            bencode::from_bytes::<DhtMessage>(v);
        }
    }
}
