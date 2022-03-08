use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};

use magnetite_common::TorrentId;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smallvec::SmallVec;

use crate::BinStr;

trait AddressFamily {
    type SocketAddr: Eq + std::fmt::Debug;
}

pub struct AddressFamilyV4;

impl AddressFamily for AddressFamilyV4 {
    type SocketAddr = SocketAddrV4;
}

pub struct AddressFamilyV6;

impl AddressFamily for AddressFamilyV6 {
    type SocketAddr = SocketAddrV6;
}

#[derive(Eq, PartialEq, Serialize, Deserialize)]
pub struct DhtMessage {
    #[serde(rename = "t", with = "serde_bytes")]
    pub transaction: Vec<u8>,
    #[serde(flatten)]
    pub data: DhtMessageData,
}

impl std::fmt::Debug for DhtMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("DhtMessage")
            .field("transaction", &BinStr(&self.transaction))
            .field("data", &self.data)
            .finish()
    }
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
    FindNode(DhtMessageQueryFindNode),
    #[serde(rename = "get_peers")]
    GetPeers(DhtMessageQueryGetPeers),
    #[serde(rename = "announce_peer")]
    AnnouncePeer(DhtMessageQueryAnnouncePeer),
    #[serde(rename = "sample_infohashes")]
    SampleInfohashes(DhtMessageQuerySampleInfohashes),
    #[serde(rename = "vote")]
    Vote(DhtMessageQueryVote),
}

#[derive(Eq, PartialEq, Serialize, Deserialize)]
pub struct DhtMessageQueryVote {
    pub id: TorrentId,
    pub target: TorrentId,
    pub vote: i64,
    #[serde(with = "serde_bytes")]
    pub token: Vec<u8>,
}

impl std::fmt::Debug for DhtMessageQueryVote {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("DhtMessageQueryVote")
            .field("id", &self.id)
            .field("target", &self.target)
            .field("vote", &self.vote)
            .field("token", &BinStr(&self.token))
            .finish()
    }
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct DhtMessageQuerySampleInfohashes {
    pub id: TorrentId,
    pub target: TorrentId,
}

#[derive(Eq, PartialEq, Serialize, Deserialize)]
pub struct DhtMessageQueryAnnouncePeer {
    pub id: TorrentId,
    pub info_hash: TorrentId,
    pub port: u16,
    #[serde(with = "serde_bytes")]
    pub token: Vec<u8>,
}

impl std::fmt::Debug for DhtMessageQueryAnnouncePeer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("DhtMessageQueryAnnouncePeer")
            .field("id", &self.id)
            .field("info_hash", &self.info_hash)
            .field("port", &self.port)
            .field("token", &BinStr(&self.token))
            .finish()
    }
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct DhtMessageQueryGetPeers {
    pub id: TorrentId,
    pub info_hash: TorrentId,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct DhtMessageQueryFindNode {
    pub id: TorrentId,
    pub target: TorrentId,
    #[serde(default, skip_serializing_if="SmallVec::is_empty")]
    pub want: SmallVec<[String; 2]>,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct DhtErrorResponse {
    // 201 Generic Error
    // 202 Server Error
    // 203 Protocol Error, such as a malformed packet, invalid arguments, or bad token
    // 204 Method Unknown
    #[serde(rename = "e")]
    pub error: (i64, String),
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct DhtMessageResponse {
    #[serde(rename = "r")]
    pub response: DhtMessageResponseData,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DhtMessageResponseData {
    GetPeers(DhtMessageResponseGetPeers),
    FindNode(DhtMessageResponseFindNode),
    // /// includes "ping" and "announce_peer" responses.
    GeneralId {
        id: TorrentId,
    },
}

// impl Serialize for DhtMessageResponseData {
//     //
// }

// impl<'de> Deserialize<'de> for DhtMessageResponseData {
//     //
// }

// struct CompactV4(SocketAddrV4);

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct DhtMessageResponseFindNode {
    pub id: TorrentId,
    #[serde(with = "serde_vec_socket_addr_v4", skip_serializing_if="Vec::is_empty")]
    pub nodes: Vec<SocketAddrV4>,
    #[serde(with = "serde_vec_socket_addr_v6", skip_serializing_if="Vec::is_empty")]
    pub nodes6: Vec<SocketAddrV6>,
}

#[derive(Eq, PartialEq, Serialize, Deserialize)]
pub struct DhtMessageResponseGetPeers {
    pub id: TorrentId,
    /// the write token - used for subsequent `announce_peer`
    #[serde(with = "serde_bytes")]
    pub token: Vec<u8>,
    #[serde(flatten)]
    pub data: DhtMessageResponseGetPeersData,
}

impl std::fmt::Debug for DhtMessageResponseGetPeers {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("DhtMessageResponseGetPeers")
            .field("id", &self.id)
            .field("token", &BinStr(&self.token))
            .field("data", &self.data)
            .finish()
    }
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DhtMessageResponseGetPeersData {
    Peers {
        // #[serde(with = "<T as AddressFamily>::")]
        // was Vec<serde_bytes::ByteBuf>
        #[serde(with = "serde_vec_socket_addr")]
        values: Vec<SocketAddr>,
    },
    CloseNodes {
        #[serde(with = "serde_vec_socket_addr_v4")]
        nodes: Vec<SocketAddrV4>,
        #[serde(with = "serde_vec_socket_addr_v6")]
        nodes6: Vec<SocketAddrV6>,
    },
}

mod serde_vec_socket_addr {
    use std::net::SocketAddr;

    use serde::de::SeqAccess;
    use serde::{Deserializer, Serialize, Serializer};
    use smallvec::SmallVec;

    struct AddressListVisitor;

    impl<'de> serde::de::Visitor<'de> for AddressListVisitor {
        type Value = Vec<SocketAddr>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("FIXME")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut out = Vec::new();
            if let Some(size) = seq.size_hint() {
                out.reserve(size);
            }

            while let Some(v) = seq.next_element::<SmallVec<[u8; 18]>>()? {
                match v.len() {
                    6 => {
                        //
                    }
                    18 => {
                        //
                    }
                    _ => return Err(serde::de::Error::custom(format!("bad length: {}", v.len()))),
                }
            }

            Ok(out)
        }
    }

    pub fn serialize<S>(v: &Vec<SocketAddr>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        todo!()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<SocketAddr>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(AddressListVisitor)
    }
}

mod serde_vec_socket_addr_v4 {
    use std::net::SocketAddrV4;

    use serde::{Deserializer, Serialize, Serializer};

    use super::{deserialize_sock_addr_v4, serialize_sock_addr_v4};

    struct AddressListVisitorV4;

    impl<'de> serde::de::Visitor<'de> for AddressListVisitorV4 {
        type Value = Vec<SocketAddrV4>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("an byte-aray where the length is a multiple of 6")
        }

        fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value.len() % 6 != 0 {
                return Err(E::custom(format!("bad length: {}", value.len())));
            }

            let mut out = Vec::new();
            for ch in value.chunks(6) {
                let mut buf: [u8; 6] = [0; 6];
                buf.copy_from_slice(ch);
                out.push(deserialize_sock_addr_v4(&buf));
            }

            Ok(out)
        }
    }

    pub fn serialize<S>(addrs: &Vec<SocketAddrV4>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut out = Vec::with_capacity(addrs.len() * 6);
        for a in addrs {
            let mut buf = [0; 6];
            serialize_sock_addr_v4(&mut buf, a);
            out.extend(&buf[..]);
        }
        serializer.serialize_bytes(&out)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<SocketAddrV4>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(AddressListVisitorV4)
    }
}

mod serde_vec_socket_addr_v6 {
    use std::net::SocketAddrV6;

    use serde::{Deserializer, Serialize, Serializer};

    use super::{deserialize_sock_addr_v6, serialize_sock_addr_v6};

    struct AddressListVisitorV6;

    impl<'de> serde::de::Visitor<'de> for AddressListVisitorV6 {
        type Value = Vec<SocketAddrV6>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("an byte-aray where the length is a multiple of 18")
        }

        fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value.len() % 18 != 0 {
                return Err(E::custom(format!("bad length: {}", value.len())));
            }

            let mut out = Vec::new();
            for ch in value.chunks(18) {
                let mut buf: [u8; 18] = [0; 18];
                buf.copy_from_slice(ch);
                out.push(deserialize_sock_addr_v6(&buf));
            }

            Ok(out)
        }
    }

    pub fn serialize<S>(addrs: &Vec<SocketAddrV6>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut out = Vec::with_capacity(addrs.len() * 18);
        for a in addrs {
            let mut buf = [0; 18];
            serialize_sock_addr_v6(&mut buf, a);
            out.extend(&buf[..]);
        }
        serializer.serialize_bytes(&out)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<SocketAddrV6>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(AddressListVisitorV6)
    }
}

fn serialize_sock_addr_v4(into: &mut [u8; 6], v4: &SocketAddrV4) {
    // let mut tmp_buf = [0; 6];
    let port = v4.port();
    let octets = v4.ip().octets();
    into[0..4].copy_from_slice(&octets[..]);
    into[4] = (port >> 8) as u8;
    into[5] = (port & 0xFF) as u8;
}

fn deserialize_sock_addr_v4(from: &[u8; 6]) -> SocketAddrV4 {
    let mut ip_octets = [0; 4];
    ip_octets.copy_from_slice(&from[..4]);
    let port = (u16::from(from[4]) << 8) + u16::from(from[5]);
    SocketAddrV4::new(ip_octets.into(), port)
}

fn serialize_sock_addr_v6(into: &mut [u8; 18], v6: &SocketAddrV6) {
    // let mut tmp_buf = [0; 18];
    let port = v6.port();
    let octets = v6.ip().octets();
    into[0..16].copy_from_slice(&octets[..]);
    into[16] = (port >> 8) as u8;
    into[17] = (port & 0xFF) as u8;
}

fn deserialize_sock_addr_v6(from: &[u8; 18]) -> SocketAddrV6 {
    let mut ip_octets = [0; 16];
    ip_octets.copy_from_slice(&from[..16]);
    let port = (u16::from(from[16]) << 8) + u16::from(from[17]);
    SocketAddrV6::new(ip_octets.into(), port, 0, 0)
}

fn serialize_sock_addr(into: &mut [u8; 18], v6: &SocketAddrV6) {
    // let mut tmp_buf = [0; 18];
    let port = v6.port();
    let octets = v6.ip().octets();
    into[0..16].copy_from_slice(&octets[..]);
    into[16] = (port >> 8) as u8;
    into[17] = (port & 0xFF) as u8;
}

fn deserialize_sock_addr(from: &[u8]) -> Option<SocketAddr> {
    match from.len() {
        6 => {
            let mut ip_octets = [0; 6];
            ip_octets.copy_from_slice(from);
            Some(SocketAddr::V4(deserialize_sock_addr_v4(&ip_octets)))
        }
        18 => {
            let mut ip_octets = [0; 18];
            ip_octets.copy_from_slice(from);
            Some(SocketAddr::V6(deserialize_sock_addr_v6(&ip_octets)))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::BinStr;
    use serde_bytes::ByteBuf;
    use std::fmt::Write;
    use std::{fmt, str};

    use super::{
        DhtErrorResponse, DhtMessage, DhtMessageData, DhtMessageQuery, DhtMessageQueryAnnouncePeer,
        DhtMessageQueryGetPeers, DhtMessageResponse, DhtMessageResponseData,
        DhtMessageResponseFindNode, DhtMessageResponseGetPeers, DhtMessageResponseGetPeersData,
        TorrentId, DhtMessageQueryFindNode,
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
                        response: DhtMessageResponseData::FindNode(DhtMessageResponseFindNode {
                            id: TorrentId([
                                0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78,
                                0x79, 0x7a, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36,
                            ]),
                            nodes: Vec::new(),
                            nodes6: Vec::new(),
                        })
                    }),
                }
            ),
            (
                "find_node_query",
                b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t1:01:y1:qe",
                DhtMessage {
                    transaction: vec![0x30],
                    data: DhtMessageData::Query(DhtMessageQuery::FindNode(DhtMessageQueryFindNode {
                        id: TorrentId([
                            0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x30, 0x31,
                            0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
                        ]),
                        target: TorrentId([
                            0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78,
                            0x79, 0x7a, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36,
                        ]),
                        want: Default::default(),
                    }))
                }
            ),
            (
                "find_node_response",
                b"d1:rd2:id20:0123456789abcdefghij5:nodes6:\xc0\0\x02B\0Pe1:t1:01:y1:re",
                DhtMessage {
                    transaction: vec![0x30],
                    data: DhtMessageData::Response(DhtMessageResponse {
                        response: DhtMessageResponseData::FindNode(DhtMessageResponseFindNode {
                            id: TorrentId([
                                0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62,
                                0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a,
                            ]),
                            nodes: vec![
                                "192.0.2.66:80".parse().unwrap(),
                            ],
                            nodes6: vec![],
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
                                    "192.0.2.1:1234".parse().unwrap(),
                                    "[2001:db8::1]:1234".parse().unwrap(),
                                    "192.0.2.99:1024".parse().unwrap(),
                                    "[2001:db8::99]:1024".parse().unwrap(),
                                ],
                            }
                        })
                    })
                }
            ),
            (
                "get_peer_response_close_nodes",
                b"d1:rd2:id20:abcdefghij01234567895:nodes6:\xc0\0\x02B\0P5:token8:aoeusnthe1:t1:01:y1:re",
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
                                nodes: vec![
                                    "192.0.2.66:80".parse().unwrap(),
                                ],
                                nodes6: vec![],
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
                    data: DhtMessageData::Query(DhtMessageQuery::AnnouncePeer(DhtMessageQueryAnnouncePeer {
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
            println!("to-decode: {:?}", &BinStr(encoded_bytes));

            if let Ok(encoded) = bencode::to_bytes(&expected_decode) {
                println!("compare-bytes: {:?}", &BinStr(&encoded));
            }
            
            let decoded: DhtMessage = bencode::from_bytes(encoded_bytes).unwrap();
            assert_eq!(expected_decode, &decoded, "{} failed", name);
        }
    }

    #[test]
    fn dht_message_encodes() {
        for (name, expected_encode, message) in dht_message_get_table().iter() {
            println!("execution test case: encode {:?}", name);
            let encoded = bencode::to_bytes(message).unwrap();
            assert_eq!(BinStr(*expected_encode), BinStr(&encoded[..]), "{} failed", name);
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
