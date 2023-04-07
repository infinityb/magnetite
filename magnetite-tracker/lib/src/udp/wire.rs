use bytes::{Bytes, BytesMut};
use byteorder::{NetworkEndian, ByteOrder};
use smallvec::SmallVec;

use magnetite_common::TorrentId;

pub const CONNECT_MAGIC: u64 = 0x0417_2710_1980;
pub const ACTION_CONNECT: u32 = 0;
pub const ACTION_ANNOUNCE: u32 = 1;
pub const ACTION_SCRAPE: u32 = 2;
pub const ACTION_ERROR: u32 = 3;

#[derive(Debug)]
enum UnimplementedVoid {}

#[derive(Debug)]
pub struct Request {
    // action always implicit in payload variant
    pub conn_id: u64, // un-used for Connect req
    pub txid: u32,
    pub payload: RequestPayload,
}

impl Request {
    fn get_action(&self) -> u32 {
        match self.payload {
            RequestPayload::Connect => ACTION_CONNECT,
            RequestPayload::AnnounceRequest(..) => ACTION_ANNOUNCE,
            RequestPayload::Scrape(..) => ACTION_SCRAPE,
        }
    }
}

#[derive(Debug)]
pub enum RequestPayload {
    // Offset  Size            Name            Value
    // 0       64-bit integer  protocol_id     0x41727101980 // magic constant
    // 8       32-bit integer  action          0 // connect
    // 12      32-bit integer  transaction_id
    // 16
    Connect,
    // Offset  Size    Name    Value
    // 0       64-bit integer  connection_id
    // 8       32-bit integer  action          1 // announce
    // 12      32-bit integer  transaction_id
    // 16      20-byte string  info_hash
    // 36      20-byte string  peer_id
    // 56      64-bit integer  downloaded
    // 64      64-bit integer  left
    // 72      64-bit integer  uploaded
    // 80      32-bit integer  event           0 // 0: none; 1: completed; 2: started; 3: stopped
    // 84      32-bit integer  IP address      0 // default
    // 88      32-bit integer  key
    // 92      32-bit integer  num_want        -1 // default
    // 96      16-bit integer  port
    // 98
    AnnounceRequest(RequestPayloadAnnounceRequest),
    // scrape request:
    // Offset          Size            Name            Value
    // 0               64-bit integer  connection_id
    // 8               32-bit integer  action          2 // scrape
    // 12              32-bit integer  transaction_id
    // 16 + 20 * n     20-byte string  info_hash
    // 16 + 20 * N
    Scrape(RequestPayloadScrape),
}

#[derive(Debug)]
pub struct RequestPayloadScrape {
    // up to 74 of these
    pub info_hashes: SmallVec::<[TorrentId; 4]>,
}

#[derive(Debug)]
pub struct RequestPayloadAnnounceRequest {
    unimplemented: UnimplementedVoid,
}

#[derive(Debug)]
pub struct Response {
    // action always implicit in payload variant
    pub txid: u32,
    pub payload: ResponsePayload,
}

impl Response {
    pub fn as_connect(&self) -> Option<&ResponsePayloadConnect> {
        match self.payload {
            ResponsePayload::Connect(ref c) => Some(c),
            _ => None,
        }
    }

    pub fn as_announce(&self) -> Option<&ResponsePayloadAnnounce> {
        match self.payload {
            ResponsePayload::Announce(ref c) => Some(c),
            _ => None,
        }
    }

    pub fn as_scrape(&self) -> Option<&ResponsePayloadScrape> {
        match self.payload {
            ResponsePayload::Scrape(ref c) => Some(c),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct ScrapeResponseStats {
    pub seeders: u32,
    pub completed: u32,
    pub leechers: u32,
}

#[derive(Debug)]
pub struct AnnounceResponsePeerInfo {
    pub ip: u32,
    pub port: u16,
}

#[derive(Debug)]
pub struct ResponsePayloadConnect {
    pub conn_id: u64,
}

#[derive(Debug)]
pub struct ResponsePayloadAnnounce {
    pub interval: u32,
    pub leechers: u32,
    pub seeders: u32,
    pub peers: SmallVec::<[AnnounceResponsePeerInfo; 8]>,
}

#[derive(Debug)]
pub struct ResponsePayloadScrape {
    pub results: SmallVec::<[ScrapeResponseStats; 4]>,
}

#[derive(Debug)]
pub enum ResponsePayload {
    // Offset  Size            Name            Value
    // 0       32-bit integer  action          0 // connect
    // 4       32-bit integer  transaction_id
    // 8       64-bit integer  connection_id
    // 16
    Connect(ResponsePayloadConnect),
    // Offset      Size            Name            Value
    // 0           32-bit integer  action          1 // announce
    // 4           32-bit integer  transaction_id
    // 8           32-bit integer  interval
    // 12          32-bit integer  leechers
    // 16          32-bit integer  seeders
    // 20 + 6 * n  32-bit integer  IP address
    // 24 + 6 * n  16-bit integer  TCP port
    // 20 + 6 * N
    Announce(ResponsePayloadAnnounce),
    // scrape response:
    // Offset      Size            Name            Value
    // 0           32-bit integer  action          2 // scrape
    // 4           32-bit integer  transaction_id
    // 8 + 12 * n  32-bit integer  seeders
    // 12 + 12 * n 32-bit integer  completed
    // 16 + 12 * n 32-bit integer  leechers
    // 8 + 12 * N
    Scrape(ResponsePayloadScrape),
}

pub trait BitTorrentSerialize {
    type Item;

    fn serialize(&self, item: &Self::Item, buf: &mut BytesMut) -> anyhow::Result<()>;
}

pub trait BitTorrentDeserialize {
    type Item;

    fn deserialize(&self, buf: &Bytes) -> anyhow::Result<Self::Item>;
}

#[derive(Debug)]
pub enum RequestVisitorAddressFamily {
    Ipv4,
    Ipv6,
}

#[derive(Debug)]
pub struct RequestVisitor {
    pub address_family: RequestVisitorAddressFamily,
}

impl BitTorrentSerialize for RequestVisitor {
    type Item = Request;
    
    fn serialize(&self, item: &Self::Item, buf: &mut BytesMut) -> anyhow::Result<()> {

        let connect_id = item.conn_id;
        
        match item.payload {
            RequestPayload::Connect => {
                let mut scratch = [0; 16];
                NetworkEndian::write_u64(&mut scratch[0..][..8], CONNECT_MAGIC);
                NetworkEndian::write_u32(&mut scratch[8..][..4], ACTION_CONNECT);
                NetworkEndian::write_u32(&mut scratch[12..][..4], item.txid);
                buf.extend_from_slice(&scratch[..16]);
                return Ok(());
            },
            RequestPayload::AnnounceRequest(..) => {
                unimplemented!("unconstructable");
            },
            RequestPayload::Scrape(ref scrape) => {
                let mut scratch = [0; 16];
                NetworkEndian::write_u64(&mut scratch[0..][..8], item.conn_id);
                NetworkEndian::write_u32(&mut scratch[8..][..4], ACTION_SCRAPE);
                NetworkEndian::write_u32(&mut scratch[12..][..4], item.txid);
                buf.extend_from_slice(&scratch[..16]);
                for hash in &scrape.info_hashes {
                    buf.extend_from_slice(hash.as_bytes());
                }
            },
        }

        Ok(())
    }
}

impl BitTorrentDeserialize for RequestVisitor {
    type Item = Request;
    
    fn deserialize(&self, buf: &Bytes) -> anyhow::Result<Self::Item> {
        if buf.len() < 16 {
            return Err(anyhow::format_err!("not enough bytes for Request"));
        }

        let conn_id = NetworkEndian::read_u64(&buf[0..][..8]);
        let action = NetworkEndian::read_u32(&buf[8..][..4]);
        let txid = NetworkEndian::read_u32(&buf[12..][..4]);

        match action {
            ACTION_CONNECT => {
                if conn_id != CONNECT_MAGIC {
                    return Err(anyhow::format_err!("bad connect magic"));
                }
                return Ok(Request {
                    conn_id,
                    txid,
                    payload: RequestPayload::Connect,
                });
            }
            ACTION_ANNOUNCE => {
                return Err(anyhow::format_err!("unimplemented"));
            }
            ACTION_SCRAPE => {
                let mut ih_buf = &buf[16..];
                let mut info_hashes = SmallVec::<[TorrentId; 4]>::new();
                while ih_buf.len() >= 20 {
                    let tid = TorrentId::from_slice(&ih_buf[..20]).unwrap();
                    info_hashes.push(tid);
                    ih_buf = &ih_buf[20..];
                }
                return Ok(Request {
                    conn_id,
                    txid,
                    payload: RequestPayload::Scrape(RequestPayloadScrape {
                        info_hashes,
                    }),
                });
            }
            ACTION_ERROR => return Err(anyhow::format_err!("unimplemented")),
            _ => return Err(anyhow::format_err!("unimplemented")),
        }
    }
}

#[derive(Debug)]
pub struct ResponseVisitor;

impl BitTorrentSerialize for ResponseVisitor {
    type Item = Response;
    
    fn serialize(&self, item: &Self::Item, buf: &mut BytesMut) -> anyhow::Result<()> {

        // let mut scratch = [0; 16];
        // NetworkEndian::write_u64(&mut scratch[0..][..8], 0x41727101980);
        // NetworkEndian::write_u32(&mut scratch[8..][..4], 0);
        // NetworkEndian::write_u32(&mut scratch[12..][..4], item.txid);
        // buf.extend_from_slice(&scratch[..]);

        unimplemented!();
    }
}

impl BitTorrentDeserialize for ResponseVisitor {
    type Item = Response;
    
    fn deserialize(&self, buf: &Bytes) -> anyhow::Result<Self::Item> {
        if buf.len() < 8 {
            return Err(anyhow::format_err!("not enough bytes for Response"));
        }

        let action = NetworkEndian::read_u32(&buf[0..][..4]);
        let txid = NetworkEndian::read_u32(&buf[4..][..4]);
        let mut rest = &buf[8..];

        match action {
            ACTION_CONNECT => {
                let conn_id = NetworkEndian::read_u64(&rest[..8]);
                return Ok(Response {
                    txid,
                    payload: ResponsePayload::Connect(ResponsePayloadConnect {
                        conn_id,
                    }),
                });
            }
            ACTION_ANNOUNCE => {
                return Err(anyhow::format_err!("unimplemented"));
            }
            ACTION_SCRAPE => {
                let mut results = SmallVec::new();
                while rest.len() >= 12 {
                    let seeders = NetworkEndian::read_u32(&rest[..4]);
                    let completed = NetworkEndian::read_u32(&rest[4..8]);
                    let leechers = NetworkEndian::read_u32(&rest[8..12]);
                    rest = &rest[12..];

                    results.push(ScrapeResponseStats {
                        seeders,
                        completed,
                        leechers,
                    });
                }
                return Ok(Response {
                    txid,
                    payload: ResponsePayload::Scrape(ResponsePayloadScrape {
                        results,
                    }),
                });
            }
            ACTION_ERROR => return Err(anyhow::format_err!("unimplemented")),
            _ => return Err(anyhow::format_err!("unimplemented")),
        }

    }
}
