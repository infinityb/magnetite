use std::net::{SocketAddr, IpAddr, SocketAddrV4};
use std::time::{Instant, Duration};
use std::fs::File;
use std::io::{self, Write};
use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use pcap::{Device, Capture};
use packet::Packet;
use magnetite_tracker_lib::{Tracker, AnnounceCtx};
use dht::wire::{DhtMessageData, DhtMessageQuery, DhtMessage, DhtMessageQueryAnnouncePeer};

fn main() -> io::Result<()> {
    let main_device = Device::lookup().unwrap();
    let mut cap = Capture::from_device(main_device).unwrap()
                      .promisc(false)
                      .snaplen(8192)
                      .open()
                      .unwrap();

    let mut next_emit = Instant::now() + Duration::new(10, 0);
    cap.filter("port 3019", true).unwrap();
    let mut tracker = Tracker::new();

    let mut sent_to: BTreeMap<SocketAddrV4, u64> = BTreeMap::new();
    // from
    while let Ok(packet) = cap.next() {
        let now = Instant::now();

        if next_emit < now {
            while next_emit < now {
                next_emit += Duration::new(10, 0);
            }
            let mut ff = File::options()
                .create(true)
                .write(true)
                .truncate(true)
                .open("stats.txt.tmp")
                ?;

            let mut v: Vec<_> = sent_to.iter().map(|(k, v)| (*k, *v)).collect();
            v.sort_by_key(|(k, v)| (u64::max_value() - *v, *k));

            for (k, v) in &v {
                write!(&mut ff, "{}\t{}\n", v, k)?;
            }
            
            drop(ff);
            std::fs::rename("stats.txt.tmp", "stats.txt")?;
            eprintln!("wrote");
        }

        let pp;
        if let Ok(pp_tmp) = decode_ether_payload(packet.data) {
            pp = pp_tmp;
        } else {
            continue;
        }
        if pp.data.data == b"d1:eli203e17:No transaction IDe1:v4:lt\rp1:y1:ee" {
            continue;
        }
        if pp.data.data == b"\xff\xff\xff\xffTSource Engine Query\0" {
            continue;
        }

        let fm;
        match bencode::from_bytes_allow_unconsumed::<DhtMessage>(&pp.data.data[..]) {
            Ok(fm_tmp) => fm = fm_tmp,
            Err(err) => {
                //println!("[from {}:{}] msg: {:?} --> {}", pp.from, pp.data.from, BinStr(&pp.data.data[..]), err);
                continue;
            }
        };

        let from = pp.from_addr();
        let to = pp.to_addr();
        if *from.ip() == Ipv4Addr::from([10, 0, 1, 2]) {
            *sent_to.entry(to).or_insert(0) += 1;
        } else {
            // *sent_to.entry(from).or_insert(0) += 1;
        }

        // let client_sock = SocketAddr::new(IpAddr::V4(pp.from), pp.data.from);
        // let peer_id;
        // let response: ();
        // match fm.data {
        //     DhtMessageData::Query(DhtMessageQuery::AnnouncePeer(ref ap)) => {
        //         //
        //     }
        //     _ => continue,
        // }
    }

    Ok(())
}


// fn handle_query_announce_peer(
//     tracker: &mut Tracker,
//     message: &DhtMessage,
//     m_ap: &DhtMessageQueryAnnouncePeer,
//     // environment: &NodeEnvironment,
//     client_addr: &SocketAddr,
// ) {
//     if !bm.check_token(&m_ap.token, client_addr) {
//         return DhtMessage {
//             transaction: message.transaction.clone(),
//             data: DhtMessageData::Error(DhtErrorResponse {
//                 error: (203, "Bad token".to_string()),
//             }),
//         };
//     }

//     tracker.insert_announce(&m_ap.info_hash, client_addr, &AnnounceCtx {
//         now: Instant::now(),
//     });

//     DhtMessage {
//         transaction: message.transaction.clone(),
//         data: DhtMessageData::Response(DhtMessageResponse {
//             response: DhtMessageResponseData::FindNode(DhtMessageResponseFindNode {
//                 id: bm.self_peer_id,
//                 nodes: Default::default(),
//                 nodes6: Default::default(),
//             }),
//         }),
//     }
// }


struct Ipv4 {
    from:std::net::Ipv4Addr,
    to: std::net::Ipv4Addr,
    data: Udp,
}

struct Udp {
    from: u16,
    to: u16,
    data: Vec<u8>,
}

impl Ipv4 {
    fn from_addr(&self) -> SocketAddrV4 {
        SocketAddrV4::new(self.from, self.data.from)
    }

    fn to_addr(&self) -> SocketAddrV4 {
        SocketAddrV4::new(self.to, self.data.to)
    }
}

fn decode_ether_payload(packet_data: &[u8]) -> packet::Result<Ipv4> {
    let pp = packet::ether::Packet::new(packet_data)?;
    match pp.protocol() {
        packet::ether::Protocol::Ipv4 => decode_ipv4_payload(pp.payload()),
        // hack..
        _ => Err(packet::Error::SmallBuffer),
    }
}

fn decode_ipv4_payload(packet_data: &[u8]) -> packet::Result<Ipv4> {
    let pp = packet::ip::v4::Packet::new(packet_data)?;
    match pp.protocol() {
        packet::ip::Protocol::Udp => Ok(Ipv4 {
            from: pp.source(),
            to: pp.destination(),
            data: decode_udp_payload(pp.payload())?,
        }),
        // hack..
        _ => Err(packet::Error::SmallBuffer),
    }
}

fn decode_udp_payload(packet_data: &[u8]) -> packet::Result<Udp> {
    let pp = packet::udp::Packet::new(packet_data)?;
    Ok(Udp {
        from: pp.source(),
        to: pp.destination(),
        data: pp.payload().to_vec(),
    })
}

