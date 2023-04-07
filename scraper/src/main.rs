use magnetite_tracker_lib::udp::wire::{self as uw, BitTorrentSerialize, BitTorrentDeserialize};

use bytes::BytesMut;
use bin_str::BinStr;
use magnetite_common::TorrentId;
use std::net::UdpSocket;

fn main() {
    println!("Hello, world!");
    let req_visitor = uw::RequestVisitor {
        address_family: uw::RequestVisitorAddressFamily::Ipv4,
    };
    let resp_visitor = uw::ResponseVisitor;

    let socket = UdpSocket::bind("0.0.0.0:34254").unwrap();

    let mut info_hashes: Vec<TorrentId> = Vec::new();
    info_hashes.push("e220434257213458c3c84bd914dd4e3c896cc514".parse().unwrap());
    info_hashes.push("f22ef47575048b9e14d94ddf1c2aa93221ef0270".parse().unwrap());
    info_hashes.push("966471f5da12e72dc40cc4cfb0a2bde773bc74ca".parse().unwrap());
    info_hashes.push("ecbfaddaf35b27cbdda276cb5713cfb2ce118a0f".parse().unwrap());
    info_hashes.push("788f12afc1f6ae61725144c5c03a18df50e15810".parse().unwrap());

    let mut send_q = BytesMut::new();
    let req = uw::Request {
        conn_id: uw::CONNECT_MAGIC,
        txid: 0x99AA_AAA9,
        payload: uw::RequestPayload::Connect
    };
    req_visitor.serialize(&req, &mut send_q).unwrap();
    println!("sending: {:?} -- {:?}", req, BinStr(&send_q));
    socket.send_to(&send_q[..], "tracker.tiny-vps.com:6969").unwrap();

    let mut buf = [0; 4094];
    let (rx_byte_count, src_addr) = socket.recv_from(&mut buf).unwrap();
    let mut b = BytesMut::new();
    b.extend_from_slice(&buf[..rx_byte_count]);
    let b = b.freeze();
    
    println!("got-raw: {:?}", BinStr(&b[..]));
    let deserialized = resp_visitor.deserialize(&b).unwrap();
    println!("got    : {:?}", deserialized);

    let connect = deserialized.as_connect().unwrap();
    let conn_id = connect.conn_id;

    let req = uw::Request {
        conn_id: conn_id,
        txid: 0x99AA_AAA9,
        payload: uw::RequestPayload::Scrape(uw::RequestPayloadScrape {
            info_hashes: info_hashes.into(),
        }),
    };
    let mut send_q = BytesMut::new();
    req_visitor.serialize(&req, &mut send_q).unwrap();
    println!("sending: {:?} -- {:?}", req, BinStr(&send_q));
    socket.send_to(&send_q[..], "tracker.tiny-vps.com:6969").unwrap();

    // for i in 0..2 {
        let mut buf = [0; 4094];
        let (rx_byte_count, src_addr) = socket.recv_from(&mut buf).unwrap();
        let mut b = BytesMut::new();
        b.extend_from_slice(&buf[..rx_byte_count]);
        let b = b.freeze();
        
        println!("got-raw: {:?}", BinStr(&b[..]));
        let deserialized = resp_visitor.deserialize(&b).unwrap();
        println!("got    : {:?}", deserialized);
    //}
}
