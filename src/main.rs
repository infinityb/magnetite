#![feature(raw, slice_bytes, iter_arith)]

#[macro_use]
extern crate log;

extern crate byteorder;
extern crate mio;
extern crate sha1;
extern crate bytes;

use std::path::PathBuf;
use std::collections::HashMap;

use mio::util::Slab;
use mio::{EventLoop, EventSet, Token};
use mio::tcp::{TcpListener, TcpSocket, TcpStream};

mod util;
mod connection;
mod message_types;
mod bitfield;
mod storage;

use util::sha1::Sha1;
use storage::Storage;
use bitfield::Bitfield;
use connection::{Handshake, ConnectionState};
use message_types::{Message, RingParser};

fn main() {
    println!("Hello, world!");
}

struct Torrent {
    info: connection::TorrentInfo,
    pieces: Bitfield,

    // Dynamic dispatch because disks are expensive anyway.
    storage: Box<Storage>,
}


struct HandshakeConn {
    peer_addr: std::net::SocketAddr,
    conn: TcpStream,
    state: Handshake,
}

enum HandshakeError {
    Continue,
}

impl HandshakeError {
    pub fn is_fatal(&self) -> bool {
        match *self {
            HandshakeError::Continue => false,
            _ => true,
        }
    }
}

impl HandshakeConn {
    pub fn ready(
        &mut self,
        client: &mut TorrentClient,
        events: EventSet
    ) -> Result<(), ()> {
        let mut readable = events.is_readable();
        while readable {
            match self.state.try_read() {
                Ok(()) => (),
                Err(msg) => {
                    warn!("handshake({}): {}", self.peer_addr, msg);
                    return Err(());
                }
            }
        }

        if let Some(info_hash) = self.state.get_info_hash() {
            let torrent = try!(client.torrents.get(&info_hash).ok_or(()));
            self.state.update_torrent_info(torrent.info);
        }

        let mut writable = events.is_writable();
        while writable {
            match self.state.try_write() {
                Ok(()) => (),
                Err(msg) => {
                    warn!("handshake({}): {}", self.peer_addr, msg);
                    return Err(());
                }
            }
        }

        Ok(())
    }
}

struct PeerConn {
    peer_addr: std::net::SocketAddr,
    conn: TcpStream,
    state: ConnectionState,
}

impl PeerConn {
    // eloop: &mut EventLoop<Server>, 
    pub fn ready(
        &mut self,
        client: &mut TorrentClient,
        events: EventSet
    ) -> Result<(), ()> {
        use mio::{TryWrite, TryRead};
        
        let mut readable = events.is_readable();
        while readable {
            match self.conn.try_read_buf(&mut self.state.ingress_buf) {
                Ok(Some(0)) => {
                    readable = false;
                    info!("peer({}): filled their read buffer.", self.peer_addr);
                },
                Ok(Some(_)) => (),
                Ok(None) => readable = false,
                Err(err) => {
                    warn!("peer({}) I/O error: {}", self.peer_addr, err);
                    return Err(());
                }
            }
            let info_hash = self.state.torrent_info.info_hash;
            for msg in self.state.ingress_buf.parse_msg() {
                try!(client.handle(&info_hash, &msg));
                try!(self.state.handle(&msg));
            }
        }

        let mut writable = events.is_writable();
        while writable {
            match self.conn.try_write_buf(&mut self.state.egress_buf) {
                // Finished writing: break.
                Ok(Some(0)) => {
                    writable = false;
                    info!("peer({}): finished writeout", self.peer_addr);
                }
                // Wrote some stuff: keep trying.
                Ok(Some(_)) => (),
                // EWOULDBLOCK: break; try again next time.
                Ok(None) => writable = false, 
                // I/O Error: kill the connection.
                Err(err) => {
                    warn!("peer({}) I/O error: {}", self.peer_addr, err);
                    return Err(());
                }
            }
        }
        
        Ok(())
    }
}

struct TrackerConn {
    conn: TcpStream,
}

impl TrackerConn {
    // eloop: &mut EventLoop<Server>, 
    pub fn ready(
        &mut self,
        client: &mut TorrentClient,
        events: EventSet
    ) -> Result<(), ()> {
        Ok(())
    }
}

struct TorrentClient {
    bind_socket: TcpListener,
    connections: Slab<PeerConn>,
    handshakes: Slab<HandshakeConn>,
    trackers: Slab<TrackerConn>,
    torrents: HashMap<Sha1, Torrent>,
}

impl TorrentClient {
    pub fn new(bind: TcpListener) -> TorrentClient {
        TorrentClient {
            bind_socket: bind,
            connections: Slab::new_starting_at(Token(4096), 4096),
            handshakes: Slab::new_starting_at(Token(8192), 128),
            trackers: Slab::new_starting_at(Token(8320), 128),
            torrents: HashMap::new(),
        }
    }

    pub fn handle(&mut self, ih: &Sha1, msg: &Message) -> Result<(), ()> {
        //
    }
}

impl ::mio::Handler for TorrentClient {
    type Timeout = ();
    type Message = ();

    fn ready(&mut self, eloop: &mut EventLoop<TorrentClient>, token: Token, events: EventSet) {
        if 4096 <= token.as_usize() && token.as_usize() < 8192 {
            match self.connections[token].ready(self, events) {
                Ok(()) => println!("torrent_client.connections[token].ready(...) => OK"),
                Err(()) => println!("torrent_client.connections[token].ready(...) => ERR"),
            }
        }
        if 8192 <= token.as_usize() && token.as_usize() < 8320 {
            match self.handshakes[token].ready(self, events) {
                Ok(()) => println!("torrent_client.handshakes[token].ready(...) => OK"),
                Err(()) => println!("torrent_client.handshakes[token].ready(...) => ERR"),
            }
        }
        if 8320 <= token.as_usize() && token.as_usize() < 8448 {
            match self.trackers[token].ready(self, events) {
                Ok(()) => println!("torrent_client.trackers[token].ready(...) => OK"),
                Err(()) => println!("torrent_client.trackers[token].ready(...) => ERR"),
            }
        }
    }
}