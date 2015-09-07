extern crate mio;
extern crate rand;
extern crate metorrent_common;
extern crate metorrent_middle;
extern crate metorrent_util;

use std::io::{self, Read, Write};
use std::env;
use std::process;
use std::ffi::OsString;
use std::net::{TcpStream, SocketAddr, AddrParseError};
use std::collections::VecDeque;
use std::fs::File;

use mio::buf::RingBuf;
use rand::Rng;

mod mt {
    pub use metorrent_common as common;
    pub use metorrent_middle as middle;
    pub use metorrent_util as util;
}

use mt::common::get_info_hash;
use mt::common::message::Message;
use mt::util::Sha1;
use mt::middle::Protocol;

#[derive(Debug)]
enum RuntimeErr {
    Arguments(ArgsErr),
    PeerError(String),
    Io(io::Error),
}


impl From<ArgsErr> for RuntimeErr {
    fn from(e: ArgsErr) -> RuntimeErr {
        RuntimeErr::Arguments(e)
    }
}

impl From<io::Error> for RuntimeErr {
    fn from(e: io::Error) -> RuntimeErr {
        RuntimeErr::Io(e)
    }
}

#[derive(Debug)]
enum ArgsErr {
    InvalidArguments,
    InvalidArgument(&'static str),
    InvalidTargetHost(AddrParseError),
}

impl From<AddrParseError> for ArgsErr {
    fn from(e: AddrParseError) -> ArgsErr {
        ArgsErr::InvalidTargetHost(e)
    }
}

impl ArgsErr {
    fn print_usage(&self) -> bool {
        use self::ArgsErr::*;
        match *self {
            InvalidArguments => true,
            InvalidArgument(_) => false,
            InvalidTargetHost(_) => false,
        }
    }
}

#[derive(Debug)]
struct ProgramArgs {
    program_name: OsString,
    torrent_file: OsString,
    target_file: OsString,
    peer_addr: SocketAddr,
}

impl ProgramArgs {
    pub fn from_args<I>(mut arguments: I) -> Result<ProgramArgs, ArgsErr>
        where
            I: Iterator<Item=OsString> {
        
        use self::ArgsErr::InvalidArguments;
        let program_name = try!(arguments.next().ok_or(InvalidArguments));
        let torrent_file = try!(arguments.next().ok_or(InvalidArguments));
        let target_file = try!(arguments.next().ok_or(InvalidArguments));
        let peer_addr: SocketAddr = try!(arguments.next()
            .ok_or(InvalidArguments)
            .and_then(|s| s.into_string().map_err(|_|
                ArgsErr::InvalidArgument("peer_addr")))
            .and_then(|s| s.parse().map_err(From::from) ));
        
        Ok(ProgramArgs {
            program_name: program_name,
            torrent_file: torrent_file,
            target_file: target_file,
            peer_addr: peer_addr,
        })
    }
}

struct BitTorrentProtocol {
    info_hash: Sha1,
    client_id: Sha1,
    foreign_id: Sha1,
    handshook: false,
    egress_queue: VecDeque<Message>,
    ingress_queue: VecDeque<Message>,
}

impl Protocol for BitTorrentProtocol {
    fn try_read(&mut self, ingress: &mut RingBuf) -> Result<(), ()> {
        use mt::middle::RingParser;
        use mt::common::message::{Message, Error};

        loop {
            match RingParser::parse(ingress) {
                Ok(msg) => self.ingress_queue.push_back(msg),
                Err(Error::Unknown(_)) => (),
                Err(Error::Truncated) => break,
                Err(Error::Invalid) => return Err(()),
                Err(Error::LargeBlock) => return Err(()),
            }
        }
        Ok(())
    }

    fn try_write(&mut self, egress: &mut RingBuf) -> Result<(), ()> {
        use mt::middle::{RingAppender, RingAppenderError};

        while let Some(msg) = self.egress_queue.pop_front() {
            match egress.append(&msg) {
                Ok(()) => (),
                Err(RingAppenderError::Full) => {
                    self.egress_queue.push_front(msg);
                    break;
                }
            }
        }

        Ok(())
    }
}

impl BitTorrentProtocol {
    pub fn new(info_hash: Sha1, client_id: Sha1) -> Self {
        BitTorrentProtocol {
            info_hash: info_hash,
            client_id: client_id,
            egress_queue: VecDeque::new(),
            ingress_queue: VecDeque::new(),
        }
    }

    pub fn pop_msg(&mut self) -> Option<Message> {
        self.ingress_queue.pop_front()
    }
}


fn main2() -> Result<(), RuntimeErr> {
    use mt::middle::BlockingBufferedConn;

    let args = try!(ProgramArgs::from_args(env::args_os()));
    println!("args = {:#?}", args);

    // torrent
    let mut file = File::open(&args.torrent_file).unwrap();
    let mut torrent_file = Vec::new();
    try!(file.read_to_end(&mut torrent_file));
    drop(file);

    let info_hash = get_info_hash(&torrent_file).ok().unwrap();
    println!("info_hash: {}", info_hash);


    // let mut storage = Storage::new();
    let mut conn = TcpStream::connect(&args.peer_addr).unwrap();
    println!("connected");

    let mut rng = ::rand::thread_rng();
    let mut client_id = rng.gen();
    let mut proto = BitTorrentProtocol::new(info_hash, client_id);
    let mut buffered = BlockingBufferedConn::new(proto, conn).unwrap();

    let handshake = mt::common::HandshakeBuf::build(
        b"\x00\x00\x00\x00\x00\x00\x00\x00",
        info_hash.as_bytes(),
        client_id.as_bytes()).unwrap();

    buffered.egress_mut().write_all(handshake.as_bytes()).unwrap();
    let () = try!(buffered.flush_write());
    println!("flushed handshake: {:?}", handshake.as_bytes());

    loop {
        let () = try!(buffered.populate_read());
        // if let Err(err) = proto.try_read(buffered.ingress_mut()) {
        //     let msg = format!("peer error: {:?}", err);
        //     return Err(RuntimeErr::PeerError(msg));
        // }
        match buffered.get_mut().pop_msg() {
            Some(msg) => println!("get msg {:?}", msg),
            None => println!("??"),
        }
        // if let Err(err) = proto.try_write(buffered.egress_mut()) {
        //     let msg = format!("peer error: {:?}", err);
        //     return Err(RuntimeErr::PeerError(msg));
        // }
        let () = try!(buffered.flush_write());
    }
    Ok(())
}

fn print_usage() -> () {
    let program_name = env::args().next().unwrap();
    println!("USAGE: {} [torrent-file] [target-file] [peer-address]",
        program_name);
}

fn main() {
    if let Err(err) = main2() {
        match err {
            RuntimeErr::Arguments(ref argerr) if argerr.print_usage() => {
                print_usage();
                process::exit(1);
            },
            RuntimeErr::Arguments(ref argerr) => {
                println!("Argument parse error: {:?}", argerr);
                process::exit(1);
            },
            RuntimeErr::PeerError(ref msg) => {
                println!("peer error: {}", msg);
                process::exit(1);
            },
           RuntimeErr::Io(ref err) => {
                println!("I/O error: {}", err);
                process::exit(1);
            }
        }
    }
}
