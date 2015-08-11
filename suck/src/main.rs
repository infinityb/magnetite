extern crate metorrent_common;
extern crate metorrent_middle;
extern crate metorrent_util;

mod mt {
    pub use metorrent_common as common;
    pub use metorrent_middle as middle;
    pub use metorrent_util as util;
}

use std::env;
use std::process;
use std::ffi::OsString;
use std::net::{SocketAddr, AddrParseError};

#[derive(Debug)]
enum RuntimeErr {
    Arguments(ArgsErr),
}


impl From<ArgsErr> for RuntimeErr {
    fn from(e: ArgsErr) -> RuntimeErr {
        RuntimeErr::Arguments(e)
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

fn main2() -> Result<(), RuntimeErr> {
    use mt::middle::BufferedConn;
    
    let args = try!(ProgramArgs::from_args(env::args_os()));
    println!("args = {:#?}", args);



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
            }
        }
    }
}
