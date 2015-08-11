extern crate mio;
extern crate byteorder;
extern crate metorrent_common;

mod reset;
mod conn;

pub use conn::BufferedConn;

use byteorder::{BigEndian, ByteOrder};
use mio::buf::{RingBuf, Buf, MutBuf};
use metorrent_common::message::{Message, Error};

pub trait RingParser {
    type Item;
    type Err;

    fn parse_msg(&mut self) -> Result<Self::Item, Self::Err>;
}

fn get_length<B>(ringbuf: &mut B) -> Result<u32, Error> where B: Buf {
    let mut lenbuf = [0; 4];
    for byte in lenbuf.iter_mut() {
        *byte = try!(ringbuf.read_byte().ok_or(Error::Truncated));
    }
    Ok(BigEndian::read_u32(&lenbuf[..]))
}

impl RingParser for ::mio::buf::RingBuf {
    type Item = Message;
    type Err = Error;

    fn parse_msg(&mut self) -> Result<Self::Item, Self::Err> {
        let mut guard = reset::ResetGuard::new(self);

        let length = try!(get_length(&mut *guard)) as usize;
        if Buf::remaining(&*guard) < length {
            return Err(Error::Truncated);
        }

        // TODO: remove allocation
        let mut buf = Vec::with_capacity(length);
        for _ in 0..length {
            buf.push(try!(guard.read_byte().ok_or(Error::Truncated)));
        }

        let message = try!(Message::parse(&buf));
        guard.commit();
        Ok(message)
    }
}