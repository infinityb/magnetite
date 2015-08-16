extern crate mio;
extern crate byteorder;
extern crate metorrent_common;

mod reset;
mod conn;

pub use conn::{
    BufferedConn,
    BlockingBufferedConn,
    Protocol,
};

use byteorder::{BigEndian, ByteOrder};
use mio::buf::{RingBuf, Buf, MutBuf};
use metorrent_common::message::{Message, Error};

pub trait RingParser<T> {
    type Err;

    fn parse(&mut self) -> Result<T, Self::Err>;
}

pub trait RingAppender<T> {
    type Err;

    fn append(&mut self, val: &T) -> Result<(), Self::Err>;
}

fn get_length<B>(ringbuf: &mut B) -> Result<u32, Error> where B: Buf {
    let mut lenbuf = [0; 4];
    for byte in lenbuf.iter_mut() {
        *byte = try!(ringbuf.read_byte().ok_or(Error::Truncated));
    }
    Ok(BigEndian::read_u32(&lenbuf[..]))
}

impl RingParser<Message> for RingBuf {
    type Err = Error;

    fn parse(&mut self) -> Result<Message, Self::Err> {
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

pub enum RingAppenderError {
    Full,
}

impl RingAppenderError {
    pub fn is_permanent(&self) -> bool {
        match *self {
            RingAppenderError::Full => false,
        }
    }
}

impl RingAppender<Message> for RingBuf {
    type Err = RingAppenderError;

    fn append(&mut self, msg: &Message) -> Result<(), Self::Err> {
        let msg = msg.to_bytes();
        if MutBuf::remaining(self) < msg.len() {
            return Err(RingAppenderError::Full);
        }
        let mut written = 0;
        while 0 < msg.len() {
            let wrote = self.write_slice(&msg[written..]);
            assert!(wrote > 0);
            written += wrote;
        }
        Ok(())
    }
}