#![feature(custom_derive, plugin, result_expect)]
#![plugin(serde_macros)]
extern crate serde;
extern crate num;

use std::io;

use serde::de;

mod error;
mod number;
mod value;

pub use self::value::Value;
use self::error::{Result, Error, ErrorCode};
use self::number::{NumberParser, NumberError};

#[inline]
fn is_digit(val: u8) -> bool {
    b'0' <= val && val <= b'9'
}

pub struct Deserializer<Iter: Iterator<Item=io::Result<u8>>> {
    rdr: Iter,
    ch: Option<u8>,
    max_buf: usize,
}

impl<Iter> Deserializer<Iter>
    where Iter: Iterator<Item=io::Result<u8>>,
{
    /// Creates the bencode parser from an `std::iter::Iterator`.
    #[inline]
    pub fn new(rdr: Iter) -> Deserializer<Iter> {
        Deserializer {
            rdr: rdr,
            ch: None,
            max_buf: 1 << 24,
        }
    }

    fn eat_char(&mut self) {
        self.ch = None;
    }

    fn next_char(&mut self) -> Result<Option<u8>> {
        match self.ch.take() {
            Some(ch) => Ok(Some(ch)),
            None => {
                match self.rdr.next() {
                    Some(Err(err)) => Err(Error::IoError(err)),
                    Some(Ok(ch)) => Ok(Some(ch)),
                    None => Ok(None),
                }
            }
        }
    }

    #[inline]
    pub fn end(&mut self) -> Result<()> {
        if try!(self.eof()) {
            Ok(())
        } else {
            Err(self.error(ErrorCode::TrailingCharacters))
        }
    }

    fn eof(&mut self) -> Result<bool> {
        Ok(try!(self.peek()).is_none())
    }

    fn peek(&mut self) -> Result<Option<u8>> {
        match self.ch {
            Some(ch) => Ok(Some(ch)),
            None => {
                match self.rdr.next() {
                    Some(Err(err)) => Err(Error::IoError(err)),
                    Some(Ok(ch)) => {
                        self.ch = Some(ch);
                        Ok(self.ch)
                    }
                    None => Ok(None),
                }
            }
        }
    }

    fn error(&mut self, reason: ErrorCode) -> Error {
        Error::SyntaxError(reason)
    }

    fn parse_integer<V>(&mut self, mut visitor: V) -> Result<V::Value>
        where V: de::Visitor,
    {
        let mut parser = number::NumberParser::new();

        let mut cont = true;
        while cont {
            match try!(self.next_char()) {
                Some(byte) => {
                    if parser.push_byte(byte) {
                        break;
                    }
                }
                None => return Err(self.error(ErrorCode::EOFWhileParsingInteger)),
            }
        }
        match parser.end() {
            Ok(val) => visitor.visit_i64(val),
            Err(err) => Err(From::from(err)),
        }
    }

    fn parse_bytea<V>(&mut self, mut visitor: V) -> Result<V::Value>
        where V: de::Visitor,
    {
        // The prefix (length) parser
        let mut parser = number::NumberParser::with_stop(b':');

        loop {
            match try!(self.next_char()) {
                Some(byte) => {
                    if parser.push_byte(byte) {
                        break;
                    }
                }
                None => return Err(self.error(ErrorCode::EOFWhileParsingString)),
            }
        }
        let length: usize = try!(parser.end());
        if self.max_buf < length {
            return Err(self.error(ErrorCode::ExcessiveAllocation));
        }
        let mut val = Vec::with_capacity(length);
        for i in 0..length {
            match try!(self.next_char()) {
                Some(byte) => val.push(byte),
                None => return Err(self.error(ErrorCode::EOFWhileParsingString)),
            }
        }
        visitor.visit_byte_buf(val)
    }

    fn parse_value<V>(&mut self, mut visitor: V) -> Result<V::Value>
        where V: de::Visitor,
    {

        let value = match try!(self.peek()) {
            Some(b'd') => {
                self.eat_char();
                visitor.visit_map(MapVisitor::new(self))
            },
            Some(b'i') => {
                self.eat_char();
                self.parse_integer(visitor)
            },
            Some(b'l') => {
                self.eat_char();
                visitor.visit_seq(SeqVisitor::new(self))
            },
            Some(byte) if is_digit(byte) => {
                self.parse_bytea(visitor)
            },
            Some(_) => return Err(self.error(ErrorCode::InvalidByte)),
            None => return Err(self.error(ErrorCode::EOFWhileParsingValue)),
        };
        match value {
            Ok(value) => Ok(value),
            Err(Error::SyntaxError(code)) => Err(self.error(code)),
            Err(err) => Err(err),
        }
    }
}

impl<Iter> de::Deserializer for Deserializer<Iter>
    where Iter: Iterator<Item=io::Result<u8>>,
{
    type Error = Error;

    #[inline]
    fn visit<V>(&mut self, visitor: V) -> Result<V::Value>
        where V: de::Visitor,
    {
        self.parse_value(visitor)
    }
}

struct SeqVisitor<'a, Iter: 'a + Iterator<Item=io::Result<u8>>> {
    de: &'a mut Deserializer<Iter>,
}

impl<'a, Iter: Iterator<Item=io::Result<u8>>> SeqVisitor<'a, Iter> {
    fn new(de: &'a mut Deserializer<Iter>) -> Self {
        SeqVisitor { de: de }
    }
}

impl<'a, Iter> de::SeqVisitor for SeqVisitor<'a, Iter>
    where Iter: Iterator<Item=io::Result<u8>>,
{
    type Error = Error;

    fn visit<T>(&mut self) -> Result<Option<T>>
        where T: de::Deserialize,
    {
        match try!(self.de.peek()) {
            Some(b'e') => return Ok(None),
            Some(_) => (),
            None => {
                return Err(self.de.error(ErrorCode::EOFWhileParsingList));
            }
        }

        let value = try!(de::Deserialize::deserialize(self.de));
        Ok(Some(value))
    }

    fn end(&mut self) -> Result<()> {
        match try!(self.de.next_char()) {
            Some(b'e') => Ok(()),
            Some(_) => Err(self.de.error(ErrorCode::TrailingCharacters)),
            None => Err(self.de.error(ErrorCode::EOFWhileParsingList))
        }
    }
}

struct MapVisitor<'a, Iter: 'a + Iterator<Item=io::Result<u8>>> {
    de: &'a mut Deserializer<Iter>,
}

impl<'a, Iter: Iterator<Item=io::Result<u8>>> MapVisitor<'a, Iter> {
    fn new(de: &'a mut Deserializer<Iter>) -> Self {
        MapVisitor { de: de }
    }
}

impl<'a, Iter> de::MapVisitor for MapVisitor<'a, Iter>
    where Iter: Iterator<Item=io::Result<u8>>
{
    type Error = Error;

    fn visit_key<K>(&mut self) -> Result<Option<K>>
        where K: de::Deserialize,
    {
        match try!(self.de.peek()) {
            Some(b'e') => return Ok(None),
            Some(_) => (),
            None => {
                return Err(self.de.error(ErrorCode::EOFWhileParsingDict));
            }
        }

        match try!(self.de.peek()) {
            Some(byte) if is_digit(byte) => {
                Ok(Some(try!(de::Deserialize::deserialize(self.de))))
            }
            Some(_) => {
                Err(self.de.error(ErrorCode::KeyMustBeAString))
            }
            None => {
                Err(self.de.error(ErrorCode::EOFWhileParsingValue))
            }
        }
    }

    fn visit_value<V>(&mut self) -> Result<V>
        where V: de::Deserialize,
    {
        Ok(try!(de::Deserialize::deserialize(self.de)))
    }

    fn end(&mut self) -> Result<()> {
        match try!(self.de.next_char()) {
            Some(b'e') => { Ok(()) }
            Some(_) => {
                Err(self.de.error(ErrorCode::TrailingCharacters))
            }
            None => {
                Err(self.de.error(ErrorCode::EOFWhileParsingDict))
            }
        }
    }

    fn missing_field<V>(&mut self, _field: &'static str) -> Result<V>
        where V: de::Deserialize,
    {
        let mut de = de::value::ValueDeserializer::into_deserializer(());
        Ok(try!(de::Deserialize::deserialize(&mut de)))
    }
}


pub fn from_iter<I, T>(iter: I) -> Result<T>
    where I: Iterator<Item=io::Result<u8>>,
          T: de::Deserialize,
{
    let mut de = Deserializer::new(iter);
    let value = try!(de::Deserialize::deserialize(&mut de));

    // Make sure the whole stream has been consumed.
    try!(de.end());
    Ok(value)
}

/// Decodes a bencode value from a `std::io::Read`.
pub fn from_reader<R, T>(rdr: R) -> Result<T>
    where R: io::Read,
          T: de::Deserialize,
{
    from_iter(rdr.bytes())
}

/// Decodes a bencode value from a `&str`.
pub fn from_slice<T>(v: &[u8]) -> Result<T>
    where T: de::Deserialize
{
    from_iter(v.iter().map(|byte| Ok(*byte)))
}

#[cfg(test)]
mod tests {
    use serde::bytes::ByteBuf;
    use std::collections::HashMap;
    use super::from_slice;

    #[derive(Deserialize, Debug)]
    struct Document {
        a: ByteBuf,
        b: i64,
        c: Vec<i64>,
        d: Vec<ByteBuf>,
        e: HashMap<ByteBuf, ByteBuf>,
    }

    #[test]
    fn simple_test() {
        // let document = b"d1:a3:eh?1:bl3:beee1:dd1:alldedeeee";
        let document = b"d1:a3:4441:bi444e1:cli123ee1:dl3:foo3:bare1:ed3:foo3:baree";
        let bencode: Document = from_slice(document).expect("error deserializing");
        assert_eq!(&*bencode.a, b"444");
        assert_eq!(bencode.b, 444);
        assert_eq!(bencode.c[0], 123);
        assert_eq!(&*bencode.d[0], b"foo");
        assert_eq!(&*bencode.d[1], b"bar");
        //assert_eq!(bencode.e[b"foo"], b"bar");
    }
}

#[cfg(feature="afl")]
pub mod afl {
    use std::io::{self, Read};
    use super::{from_slice, Value};

    pub fn bdecode() {
        let mut buf = Vec::new();
        io::stdin().take(1 << 20).read_to_end(&mut buf).unwrap();
        match from_slice::<Value>(&*buf) {
            Ok(bencode) => println!("{:#?}", bencode),
            Err(err) => println!("erorr: {:?}", err),
        }
    }
}
