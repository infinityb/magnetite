use std::io;

use serde::ser;
use serde::bytes::ByteBuf;

use super::error::{Error, ErrorCode, Result};


/// A structure for serializing Rust values into Bencode.
pub struct Serializer<W> { writer: W }

impl<W> Serializer<W> where W: io::Write
{
    /// Creates a new Bencode serializer.
    #[inline]
    pub fn new(writer: W) -> Self {
        Serializer { writer: writer }
    }

    /// Unwrap the `Writer` from the `Serializer`.
    #[inline]
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W> ser::Serializer for Serializer<W> 
    where W: io::Write
{
    type Error = Error;

    fn visit_bool(&mut self, value: bool) -> Result<()> {
        if value {
            self.writer.write_all(b"i1e").map_err(From::from)
        } else {
            self.writer.write_all(b"i0e").map_err(From::from)
        }
    }

    #[inline]
    fn visit_isize(&mut self, value: isize) -> Result<()> {
        write!(&mut self.writer, "i{}e", value).map_err(From::from)
    }

    #[inline]
    fn visit_i8(&mut self, value: i8) -> Result<()> {
        write!(&mut self.writer, "i{}e", value).map_err(From::from)
    }

    #[inline]
    fn visit_i16(&mut self, value: i16) -> Result<()> {
        write!(&mut self.writer, "i{}e", value).map_err(From::from)
    }

    #[inline]
    fn visit_i32(&mut self, value: i32) -> Result<()> {
        write!(&mut self.writer, "i{}e", value).map_err(From::from)
    }

    #[inline]
    fn visit_i64(&mut self, value: i64) -> Result<()> {
        write!(&mut self.writer, "i{}e", value).map_err(From::from)
    }

    #[inline]
    fn visit_usize(&mut self, value: usize) -> Result<()> {
        write!(&mut self.writer, "i{}e", value).map_err(From::from)
    }

    #[inline]
    fn visit_u8(&mut self, value: u8) -> Result<()> {
        write!(&mut self.writer, "i{}e", value).map_err(From::from)
    }

    #[inline]
    fn visit_u16(&mut self, value: u16) -> Result<()> {
        write!(&mut self.writer, "i{}e", value).map_err(From::from)
    }

    #[inline]
    fn visit_u32(&mut self, value: u32) -> Result<()> {
        write!(&mut self.writer, "i{}e", value).map_err(From::from)
    }

    #[inline]
    fn visit_u64(&mut self, value: u64) -> Result<()> {
        write!(&mut self.writer, "i{}e", value).map_err(From::from)
    }

    #[inline]
    fn visit_f32(&mut self, _value: f32) -> Result<()> {
        Err(Error::SyntaxError(ErrorCode::UnsupportedType, 0, 0))
    }

    #[inline]
    fn visit_f64(&mut self, _value: f64) -> Result<()> {
        Err(Error::SyntaxError(ErrorCode::UnsupportedType, 0, 0))
    }

    #[inline]
    fn visit_char(&mut self, _value: char) -> Result<()> {
        Err(Error::SyntaxError(ErrorCode::UnsupportedType, 0, 0))
    }

    #[inline]
    fn visit_bytes(&mut self, value: &[u8]) -> Result<()> {
        try!(write!(&mut self.writer, "{}:", value.len()));
        self.writer.write_all(value).map_err(From::from)
    }

    #[inline]
    fn visit_str(&mut self, value: &str) -> Result<()> {
        self.visit_bytes(value.as_bytes())
    }

    #[inline]
    fn visit_none(&mut self) -> Result<()> {
        Err(Error::SyntaxError(ErrorCode::UnsupportedType, 0, 0))
    }

    #[inline]
    fn visit_some<V>(&mut self, value: V) -> Result<()>
        where V: ser::Serialize
    {
        value.serialize(self)
    }

    #[inline]
    fn visit_unit(&mut self) -> Result<()> {
        Err(Error::SyntaxError(ErrorCode::UnsupportedType, 0, 0))
    }

    #[inline]
    fn visit_seq<V>(&mut self, mut visitor: V) -> Result<()>
        where V: ser::SeqVisitor,
    {
        try!(self.writer.write_all(b"l"));
        loop {
            match try!(visitor.visit(self)) {
                Some(()) => (),
                None => break,
            }
        }
        try!(self.writer.write_all(b"e"));
        Ok(())
    }

    #[inline]
    fn visit_seq_elt<T>(&mut self, value: T) -> Result<()>
        where T: ser::Serialize,
    {
        value.serialize(self)
    }

    #[inline]
    fn visit_map<V>(&mut self, mut visitor: V) -> Result<()>
        where V: ser::MapVisitor,
    {
        try!(self.writer.write_all(b"d"));
        loop {
            match try!(visitor.visit(self)) {
                Some(()) => (),
                None => break,
            }
        }
        try!(self.writer.write_all(b"e"));
        Ok(())
    }

    #[inline]
    fn visit_map_elt<K, V>(&mut self, key: K, value: V) -> Result<()>
        where K: ser::Serialize,
              V: ser::Serialize,
    {
        try!(key.serialize(&mut MapKeySerializer { ser: self }));
        try!(value.serialize(self));
        Ok(())
    }

    #[inline]
    fn format() -> &'static str {
        "bencode"
    }
}


struct MapKeySerializer<'a, W: 'a> {
    ser: &'a mut Serializer<W>,
}

impl<'a, W> ser::Serializer for MapKeySerializer<'a, W>
    where W: io::Write
{
    type Error = Error;

    #[inline]
    fn visit_bytes(&mut self, value: &[u8]) -> Result<()> {
        self.ser.visit_bytes(value)
    }

    #[inline]
    fn visit_str(&mut self, value: &str) -> Result<()> {
        self.ser.visit_bytes(value.as_bytes())
    }

    fn visit_bool(&mut self, _value: bool) -> Result<()> {
        Err(Error::SyntaxError(ErrorCode::KeyMustBeABytes, 0, 0))
    }

    fn visit_i64(&mut self, _value: i64) -> Result<()> {
        Err(Error::SyntaxError(ErrorCode::KeyMustBeABytes, 0, 0))
    }

    fn visit_u64(&mut self, _value: u64) -> Result<()> {
        Err(Error::SyntaxError(ErrorCode::KeyMustBeABytes, 0, 0))
    }

    fn visit_f64(&mut self, _value: f64) -> Result<()> {
        Err(Error::SyntaxError(ErrorCode::KeyMustBeABytes, 0, 0))
    }

    fn visit_unit(&mut self) -> Result<()> {
        Err(Error::SyntaxError(ErrorCode::KeyMustBeABytes, 0, 0))
    }

    fn visit_none(&mut self) -> Result<()> {
        Err(Error::SyntaxError(ErrorCode::KeyMustBeABytes, 0, 0))
    }

    fn visit_some<V>(&mut self, _value: V) -> Result<()>
        where V: ser::Serialize
    {
        Err(Error::SyntaxError(ErrorCode::KeyMustBeABytes, 0, 0))
    }

    fn visit_seq<V>(&mut self, mut visitor: V) -> Result<()>
        where V: ser::SeqVisitor,
    {
        Err(Error::SyntaxError(ErrorCode::KeyMustBeABytes, 0, 0))
    }

    fn visit_seq_elt<T>(&mut self, _value: T) -> Result<()>
        where T: ser::Serialize,
    {
        Err(Error::SyntaxError(ErrorCode::KeyMustBeABytes, 0, 0))
    }

    fn visit_map<V>(&mut self, _visitor: V) -> Result<()>
        where V: ser::MapVisitor,
    {
        Err(Error::SyntaxError(ErrorCode::KeyMustBeABytes, 0, 0))
    }

    fn visit_map_elt<K, V>(&mut self, _key: K, _value: V) -> Result<()>
        where K: ser::Serialize,
              V: ser::Serialize,
    {
        Err(Error::SyntaxError(ErrorCode::KeyMustBeABytes, 0, 0))
    }
}