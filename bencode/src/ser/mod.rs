use std::collections::BTreeMap;
use std::io::{self, Write};

use serde::ser;
use serde::Serialize;

mod string;

use self::string::StringSerializer;
use crate::{Error, Result};

// By convention, the public API of a Serde serializer is one or more `to_abc`
// functions such as `to_string`, `to_bytes`, or `to_writer` depending on what
// Rust types the serializer is able to produce as output.
//
// This basic serializer supports only `to_string`.
pub fn to_bytes<T>(value: &T) -> Result<Vec<u8>>
where
    T: Serialize,
{
    let mut cur = io::Cursor::new(Vec::new());
    let mut serializer = Serializer { writer: &mut cur };
    value.serialize(&mut serializer)?;
    drop(serializer);
    Ok(cur.into_inner())
}

/// A structure for serializing Rust values into Bencode.
pub struct Serializer<W> {
    writer: W,
}

impl<W> Serializer<W>
where
    W: io::Write,
{
    /// Creates a new Bencode serializer.
    #[inline]
    pub fn new(writer: W) -> Self {
        Serializer { writer }
    }

    /// Unwrap the `Writer` from the `Serializer`.
    #[inline]
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<'a, 'wri, W> ser::Serializer for &'a mut Serializer<W>
where
    W: Write + 'wri,
    'wri: 'a,
{
    type Ok = ();

    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = SerializeMap<'a, W>;
    type SerializeStruct = SerializeMap<'a, W>;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<()> {
        self.serialize_i8(if v { 1 } else { 0 })
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        write!(&mut self.writer, "i{}e", v)?;
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        write!(&mut self.writer, "i{}e", v)?;
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        self.serialize_f64(f64::from(v))
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        let fs = format!("{}", v);
        self.serialize_str(&fs)
    }

    fn serialize_char(self, v: char) -> Result<()> {
        self.serialize_str(&v.to_string())
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        self.serialize_bytes(v.as_bytes())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        write!(&mut self.writer, "{}:", v.len())?;
        self.writer.write_all(v)?;
        Ok(())
    }

    fn serialize_none(self) -> Result<()> {
        self.serialize_unit()
    }

    fn serialize_some<T>(self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<()> {
        write!(&mut self.writer, "de")?;
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        write!(&mut self.writer, "d")?;
        variant.serialize(&mut *self)?;
        value.serialize(&mut *self)?;
        write!(&mut self.writer, "e")?;
        Ok(())
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        write!(&mut self.writer, "l")?;
        Ok(self)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        write!(&mut self.writer, "d")?;
        variant.serialize(&mut *self)?;
        write!(&mut self.writer, "l")?;
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        write!(&mut self.writer, "d")?;
        Ok(SerializeMap::new(self))
    }

    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        write!(&mut self.writer, "d")?;
        variant.serialize(&mut *self)?;
        write!(&mut self.writer, "d")?;
        Ok(self)
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

impl<'a, 'wri, W> ser::SerializeSeq for &'a mut Serializer<W>
where
    W: Write + 'wri,
    'wri: 'a,
{
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        write!(&mut self.writer, "e")?;
        Ok(())
    }
}

// Same thing but for tuples.
impl<'a, 'wri, W> ser::SerializeTuple for &'a mut Serializer<W>
where
    W: Write + 'wri,
    'wri: 'a,
{
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        write!(&mut self.writer, "e")?;
        Ok(())
    }
}

// Same thing but for tuple structs.
impl<'a, 'wri, W> ser::SerializeTupleStruct for &'a mut Serializer<W>
where
    W: Write + 'wri,
    'wri: 'a,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        write!(&mut self.writer, "e")?;
        Ok(())
    }
}

impl<'a, 'wri, W> ser::SerializeTupleVariant for &'a mut Serializer<W>
where
    W: Write + 'wri,
    'wri: 'a,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        write!(&mut self.writer, "ee")?;
        Ok(())
    }
}

// --

pub struct SerializeMap<'a, W> {
    upstream: &'a mut Serializer<W>,
    keyval: BTreeMap<String, Vec<u8>>,
    last_key: Option<String>,
}

impl<'a, W> SerializeMap<'a, W>
where
    W: Write,
{
    fn new(upstream: &'a mut Serializer<W>) -> SerializeMap<'a, W> {
        SerializeMap {
            upstream: upstream,
            keyval: BTreeMap::new(),
            last_key: None,
        }
    }
}

impl<'a, W> ser::SerializeMap for SerializeMap<'a, W>
where
    W: Write,
{
    type Ok = ();
    type Error = Error;

    // sell FIXME: we need to validate the keys are strings
    //
    fn serialize_key<T>(&mut self, key: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        let mut s = StringSerializer::new();
        key.serialize(&mut s)?;
        self.last_key = Some(s.into_inner());
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        let key_string = self
            .last_key
            .take()
            .expect("serialize_value without serialize_key");
        let mut s = Serializer::new(io::Cursor::new(Vec::new()));
        value.serialize(&mut s)?;
        self.keyval.insert(key_string, s.into_inner().into_inner());
        Ok(())
    }

    fn end(self) -> Result<()> {
        for (k, v) in self.keyval {
            write!(&mut self.upstream.writer, "{}:{}", k.len(), k)?;
            self.upstream.writer.write_all(&v[..]).unwrap();
        }
        write!(&mut self.upstream.writer, "e")?;
        Ok(())
    }
}

// Structs are like maps in which the keys are constrained to be compile-time
// constant strings.
impl<'a, W> ser::SerializeStruct for SerializeMap<'a, W>
where
    W: Write,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        let mut s = StringSerializer::new();
        key.serialize(&mut s)?;
        let key_string = s.into_inner();

        let mut s = Serializer::new(io::Cursor::new(Vec::new()));
        value.serialize(&mut s)?;
        self.keyval.insert(key_string, s.into_inner().into_inner());

        Ok(())
    }

    fn end(self) -> Result<()> {
        for (k, v) in self.keyval {
            write!(&mut self.upstream.writer, "{}:{}", k.len(), k)?;
            self.upstream.writer.write_all(&v[..]).unwrap();
        }
        write!(&mut self.upstream.writer, "e")?;
        Ok(())
    }
}

// Similar to `SerializeTupleVariant`, here the `end` method is responsible for
// closing both of the curly braces opened by `serialize_struct_variant`.
impl<'a, 'wri, W> ser::SerializeStructVariant for &'a mut Serializer<W>
where
    W: Write + 'wri,
    'wri: 'a,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        key.serialize(&mut **self)?;
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        write!(&mut self.writer, "ee")?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_struct() {
    #[derive(Serialize)]
    struct Test {
        int: u32,
        seq: Vec<&'static str>,
    }

    let test = Test {
        int: 1,
        seq: vec!["a", "b"],
    };

    let expected = b"d3:inti1e3:seql1:a1:bee";
    assert_eq!(to_bytes(&test).unwrap(), expected);
}

#[test]
fn test_enum() {
    #[derive(Serialize)]
    enum E {
        Unit,
        Newtype(u32),
        Tuple(u32, u32),
        Struct { a: u32 },
    }

    let u = E::Unit;
    let expected = b"4:Unit";
    assert_eq!(to_bytes(&u).unwrap(), expected);

    let n = E::Newtype(1);
    let expected = b"d7:Newtypei1ee";
    assert_eq!(to_bytes(&n).unwrap(), expected);

    let t = E::Tuple(1, 2);
    let expected = b"d5:Tupleli1ei2eee";
    assert_eq!(to_bytes(&t).unwrap(), expected);

    let s = E::Struct { a: 1 };
    let expected = b"d6:Structd1:ai1eee";
    assert_eq!(to_bytes(&s).unwrap(), expected);
}
