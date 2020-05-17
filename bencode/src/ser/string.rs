use serde::ser::{self, Serialize};

use crate::{Error, ErrorData, Result};

pub struct StringSerializer {
    key: Option<String>,
}

fn unexpected_type(type_name: &'static str) -> Error {
    Error {
        offset: u64::max_value(),
        data: ErrorData::ExpectedString {
            got: type_name.to_string(),
        },
    }
}

impl StringSerializer {
    pub fn new() -> StringSerializer {
        StringSerializer { key: None }
    }

    pub fn into_inner(mut self) -> String {
        self.key.take().unwrap()
    }
}

impl<'a> ser::Serializer for &'a mut StringSerializer {
    type Ok = ();

    type Error = Error;

    type SerializeSeq = serde::ser::Impossible<(), Error>;
    type SerializeTuple = serde::ser::Impossible<(), Error>;
    type SerializeTupleStruct = serde::ser::Impossible<(), Error>;
    type SerializeTupleVariant = serde::ser::Impossible<(), Error>;
    type SerializeMap = serde::ser::Impossible<(), Error>;
    type SerializeStruct = serde::ser::Impossible<(), Error>;
    type SerializeStructVariant = serde::ser::Impossible<(), Error>;

    fn serialize_str(self, v: &str) -> Result<()> {
        self.key = Some(v.to_string());
        Ok(())
    }

    fn is_human_readable(&self) -> bool {
        false
    }

    fn serialize_bool(self, _v: bool) -> Result<()> {
        Err(unexpected_type("bool"))
    }

    fn serialize_i8(self, _v: i8) -> Result<()> {
        Err(unexpected_type("i8"))
    }

    fn serialize_i16(self, _v: i16) -> Result<()> {
        Err(unexpected_type("i16"))
    }

    fn serialize_i32(self, _v: i32) -> Result<()> {
        Err(unexpected_type("i32"))
    }

    fn serialize_i64(self, _v: i64) -> Result<()> {
        Err(unexpected_type("i64"))
    }

    fn serialize_u8(self, _v: u8) -> Result<()> {
        Err(unexpected_type("i32"))
    }

    fn serialize_u16(self, _v: u16) -> Result<()> {
        Err(unexpected_type("u16"))
    }

    fn serialize_u32(self, _v: u32) -> Result<()> {
        Err(unexpected_type("u32"))
    }

    fn serialize_u64(self, _v: u64) -> Result<()> {
        Err(unexpected_type("u64"))
    }

    fn serialize_f32(self, _v: f32) -> Result<()> {
        Err(unexpected_type("f32"))
    }

    fn serialize_f64(self, _v: f64) -> Result<()> {
        Err(unexpected_type("f64"))
    }

    fn serialize_char(self, _v: char) -> Result<()> {
        Err(unexpected_type("char"))
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
        Err(unexpected_type("bytes"))
    }

    fn serialize_none(self) -> Result<()> {
        Err(unexpected_type("option/none"))
    }

    fn serialize_some<T>(self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        Err(unexpected_type("option/some"))
    }

    fn serialize_unit(self) -> Result<()> {
        Err(unexpected_type("unit"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        Err(unexpected_type("unit struct"))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        Err(unexpected_type("unit variant"))
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        Err(unexpected_type("newtype struct"))
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        Err(unexpected_type("newtype variant"))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Err(unexpected_type("seq"))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Err(unexpected_type("tuple"))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Err(unexpected_type("tuple struct"))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(unexpected_type("tuple variant"))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Err(unexpected_type("map"))
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Err(unexpected_type("struct"))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(unexpected_type("struct variant"))
    }
}
