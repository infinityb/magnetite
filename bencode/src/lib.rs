use std::borrow::Cow;
use std::error::Error as StdError;
use std::fmt::{self, Display, Write};
use std::io;
use std::num::ParseIntError;
use std::str::{self, FromStr, Utf8Error};

use serde::de::{self, IntoDeserializer, Unexpected, Visitor};

use iresult::IResult;
mod ser;
mod value;

pub use self::ser::to_bytes;
pub use self::value::Value;

#[inline]
fn is_digit(val: u8) -> bool {
    b'0' <= val && val <= b'9'
}

fn check_is_digits(digits: &[u8]) -> Option<&str> {
    for d in digits {
        if !is_digit(*d) {
            return None;
        }
    }
    Some(str::from_utf8(digits).unwrap())
}

pub fn from_bytes<'a, T>(s: &'a [u8]) -> Result<T>
where
    T: de::Deserialize<'a>,
{
    let mut deserializer = Deserializer::from_bytes(s);
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.tokenizer.scratch.is_empty() && deserializer.tokenizer.data.is_empty() {
        Ok(t)
    } else {
        Err(Error::trailing_characters(&deserializer.tokenizer))
    }
}

trait CopyReadChunk {
    // because we're guaranteed to copy because of the lifetime requirements for reading
    // from an I/O resource, we can just copy the data into the tokenizer's scratch
    fn read_chunk(&mut self, scratch: &mut Vec<u8>) -> io::Result<usize>;
}

struct CopyReadChunkReader<R>
where
    R: io::Read,
{
    io: R,
}

impl<R> CopyReadChunk for CopyReadChunkReader<R>
where
    R: io::Read,
{
    fn read_chunk(&mut self, scratch: &mut Vec<u8>) -> io::Result<usize> {
        let mut tmp_buf = [0; 4096];
        let read_length = self.io.read(&mut tmp_buf[..])?;
        scratch.extend(&tmp_buf[..read_length]);
        Ok(read_length)
    }
}

#[derive(PartialEq, Eq)]
pub enum Node<'a> {
    DictionaryStart,
    ArrayStart,
    ContainerEnd,
    Integer(Cow<'a, str>),
    Bytes(Cow<'a, [u8]>),
}

impl<'a> fmt::Debug for Node<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Node::DictionaryStart => {
                write!(f, "DictionaryStart")?;
                Ok(())
            }
            Node::ArrayStart => {
                write!(f, "ArrayStart")?;
                Ok(())
            }
            Node::ContainerEnd => {
                write!(f, "ContainerEnd")?;
                Ok(())
            }
            Node::Integer(ref v) => {
                write!(f, "Integer({:?})", v)?;
                Ok(())
            }
            Node::Bytes(ref by) => {
                write!(f, "Bytes({:?})", BinStr(&by))?;
                Ok(())
            }
        }
    }
}

impl<'a> Node<'a> {
    fn into_owned(self) -> Node<'static> {
        match self {
            Node::DictionaryStart => Node::DictionaryStart,
            Node::ArrayStart => Node::ArrayStart,
            Node::ContainerEnd => Node::ContainerEnd,
            Node::Integer(c) => Node::Integer(Cow::Owned(c.into_owned())),
            Node::Bytes(c) => Node::Bytes(Cow::Owned(c.into_owned())),
        }
    }
}

#[derive(Debug)]
pub struct Tokenizer<'de> {
    total_offset: u64,
    scratch_offset: usize,
    scratch: Vec<u8>,
    data: &'de [u8],
}

impl<'de> Tokenizer<'de> {
    pub fn new(data: &'de [u8]) -> Tokenizer<'de> {
        Tokenizer {
            total_offset: 0,
            scratch_offset: 0,
            scratch: Vec::new(),
            data,
        }
    }

    pub fn add_data_copying(&mut self, data: &[u8]) {
        // flush remainder of data into scratch buffer.
        self.scratch.extend(self.data);
        self.scratch.extend(data);
        self.data = &[];
    }

    pub fn add_data(&mut self, data: &'de [u8]) {
        // flush remainder of data into scratch buffer.
        self.scratch.extend(self.data);
        self.data = data;
    }

    pub fn next(&mut self, is_peek: bool) -> IResult<Node<'de>, Box<dyn StdError>> {
        fn fixup_scratch<T>(data: &mut Vec<T>, split_point: usize)
        where
            T: Copy,
        {
            let data_valid_length = data.len() - split_point;
            let (into, from) = data.split_at_mut(split_point);
            // FIXME: readability - use copy_from_slice if possible.
            for (o, i) in into.iter_mut().zip(from) {
                *o = *i;
            }
            data.truncate(data_valid_length);
        }

        loop {
            let buf: &[u8] = &self.data;

            if self.scratch.capacity() / 2 < self.scratch_offset {
                fixup_scratch(&mut self.scratch, self.scratch_offset);
                self.scratch_offset = 0;
            }

            if self.scratch_offset < self.scratch.len() {
                assert!(self.scratch[self.scratch_offset..].len() > 0);
                return match parse_next_item(&self.scratch[self.scratch_offset..]) {
                    IResult::Done((length, value)) => {
                        if !is_peek {
                            self.total_offset += length as u64;
                            self.scratch_offset += length;
                        }
                        // since we're in our scratch space, we need to allocate an owned copy.
                        IResult::Done(value.into_owned())
                    }
                    IResult::ReadMore(length) => {
                        // take the desired amount into scratch and retry.
                        if length <= self.data.len() {
                            self.scratch.extend(&self.data[..length]);
                            self.data = &self.data[length..];
                            continue;
                        } else {
                            IResult::ReadMore(length)
                        }
                    }
                    IResult::Err(err) => IResult::Err(err),
                };
            }
            if !buf.is_empty() {
                return match parse_next_item(buf) {
                    IResult::Done((length, v)) => {
                        if !is_peek {
                            self.total_offset += length as u64;
                            self.data = &self.data[length..];
                        }
                        IResult::Done(v)
                    }
                    IResult::ReadMore(length) => {
                        self.scratch.extend(self.data);
                        self.data = &[];
                        IResult::ReadMore(length)
                    }
                    IResult::Err(err) => IResult::Err(err),
                };
            } else {
                return IResult::ReadMore(1);
            }
        }
    }
}

fn parse_next_item<'a>(data: &'a [u8]) -> IResult<(usize, Node<'a>), Box<dyn StdError>> {
    let mut rem_iter = data.iter();
    let mut rem_iter_copy = rem_iter.clone();
    match rem_iter.next().unwrap() {
        b'd' => IResult::Done((1, Node::DictionaryStart)),
        b'l' => IResult::Done((1, Node::ArrayStart)),
        b'e' => IResult::Done((1, Node::ContainerEnd)),
        b'i' => match rem_iter_copy.position(|x| *x == b'e') {
            Some(pos) => {
                let rem_slice = rem_iter.as_slice();
                let value = check_is_digits(&rem_slice[..pos - 1]).expect("xTODO: error handling");
                IResult::Done((1 + pos, Node::Integer(Cow::Borrowed(value))))
            }
            None => IResult::ReadMore(1),
        },
        b'0'..=b'9' => match rem_iter.position(|x| *x == b':') {
            Some(pos) => {
                let pos = pos + 1; // first byte consumed by match.
                let (length_prefix, rest) = rem_iter_copy.as_slice().split_at(pos);
                let rest = &rest[1..]; // skip the b':'

                let str_len = check_is_digits(&length_prefix).expect("yTODO: error handling");
                let length: usize = str_len.parse().expect("aTODO: error handling");
                if rest.len() < length {
                    return IResult::ReadMore(length - rest.len());
                }
                IResult::Done((
                    pos + length + 1,
                    Node::Bytes(Cow::Borrowed(&rest[..length])),
                ))
            }
            None => IResult::ReadMore(1),
        },
        v => panic!("zTODO: error handling - bad byte {}", *v as char),
    }
}

#[derive(Debug)]
pub struct Error {
    offset: u64,
    data: ErrorData,
}

#[derive(Debug)]
pub enum ErrorData {
    Custom(String),
    Truncated,
    AntiTruncated,
    ExpectedString { got: String },
}

impl serde::ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Error {
            offset: u64::max_value(),
            data: ErrorData::Custom(format!("{}", msg)),
        }
    }
}

impl Error {
    fn trailing_characters(tz: &Tokenizer) -> Error {
        Error {
            offset: tz.total_offset,
            data: ErrorData::AntiTruncated,
        }
    }

    fn token_expected(expected: Node, got: Node) -> Error {
        panic!(
            "expected: {:?}, got: {:?}, {}:{}",
            expected,
            got,
            file!(),
            line!()
        );
    }

    fn expected_string(tz: &Tokenizer, node: &Node) -> Error {
        Error {
            offset: tz.total_offset,
            data: ErrorData::ExpectedString {
                got: format!("{:?}", node),
            },
        }
    }

    fn expected_bytes() -> Error {
        panic!("{}:{}", file!(), line!());
    }

    fn truncated(tz: &Tokenizer) -> Error {
        Error {
            offset: tz.total_offset,
            data: ErrorData::Truncated,
        }
    }

    fn unexpected_container_end() -> Error {
        panic!("{}:{}", file!(), line!());
    }
}

fn expect_assert(expected: Node, got: Node) -> Result<()> {
    if expected != got {
        Err(Error::token_expected(expected, got))
    } else {
        Ok(())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error {
            offset: u64::max_value(),
            data: ErrorData::Custom(format!("{}", e)),
        }
    }
}

impl From<Box<dyn StdError>> for Error {
    fn from(e: Box<dyn StdError>) -> Error {
        Error {
            offset: u64::max_value(),
            data: ErrorData::Custom(format!("{}", e)),
        }
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        "error"
    }

    fn cause(&self) -> Option<&dyn StdError> {
        None
    }
}

impl de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        unimplemented!("adapting error: {}", msg);
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
struct Deserializer<'de> {
    tokenizer: Tokenizer<'de>,
}

impl<'de> Deserializer<'de> {
    fn from_bytes(b: &'de [u8]) -> Deserializer<'de> {
        Deserializer {
            tokenizer: Tokenizer::new(b),
        }
    }
}

impl From<ParseIntError> for Error {
    fn from(_e: ParseIntError) -> Error {
        unimplemented!();
    }
}

impl From<Utf8Error> for Error {
    fn from(_e: Utf8Error) -> Error {
        unimplemented!();
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(_e: std::string::FromUtf8Error) -> Error {
        unimplemented!();
    }
}

pub fn into_result<T, E1>(res: IResult<T, E1>, tz: &Tokenizer) -> std::result::Result<T, Error>
where
    Error: From<E1>,
{
    match res {
        IResult::Done(v) => Ok(v),
        IResult::ReadMore(..) => Err(Error::truncated(tz)),
        IResult::Err(e) => Err(e.into()),
    }
}

impl<'de> Deserializer<'de> {
    fn parse_integer<T>(&mut self) -> Result<T>
    where
        T: FromStr<Err = ParseIntError>,
    {
        let res = self.tokenizer.next(false);
        match into_result(res, &self.tokenizer)? {
            Node::Integer(v) => {
                let vv: T = v.parse()?;
                Ok(vv)
            }
            v => {
                unimplemented!("unhandled value {:?}", v);
            }
        }
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let res = self.tokenizer.next(true);
        match into_result(res, &self.tokenizer)? {
            Node::DictionaryStart => self.deserialize_map(visitor),
            Node::ArrayStart => self.deserialize_seq(visitor),
            Node::ContainerEnd => {
                unimplemented!(
                    "error out here - we hit a container-end while not in a collection?"
                );
            }
            Node::Integer(ii) => {
                if ii.starts_with("-") {
                    return self.deserialize_i64(visitor);
                } else {
                    return self.deserialize_u64(visitor);
                }
            }
            Node::Bytes(Cow::Borrowed(bb)) => {
                let _ = self.tokenizer.next(false);
                match str::from_utf8(bb) {
                    Ok(ss) => visitor.visit_borrowed_str(ss),
                    Err(..) => visitor.visit_borrowed_bytes(bb),
                }
            }
            Node::Bytes(Cow::Owned(bb)) => {
                let _ = self.tokenizer.next(false);
                match String::from_utf8(bb) {
                    Ok(ss) => visitor.visit_string(ss),
                    Err(err) => visitor.visit_byte_buf(err.into_bytes()),
                }
            }
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let res = self.tokenizer.next(false);
        match into_result(res, &self.tokenizer)? {
            Node::Integer(v) => {
                if v == "0" {
                    return visitor.visit_bool(false);
                }
                if v == "1" {
                    return visitor.visit_bool(true);
                }
                unimplemented!("unhandled value {:?}", v);
            }
            v => {
                unimplemented!("unhandled value {:?}", v);
            }
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i16(self.parse_integer()?)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i16(self.parse_integer()?)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i32(self.parse_integer()?)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i64(self.parse_integer()?)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u8(self.parse_integer()?)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u16(self.parse_integer()?)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u32(self.parse_integer()?)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u64(self.parse_integer()?)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let res = self.tokenizer.next(false);
        match into_result(res, &self.tokenizer)? {
            Node::Bytes(Cow::Borrowed(data)) => {
                let str_data = str::from_utf8(data)?;
                return visitor.visit_borrowed_str(str_data);
            }
            Node::Bytes(Cow::Owned(data)) => {
                str::from_utf8(&data)?;
                let str_data = String::from_utf8(data).unwrap();
                return visitor.visit_string(str_data);
            }
            v => {
                expect_assert(Node::Bytes(Cow::Borrowed(&[])), v)?;
                unreachable!();
            }
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    // The `Serializer` implementation on the previous page serialized byte
    // arrays as JSON arrays of bytes. Handle that representation here.
    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let res = self.tokenizer.next(false);
        match into_result(res, &self.tokenizer)? {
            Node::Bytes(Cow::Borrowed(data)) => {
                return visitor.visit_borrowed_bytes(data);
            }
            Node::Bytes(Cow::Owned(data)) => {
                return visitor.visit_byte_buf(data);
            }
            v => {
                unimplemented!("unhandled value {:?}", v);
            }
        }
    }

    fn deserialize_newtype_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(mut self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        struct SeqVisitor<'p, 'de: 'p> {
            de: &'p mut Deserializer<'de>,
        }

        impl<'p, 'de: 'p> de::SeqAccess<'de> for SeqVisitor<'p, 'de> {
            type Error = Error;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
            where
                T: de::DeserializeSeed<'de>,
            {
                let res = self.de.tokenizer.next(true);
                match into_result(res, &self.de.tokenizer)? {
                    Node::ContainerEnd => return Ok(None),
                    _ => seed.deserialize(&mut *self.de).map(Some),
                }
            }
        }

        let res = self.tokenizer.next(false);
        let array_start = into_result(res, &self.tokenizer)?;
        expect_assert(array_start, Node::ArrayStart)?;

        let rv = visitor.visit_seq(SeqVisitor { de: &mut self })?;

        let res = self.tokenizer.next(false);
        let array_end = into_result(res, &self.tokenizer)?;
        expect_assert(array_end, Node::ContainerEnd)?;

        Ok(rv)
    }

    // Tuples look just like sequences in JSON. Some formats may be able to
    // represent tuples more efficiently.
    //
    // As indicated by the length parameter, the `Deserialize` implementation
    // for a tuple in the Serde data model is required to know the length of the
    // tuple before even looking at the input data.
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    // Tuple structs look just like sequences in JSON.
    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(mut self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        struct MapVisitor<'p, 'de: 'p> {
            de: &'p mut Deserializer<'de>,
        }

        impl<'p, 'de: 'p> de::MapAccess<'de> for MapVisitor<'p, 'de> {
            type Error = Error;

            fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
            where
                K: de::DeserializeSeed<'de>,
            {
                let res = self.de.tokenizer.next(true);
                match into_result(res, &self.de.tokenizer)? {
                    Node::ContainerEnd => return Ok(None),
                    n @ Node::ArrayStart | n @ Node::DictionaryStart | n @ Node::Integer(..) => {
                        return Err(Error::expected_string(&self.de.tokenizer, &n));
                    }
                    Node::Bytes(..) => seed.deserialize(&mut *self.de).map(Some),
                }
            }
            fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
            where
                V: de::DeserializeSeed<'de>,
            {
                seed.deserialize(&mut *self.de)
            }
        }

        let res = self.tokenizer.next(false);
        let token = into_result(res, &self.tokenizer)?;
        expect_assert(token, Node::DictionaryStart)?;

        let rv = visitor.visit_map(MapVisitor { de: &mut self })?;

        let res = self.tokenizer.next(false);
        let token = into_result(res, &self.tokenizer)?;
        expect_assert(token, Node::ContainerEnd)?;

        Ok(rv)
    }

    // Structs look just like maps in JSON.
    //
    // Notice the `fields` parameter - a "struct" in the Serde data model means
    // that the `Deserialize` implementation is required to know what the fields
    // are before even looking at the input data. Any key-value pairing in which
    // the fields cannot be known ahead of time is probably a map.
    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        mut self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let res = self.tokenizer.next(false);
        let node = into_result(res, &self.tokenizer)?;
        match node {
            Node::DictionaryStart => visitor.visit_enum(EnumExtended { de: &mut self }),
            Node::Bytes(Cow::Borrowed(data)) => {
                let data = str::from_utf8(data)?;
                visitor.visit_enum(IntoDeserializer::into_deserializer(data))
            }
            Node::Bytes(Cow::Owned(data)) => {
                let data = String::from_utf8(data)?;
                visitor.visit_enum(IntoDeserializer::into_deserializer(data))
            }
            n @ Node::ArrayStart | n @ Node::Integer(..) => {
                return Err(Error::expected_string(&self.tokenizer, &n));
            }
            Node::ContainerEnd => Err(Error::unexpected_container_end()),
        }
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let res = self.tokenizer.next(false);
        if let Node::Bytes(ref by) = into_result(res, &self.tokenizer)? {
            visitor.visit_bytes(&by[..])
        } else {
            Err(Error::expected_bytes())
        }
    }

    // Float parsing is stupidly hard.
    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // Float parsing is stupidly hard.
    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // The `Serializer` implementation on the previous page serialized chars as
    // single-character strings so handle that representation here.
    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // Parse a string, check that it is one character, call `visit_char`.
        unimplemented!()
    }

    // An absent optional is represented as the JSON `null` and a present
    // optional is represented as just the contained value.
    //
    // As commented in `Serializer` implementation, this is a lossy
    // representation. For example the values `Some(())` and `None` both
    // serialize as just `null`. Unfortunately this is typically what people
    // expect when working with JSON. Other formats are encouraged to behave
    // more intelligently if possible.
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_some(self)
    }

    // In Serde, unit means an anonymous value containing no data.
    fn deserialize_unit<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(de::Error::custom(
            "deserialize_unit unsupported: bencode has no unit type",
        ))
    }

    // Unit struct means a named value containing no data.
    fn deserialize_unit_struct<V>(self, _name: &'static str, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(de::Error::custom(
            "deserialize_unit_struct unsupported: bencode has no unit type",
        ))
    }
}

struct EnumExtended<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
}

impl<'de, 'a> de::EnumAccess<'de> for EnumExtended<'a, 'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: de::DeserializeSeed<'de>,
    {
        let val = seed.deserialize(&mut *self.de)?;
        Ok((val, self))
    }
}

impl<'de, 'a> de::VariantAccess<'de> for EnumExtended<'a, 'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Err(de::Error::invalid_type(Unexpected::UnitVariant, &"string"))
    }

    // Newtype variants are represented in JSON as `{ NAME: VALUE }` so
    // deserialize the value here.
    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: de::DeserializeSeed<'de>,
    {
        let rv = seed.deserialize(&mut *self.de)?;

        // end the container started by `deserialize_enum`
        let res = self.de.tokenizer.next(false);
        let token = into_result(res, &self.de.tokenizer)?;
        expect_assert(token, Node::ContainerEnd)?;

        Ok(rv)
    }

    // Tuple variants are represented in JSON as `{ NAME: [DATA...] }` so
    // deserialize the sequence of data here.
    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let rv = de::Deserializer::deserialize_seq(&mut *self.de, visitor)?;

        // end the container started by `deserialize_enum`
        let res = self.de.tokenizer.next(false);
        let token = into_result(res, &self.de.tokenizer)?;
        expect_assert(token, Node::ContainerEnd)?;

        Ok(rv)
    }

    // Struct variants are represented in JSON as `{ NAME: { K: V, ... } }` so
    // deserialize the inner map here.
    fn struct_variant<V>(self, _fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let rv = de::Deserializer::deserialize_map(&mut *self.de, visitor)?;

        // end the container started by `deserialize_enum`
        let res = self.de.tokenizer.next(false);
        let token = into_result(res, &self.de.tokenizer)?;
        expect_assert(token, Node::ContainerEnd)?;

        Ok(rv)
    }
}

#[cfg(test)]
mod test {
    use super::{IResult, Node, Tokenizer};
    use serde::{Deserialize, Serialize};
    use std::borrow::Cow;

    #[derive(Serialize, Deserialize, Debug)]
    struct TorrentMeta {
        announce: String,
        #[serde(rename = "announce-list")]
        announce_list: Vec<Vec<String>>,
        comment: String,
        #[serde(rename = "created by")]
        created_by: String,
        #[serde(rename = "creation date")]
        creation_date: u64,
        info: TorrentMetaInfo,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct TorrentMetaInfo {
        files: Vec<TorrentMetaInfoFile>,
        #[serde(rename = "piece length")]
        piece_length: u64,
        #[serde(with = "serde_bytes")]
        pieces: Vec<u8>,
        name: String,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct TorrentMetaInfoFile {
        length: u64,
        path: Vec<String>,
    }

    fn allocation_how(node: &Node) -> &'static str {
        match *node {
            Node::DictionaryStart | Node::ArrayStart | Node::ContainerEnd => "non-allocating value",

            Node::Integer(Cow::Owned(_)) | Node::Bytes(Cow::Owned(_)) => "allocated",

            Node::Integer(Cow::Borrowed(_)) | Node::Bytes(Cow::Borrowed(_)) => {
                "non-allocating borrow: &'de"
            }
        }
    }

    #[test]
    fn torrent_example_broken() {
        let bufs: &[&[u8]] = &[
            b"d",
            b"8:announce43:udp://tracker.coppersurfer.tk:6969/announce",
            b"13:announce-list",
            b"l",
            b"l",
            b"4:aaaa",
            b"e",
            b"e",
            b"7:comment4:aaaa",
            b"10:created by4:aaaa",
            b"13:creation datei4444e",
            b"i33ei33e",
            b"e",
        ];

        let mut buf = Vec::new();
        for piece in bufs {
            buf.extend(*piece);
        }

        let err = crate::from_bytes::<TorrentMeta>(&buf[..]).unwrap_err();
        assert_eq!(err.offset, 139);
    }

    #[test]
    #[rustfmt::skip] // keep tree-like structure
    fn torrent_example() {
        let bufs: &[&[u8]] = &[
            b"d",
                b"8:announce43:udp://tracker.coppersurfer.tk:6969/announce",
                b"13:announce-list",
                    b"l",
                        b"l",
                            b"4:aaaa",
                        b"e",
                    b"e",
                b"7:comment4:aaaa",
                b"10:created by4:aaaa",
                b"13:creation datei4444e",
                b"4:info",
                b"d",
                    b"5:files",
                    b"l",
                        b"d",
                            b"6:length",
                            b"i44e",
                            b"4:path",
                            b"l",
                                b"4:foo1",
                            b"e",
                        b"e",
                        b"d",
                            b"6:length",
                            b"i44e",
                            b"4:path",
                            b"l",
                                b"4:foo2",
                            b"e",
                        b"e",
                    b"e",
                    b"4:name9:winter427",
                    b"12:piece length",
                    b"i262144e",
                    b"6:pieces",
                    b"20:lynnielynnlynnielynn",
                b"e",
            b"e",
        ];

        let mut buf = Vec::new();
        for piece in bufs {
            buf.extend(*piece);
        }

        let tm: TorrentMeta = crate::from_bytes(&buf[..]).unwrap();
        println!("{:#?}", tm);
    }

    #[test]
    fn test_chunks() {
        let bufs: &[&[u8]] = &[
            b"i441",
            b"e",
            b"5:quackd5:q",
            b"uack4:b",
            b"oop10:some",
            b"frogs",
            b"!0:",
            b"e",
        ];

        let mut next_bufs = bufs.iter().cloned();
        let mut tokenizer = Tokenizer::new(b"");
        loop {
            match tokenizer.next(false) {
                IResult::Done(ref v) => {
                    println!("got token {:?}, {}", v, allocation_how(v));
                }
                IResult::ReadMore(_) => {
                    if let Some(next) = next_bufs.next() {
                        tokenizer.add_data(next);
                    } else {
                        break;
                    }
                }
                IResult::Err(err) => panic!("error: {:?}", err),
            }
        }
    }

    #[test]
    fn test_one_buffer() {
        let mut tokenizer = Tokenizer::new(b"i441e5:quackd5:quack4:boop10:somefrogs!0:e");
        loop {
            match tokenizer.next(false) {
                IResult::Done(ref v) => {
                    println!("got token {:?}, {}", v, allocation_how(v));
                }
                IResult::ReadMore(_) => break,
                IResult::Err(err) => panic!("error: {:?}", err),
            }
        }
    }

    #[test]
    fn deserialize_a_string() {
        let var: String = super::from_bytes(b"5:hello").unwrap();
        assert_eq!(var, "hello");
    }

    #[test]
    fn deserialize_an_i32() {
        let var: i32 = super::from_bytes(b"i5e").unwrap();
        assert_eq!(var, 5);
    }

    #[test]
    fn deserialize_a_list() {
        let var: Vec<i32> = super::from_bytes(b"li1ei2ei3ei4ei5ee").unwrap();
        assert_eq!(var.len(), 5);
        assert_eq!(var[0], 1);
        assert_eq!(var[1], 2);
        assert_eq!(var[2], 3);
        assert_eq!(var[3], 4);
        assert_eq!(var[4], 5);
    }

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
    enum FoobarEnum {
        Foo,
        Bar(i32, i32),
        Baz { a: i32, b: i32 },
    }

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
    struct FoobarSubstruct {
        a: i32,
    }

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
    struct Foobar<'a> {
        field_i32: i32,
        field_bool: bool,
        field_string: &'a str,
        field_sub_struct: FoobarSubstruct,
        field_sub_enum: FoobarEnum,
    }

    #[test]
    fn deserialize_a_struct() {
        let data = concat!(
            "d",
            "9:field_i32i1234e10:field_booli1e12:field_string4:yeyy",
            "16:field_sub_structd1:ai4ee",
            "14:field_sub_enum",
            "d3:Bazd1:ai1e1:bi2eee",
            "e",
        );
        let var: Foobar = super::from_bytes(data.as_bytes()).unwrap();
        assert_eq!(
            var,
            Foobar {
                field_i32: 1234,
                field_bool: true,
                field_string: "yeyy",
                field_sub_struct: FoobarSubstruct { a: 4 },
                field_sub_enum: FoobarEnum::Baz { a: 1, b: 2 },
            }
        );
    }

    #[test]
    fn deserialize_fancy_enum_unit_value() {
        let data = "3:Foo";
        assert_eq!(
            super::from_bytes::<FoobarEnum>(data.as_bytes()).unwrap(),
            FoobarEnum::Foo,
        );
    }

    #[test]
    fn deserialize_fancy_enum_struct() {
        let data = "d3:Bazd1:ai1e1:bi2eee";
        assert_eq!(
            super::from_bytes::<FoobarEnum>(data.as_bytes()).unwrap(),
            FoobarEnum::Baz { a: 1, b: 2 }
        );
    }

    #[test]
    fn deserialize_fancy_enum_tuple() {
        let data = "d3:Barli1ei2eee";
        assert_eq!(
            super::from_bytes::<FoobarEnum>(data.as_bytes()).unwrap(),
            FoobarEnum::Bar(1, 2)
        );
    }

    #[test]
    fn deserialize_truncated_should_fail() {
        let data = concat!(
            "d",
            "9:field_i32i1234e10:field_booli1e12:field_string4:yeyy",
            "16:field_sub_structd1:ai4ee",
            "14:field_sub_enum",
            "3:Foo",
        );
        assert!(super::from_bytes::<Foobar>(data.as_bytes()).is_err());
    }
}

pub struct BinStr<'a>(pub &'a [u8]);

impl<'a> fmt::Debug for BinStr<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "b\"")?;
        for &b in self.0 {
            match b {
                b'\0' => write!(f, "\\0")?,
                b'\"' => write!(f, "\\\"")?,
                b'\\' => write!(f, "\\\\")?,
                b'\n' => write!(f, "\\n")?,
                b'\r' => write!(f, "\\r")?,
                b'\t' => write!(f, "\\t")?,
                _ if 0x20 <= b && b < 0x7f => write!(f, "{}", b as char)?,
                _ => write!(f, "\\x{:02x}", b)?,
            }
        }
        write!(f, "\"")?;
        Ok(())
    }
}

pub struct HexStr<'a>(pub &'a [u8]);

impl<'a> fmt::Debug for HexStr<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
        f.write_str("dehex(\"")?;
        for c in self.0.iter() {
            f.write_char(HEX_CHARS[usize::from(c >> 4)] as char)?;
            f.write_char(HEX_CHARS[usize::from(c & 0xF)] as char)?;
        }
        f.write_str("\")")?;
        Ok(())
    }
}
