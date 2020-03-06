use serde::{de, ser};
use serde_bytes::ByteBuf;
use std::collections::BTreeMap;
use std::fmt;

use super::BinStr;

#[derive(PartialEq, Eq, Clone)]
pub enum Value {
    Integer(i64),
    UInteger(u64),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Dict(BTreeMap<Vec<u8>, Value>),
}

struct DictDebugger<'a>(&'a BTreeMap<Vec<u8>, Value>);

impl<'a> fmt::Debug for DictDebugger<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let entries = self.0.iter().map(|(k, v)| (BinStr(k), v));
        f.debug_map().entries(entries).finish()
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Value::Integer(v) => f.debug_tuple("Integer").field(&v).finish(),
            Value::UInteger(v) => f.debug_tuple("UInteger").field(&v).finish(),
            Value::Bytes(ref v) => f.debug_tuple("Bytes").field(&BinStr(v)).finish(),
            Value::Array(ref v) => f.debug_tuple("Array").field(v).finish(),
            Value::Dict(ref v) => f.debug_tuple("Dict").field(&DictDebugger(v)).finish(),
        }
    }
}

impl ser::Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match *self {
            Value::Integer(v) => serializer.serialize_i64(v),
            Value::UInteger(v) => serializer.serialize_u64(v),
            Value::Bytes(ref v) => {
                let v2 = ByteBuf::from(v.clone());
                serializer.serialize_bytes(&v2)
            }
            Value::Array(ref v) => v.serialize(serializer),
            Value::Dict(ref v) => {
                let mut v2: BTreeMap<ByteBuf, Value> = Default::default();
                for (k, v) in v {
                    v2.insert(ByteBuf::from(k.clone()), v.clone());
                }
                v2.serialize(serializer)
            }
        }
    }
}

impl<'de> de::Deserialize<'de> for Value {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> de::Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "a bencode value-like")
            }

            #[inline]
            fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
                Ok(Value::Integer(value))
            }

            #[inline]
            fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
                Ok(Value::UInteger(value))
            }

            #[inline]
            fn visit_seq<A>(self, mut seq: A) -> Result<Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(v) = seq.next_element::<Value>()? {
                    values.push(v);
                }
                Ok(Value::Array(values))
            }

            #[inline]
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut values: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
                while let Some(k) = map.next_key::<ByteBuf>()? {
                    let v = map.next_value::<Value>()?;
                    values.insert(k.into_vec(), v);
                }
                Ok(Value::Dict(values))
            }

            #[inline]
            fn visit_borrowed_bytes<E>(self, value: &'de [u8]) -> Result<Self::Value, E> {
                Ok(Value::Bytes(value.to_vec().into()))
            }

            #[inline]
            fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E> {
                Ok(Value::Bytes(value.to_vec().into()))
            }

            #[inline]
            fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E> {
                Ok(Value::Bytes(From::from(value)))
            }

            #[inline]
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(Value::Bytes(value.as_bytes().to_vec().into()))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}
