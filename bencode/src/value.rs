use serde::{ser, de};
use serde::bytes::ByteBuf;
use std::collections::BTreeMap;

#[derive(PartialEq, Eq, Debug)]
pub enum Value {
    Integer(i64),
    Bytes(ByteBuf),
    Array(Vec<Value>),
    Dict(BTreeMap<ByteBuf, Value>),
}

impl ser::Serialize for Value {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error>
        where S: ser::Serializer,
    {
        match *self {
            Value::Integer(v) => serializer.visit_i64(v),
            Value::Bytes(ref v) => serializer.visit_bytes(&v),
            Value::Array(ref v) => v.serialize(serializer),
            Value::Dict(ref v) => v.serialize(serializer),
        }
    }
}

impl de::Deserialize for Value {
    #[inline]
    fn deserialize<D>(deserializer: &mut D) -> Result<Value, D::Error>
        where D: de::Deserializer,
    {
        struct ValueVisitor;

        impl de::Visitor for ValueVisitor {
            type Value = Value;

            #[inline]
            fn visit_i64<E>(&mut self, value: i64) -> Result<Value, E> {
                Ok(Value::Integer(value))
            }

            #[inline]
            fn visit_seq<V>(&mut self, visitor: V) -> Result<Value, V::Error>
                where V: de::SeqVisitor,
            {
                let values = try!(de::impls::VecVisitor::new().visit_seq(visitor));
                Ok(Value::Array(values))
            }

            #[inline]
            fn visit_map<V>(&mut self, visitor: V) -> Result<Value, V::Error>
                where V: de::MapVisitor,
            {
                let values: BTreeMap<ByteBuf, Value> = try!(
                    de::impls::BTreeMapVisitor::new().visit_map(visitor));

                // FIXME: eliminate intermediate data structure.
                let values = values.into_iter().map(|(k, v)| (k.into(), v)).collect();

                Ok(Value::Dict(values))
            }

            #[inline]
            fn visit_byte_buf<E>(&mut self, value: Vec<u8>) -> Result<Self::Value, E> {
                Ok(Value::Bytes(From::from(value)))
            }
        }

        deserializer.visit(ValueVisitor)
    }
}