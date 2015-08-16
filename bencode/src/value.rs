use serde::de;
use std::collections::BTreeMap;

#[derive(PartialEq, Eq, Debug)]
pub enum Value {
    Integer(i64),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Object(BTreeMap<String, Value>),
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
                let values = try!(de::impls::BTreeMapVisitor::new().visit_map(visitor));
                Ok(Value::Object(values))
            }

            #[inline]
            fn visit_byte_buf<E>(&mut self, value: Vec<u8>) -> Result<Self::Value, E> {
            	Ok(Value::Bytes(value))
            }
        }

        deserializer.visit(ValueVisitor)
    }
}

#[cfg(test)]
mod tests {
	use super::Value;
    use super::super::from_slice;

    #[test]
    fn value_test() {
        let doc = b"d1:ad1:yle1:zi0ee1:bllelleelleleee1:ci-4e1:dllelleellel1:xeeee";
        let val: Value = from_slice(doc).expect("deserialize error");
        println!("{:#?}", val);
    }
}