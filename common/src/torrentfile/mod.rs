mod enum_err;
mod info;

pub use serde::bytes::ByteBuf;
pub use self::enum_err::EnumErr;
pub use self::info::{InfoFile};

use bencode;
use sha1;
use metorrent_util::Sha1;

pub enum InfoHashError {
    FormatError,
    NotADictionary,
    NoInfo,
}

pub fn get_info_hash(buffer: &[u8]) -> Result<Sha1, InfoHashError> {
    use bencode::Value;
    use self::InfoHashError::*;

    let val: Value = match bencode::from_slice(buffer) {
        Ok(val) => val,
        Err(_err) => return Err(FormatError),
    };
    let dict = match val {
        Value::Integer(_) => return Err(NotADictionary),
        Value::Bytes(_) => return Err(NotADictionary),
        Value::Array(_) => return Err(NotADictionary),
        Value::Dict(dict) => dict,
    };
    let info: ByteBuf = From::from(b"info".to_vec());

    let info = match dict.get(&info) {
        Some(info) => info,
        None => return Err(NoInfo),
    };
    let doc2 = match bencode::to_vec(&info) {
        Ok(vec) => vec,
        Err(err) => unreachable!("{:?}", err),
    };
    let mut hasher = sha1::Sha1::new();
    hasher.update(&doc2);

    let mut buf = [0; 20];
    hasher.output(&mut buf);
    Ok(Sha1::new(buf))
}