use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use super::MagnetiteRequestKind;
use crate::model::{MagnetiteError, TorrentID};

pub use self::request::{read_request_from_buffer, write_request_to_buffer};
pub use self::response::{read_response_from_buffer, write_response_to_buffer};

const MAX_RESPONSE_SIZE: u32 = 64 * 1024;

mod request {
    use byteorder::{BigEndian, ByteOrder};
    use bytes::{Bytes, BytesMut};
    use serde::{Deserialize, Serialize};

    use super::{
        MagnetiteWirePieceRequest, MagnetiteWireRequest, MagnetiteWireRequestPayload,
        MAX_RESPONSE_SIZE,
    };
    use crate::model::{MagnetiteError, TorrentID};

    #[derive(Serialize, Deserialize)]
    struct MagnetiteLowRequest {
        txid: u64,
        payload: MagnetiteLowRequestPayload,
    }

    #[derive(Serialize, Deserialize)]
    enum MagnetiteLowRequestPayload {
        Piece(MagnetiteLowPieceRequest),
    }

    #[derive(Serialize, Deserialize)]
    struct MagnetiteLowPieceRequest {
        #[serde(with = "super::torrent_id")]
        content_key: TorrentID,
        piece_index: u32,
        piece_offset: u32,
        fetch_length: u32,
    }

    pub fn read_request_from_buffer(
        rbuf: &mut BytesMut,
    ) -> Result<Option<MagnetiteWireRequest>, MagnetiteError> {
        // 32 bit total length + 32 bit header length.
        if rbuf.len() < 8 {
            return Ok(None);
        }

        let total_length = BigEndian::read_u32(&rbuf[..]);
        let header_length = BigEndian::read_u32(&rbuf[4..]);
        if MAX_RESPONSE_SIZE < total_length {
            return Err(MagnetiteError::ProtocolViolation);
        }

        let header_length = header_length as usize;
        let total_length = total_length as usize;

        // completeness checks
        if rbuf.len() - 8 < header_length {
            return Err(MagnetiteError::ProtocolViolation);
        }
        if rbuf.len() - 4 < total_length {
            return Err(MagnetiteError::ProtocolViolation);
        }

        let header_bytes = &rbuf[8..][..header_length as usize];
        let resp_header: MagnetiteLowRequest =
            rmp_serde::from_read(header_bytes).map_err(|_| MagnetiteError::ProtocolViolation)?;

        let data_bytes = rbuf[8..][header_length as usize..].to_vec();

        drop(header_bytes);

        drop(rbuf.split_to(total_length + 4));

        Ok(Some(match resp_header.payload {
            MagnetiteLowRequestPayload::Piece(p) => MagnetiteWireRequest {
                txid: resp_header.txid,
                payload: MagnetiteWireRequestPayload::Piece(MagnetiteWirePieceRequest {
                    content_key: p.content_key,
                    piece_index: p.piece_index,
                    piece_offset: p.piece_offset,
                    fetch_length: p.fetch_length,
                }),
            },
        }))
    }

    pub fn write_request_to_buffer(
        wbuf: &mut BytesMut,
        m: &MagnetiteWireRequest,
    ) -> Result<(), MagnetiteError> {
        // FIXME: we can preserve wbuf by measuring it first,
        // and truncating it on failure.

        use rmp_serde::{Deserializer, Serializer};

        let low;
        match m.payload {
            MagnetiteWireRequestPayload::Piece(ref p) => {
                low = MagnetiteLowRequest {
                    txid: m.txid,
                    payload: MagnetiteLowRequestPayload::Piece(MagnetiteLowPieceRequest {
                        content_key: p.content_key,
                        piece_index: p.piece_index,
                        piece_offset: p.piece_offset,
                        fetch_length: p.fetch_length,
                    }),
                };
            }
        };

        let header_length = [0; 4];
        let total_length = [0; 4];

        let mut buf: Vec<u8> = Vec::new();
        buf.extend(&[0; 8]);
        low.serialize(&mut rmp_serde::Serializer::new(&mut buf).with_struct_map().with_string_variants())
            .unwrap();
        let header_length = buf.len() - 8;
        BigEndian::write_u32(&mut buf[4..], header_length as u32);
        let total_length = buf.len() - 4;
        BigEndian::write_u32(&mut buf[..], total_length as u32);

        wbuf.extend_from_slice(&buf[..]);

        Ok(())
    }

    const MW_TEST0: MagnetiteWireRequest = MagnetiteWireRequest {
        txid: 0xAAAAA9,
        payload: MagnetiteWireRequestPayload::Piece(MagnetiteWirePieceRequest {
            content_key: TorrentID([
                0x1A, 0x5C, 0x64, 0x8C, 0xB0, 0x0B, 0xF3, 0xE2, 0xB9, 0x27, 0x58, 0x08, 0x06, 0x29,
                0xC9, 0xC7, 0xA9, 0x2C, 0xD3, 0x7D,
            ]),
            piece_index: 12345,
            piece_offset: 11111,
            fetch_length: 64 * 1024,
        }),
    };

    #[test]
    fn check_encode_decode_roundtrip() {
        use bencode::BinStr;

        let mut wbuf = BytesMut::new();
        write_request_to_buffer(&mut wbuf, &MW_TEST0).unwrap();
        println!("encoded = {:?}", BinStr(&wbuf[..]));

        wbuf.extend_from_slice(b"\0");
        let out = read_request_from_buffer(&mut wbuf).unwrap().unwrap();
        assert_eq!(MW_TEST0, out);
        assert_eq!(&wbuf[..], b"\0");
    }

    #[test]
    fn protocol_regression() {
        let data: &[u8] = b"\0\0\0r\0\0\0n\x82\xa4txid\xce\0\xaa\xaa\xa9\xa7payload\x81\xa5Piece\x84\xabcontent_key\xc4\x14\x1a\\d\x8c\xb0\x0b\xf3\xe2\xb9'X\x08\x06)\xc9\xc7\xa9,\xd3}\xabpiece_index\xcd09\xacpiece_offset\xcd+g\xacfetch_length\xce\0\x01\0\0\0";
        let mut rbuf = BytesMut::new();
        rbuf.extend_from_slice(data);

        let out = read_request_from_buffer(&mut rbuf).unwrap().unwrap();
        assert_eq!(out, MW_TEST0);

        assert_eq!(&rbuf[..], b"\0");
    }
}

mod response {
    use byteorder::{BigEndian, ByteOrder};
    use bytes::{Bytes, BytesMut};
    use serde::{Deserialize, Serialize};

    use super::{
        MagnetiteWirePieceResponse, MagnetiteWireResponse, MagnetiteWireResponsePayload,
        MAX_RESPONSE_SIZE,
    };
    use crate::model::MagnetiteError;

    #[derive(Serialize, Deserialize)]
    struct MagnetiteLowResponse {
        txid: u64,
        payload: MagnetiteLowResponsePayload,
    }

    #[derive(Serialize, Deserialize)]
    enum MagnetiteLowResponsePayload {
        Piece,
    }

    pub fn read_response_from_buffer(
        rbuf: &mut BytesMut,
    ) -> Result<Option<MagnetiteWireResponse>, MagnetiteError> {
        // 32 bit total length + 32 bit header length.
        if rbuf.len() < 8 {
            return Ok(None);
        }

        let total_length = BigEndian::read_u32(&rbuf[..]);
        let header_length = BigEndian::read_u32(&rbuf[4..]);
        if MAX_RESPONSE_SIZE < total_length {
            return Err(MagnetiteError::ProtocolViolation);
        }

        let header_length = header_length as usize;
        let total_length = total_length as usize;

        // completeness checks
        if rbuf.len() - 8 < header_length {
            return Err(MagnetiteError::ProtocolViolation);
        }
        if rbuf.len() - 4 < total_length {
            return Err(MagnetiteError::ProtocolViolation);
        }

        let header_bytes = &rbuf[8..][..header_length as usize];
        let resp_header: MagnetiteLowResponse =
            rmp_serde::from_read(header_bytes).map_err(|_| MagnetiteError::ProtocolViolation)?;

        let data_bytes = rbuf[4..][..total_length][4..][header_length as usize..].to_vec();

        drop(header_bytes);

        drop(rbuf.split_to(total_length + 4));

        Ok(Some(match resp_header.payload {
            MagnetiteLowResponsePayload::Piece => MagnetiteWireResponse {
                txid: resp_header.txid,
                payload: MagnetiteWireResponsePayload::Piece(MagnetiteWirePieceResponse {
                    data: data_bytes.to_vec(),
                }),
            },
        }))
    }

    pub fn write_response_to_buffer(
        wbuf: &mut BytesMut,
        m: &MagnetiteWireResponse,
    ) -> Result<(), MagnetiteError> {
        // FIXME: we can preserve wbuf by measuring it first,
        // and truncating it on failure.

        use rmp_serde::{Deserializer, Serializer};

        let low;
        let mut data: &[u8] = &[];

        match m.payload {
            MagnetiteWireResponsePayload::Piece(ref p) => {
                low = MagnetiteLowResponse {
                    txid: m.txid,
                    payload: MagnetiteLowResponsePayload::Piece,
                };
                data = &p.data[..];
            }
        };

        let header_length = [0; 4];
        let total_length = [0; 4];

        let mut buf: Vec<u8> = Vec::new();
        buf.extend(&[0; 8]);
        low.serialize(&mut rmp_serde::Serializer::new(&mut buf).with_struct_map().with_string_variants())
            .unwrap();
        let header_length = buf.len() - 8;
        BigEndian::write_u32(&mut buf[4..], header_length as u32);
        buf.extend(data);
        let total_length = buf.len() - 4;
        BigEndian::write_u32(&mut buf[..], total_length as u32);

        wbuf.extend_from_slice(&buf[..]);

        Ok(())
    }

    #[test]
    fn check_encode_decode_roundtrip() {
        use bencode::BinStr;

        let mw = MagnetiteWireResponse {
            txid: 0xAAAAA9,
            payload: MagnetiteWireResponsePayload::Piece(MagnetiteWirePieceResponse {
                data: "hello world".as_bytes().to_vec(),
            }),
        };
        let mut wbuf = BytesMut::new();
        write_response_to_buffer(&mut wbuf, &mw).unwrap();
        println!("encoded = {:?}", BinStr(&wbuf[..]));

        wbuf.extend_from_slice(b"\0");
        let out = read_response_from_buffer(&mut wbuf).unwrap().unwrap();
        assert_eq!(mw, out);
        assert_eq!(&wbuf[..], b"\0");
    }

    #[test]
    fn protocol_regression() {
        let data: &[u8] = b"\0\0\0*\0\0\0\x1b\x82\xa4txid\xce\0\xaa\xaa\xa9\xa7payload\x81\xa5Piece\xc0hello world\0";
        let mut rbuf = BytesMut::new();
        rbuf.extend_from_slice(data);

        let out = read_response_from_buffer(&mut rbuf).unwrap().unwrap();
        assert_eq!(
            out,
            MagnetiteWireResponse {
                txid: 0xAAAAA9,
                payload: MagnetiteWireResponsePayload::Piece(MagnetiteWirePieceResponse {
                    data: "hello world".as_bytes().to_vec(),
                }),
            }
        );

        assert_eq!(&rbuf[..], b"\0");
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct MagnetiteWireRequest {
    pub txid: u64,
    pub payload: MagnetiteWireRequestPayload,
}

#[derive(Debug, PartialEq, Eq)]
pub enum MagnetiteWireRequestPayload {
    Piece(MagnetiteWirePieceRequest),
}

#[derive(Debug, PartialEq, Eq)]
pub struct MagnetiteWirePieceRequest {
    pub content_key: TorrentID,
    pub piece_index: u32,
    pub piece_offset: u32,
    pub fetch_length: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub struct MagnetiteWireResponse {
    pub txid: u64,
    pub payload: MagnetiteWireResponsePayload,
}

#[derive(Debug, PartialEq, Eq)]
pub enum MagnetiteWireResponsePayload {
    Piece(MagnetiteWirePieceResponse),
}

#[derive(Debug, PartialEq, Eq)]
pub struct MagnetiteWirePieceResponse {
    pub data: Vec<u8>,
}

impl MagnetiteWireRequestPayload {
    pub fn kind(&self) -> MagnetiteRequestKind {
        match self {
            MagnetiteWireRequestPayload::Piece(..) => MagnetiteRequestKind::Piece,
        }
    }
}

impl MagnetiteWireResponse {
    pub fn kind(&self) -> MagnetiteRequestKind {
        match self.payload {
            MagnetiteWireResponsePayload::Piece(..) => MagnetiteRequestKind::Piece,
        }
    }
}

mod torrent_id {
    use std::fmt;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::model::TorrentID;

    pub fn serialize<S>(id: &TorrentID, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ser.serialize_bytes(id.as_bytes())
    }

    struct TorrentIdRawBytesVisitor;

    impl<'de> serde::de::Visitor<'de> for TorrentIdRawBytesVisitor {
        type Value = TorrentID;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            write!(
                formatter,
                "a bytearray containing exactly TorrentID::LENGTH bytes"
            )
        }

        fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if v.len() != TorrentID::LENGTH {
                return Err(E::invalid_length(
                    v.len(),
                    &"a bytearray containing exactly TorrentID::LENGTH bytes",
                ));
            }

            let mut out = TorrentID::zero();
            out.as_mut_bytes().copy_from_slice(v);
            Ok(out)
        }
    }

    pub fn deserialize<'de, D>(de: D) -> Result<TorrentID, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_i32(TorrentIdRawBytesVisitor)
    }
}
