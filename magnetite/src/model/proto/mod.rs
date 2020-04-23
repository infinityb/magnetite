use byteorder::{BigEndian, ByteOrder};
use bytes::BytesMut;

use iresult::IResult;

use super::{BadHandshake, TorrentID};
use crate::model::MagnetiteError;
use crate::utils::BytesCow;

// plan:
//
// Try to use `Suggest Piece` from http://bittorrent.org/beps/bep_0006.html to
// increase cloud cache hits, so we don't have to hit the backend storage
// system as hard.

const RESERVED_SIZE: usize = 8;

// the pstrlen and pstr bytes.
const HANDSHAKE_PREFIX: &[u8] = b"\x13BitTorrent protocol";
pub const HANDSHAKE_SIZE: usize =
    HANDSHAKE_PREFIX.len() + RESERVED_SIZE + TorrentID::LENGTH + TorrentID::LENGTH;

#[inline]
fn too_short() -> MagnetiteError {
    MagnetiteError::InsufficientSpace
}

#[inline]
fn funky_size() -> MagnetiteError {
    MagnetiteError::ProtocolViolation
}

#[derive(Debug)]
pub struct PieceSlice {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

#[derive(Debug)]
pub enum Message<'a> {
    Keepalive,
    Choke,
    Unchoke,
    Interested,
    Uninterested,
    Have {
        piece_id: u32,
    },
    Bitfield {
        field_data: BytesCow<'a>,
    },
    Request(PieceSlice),
    Cancel(PieceSlice),
    Piece {
        index: u32,
        begin: u32,
        data: BytesCow<'a>,
    },
    Port {
        dht_port: u16,
    },
}

pub fn serialize_long<'a>(
    scratch: &'a mut [u8],
    command_byte: u8,
    data0: &[u8],
    data1: &[u8],
) -> Result<&'a [u8], MagnetiteError> {
    let msg_len = 1 + data0.len() + data1.len();
    let total_len = 4 + msg_len;
    if scratch.len() < total_len {
        return Err(too_short());
    }
    BigEndian::write_u32(scratch, msg_len as u32);
    scratch[4] = command_byte;
    let mut sciter = scratch[5..].iter_mut();
    for (i, o) in data0.iter().zip(&mut sciter) {
        *o = *i;
    }
    for (i, o) in data1.iter().zip(&mut sciter) {
        *o = *i;
    }
    Ok(&scratch[..total_len])
}

pub fn serialize_short<'a>(
    scratch: &'a mut [u8],
    command_byte: u8,
    data: &[u8],
) -> Result<&'a [u8], MagnetiteError> {
    serialize_long(scratch, command_byte, data, &[])
}

const CHOKE_BYTE: u8 = b'\0';
const UNCHOKE_BYTE: u8 = b'\x01';
const INTERESTED_BYTE: u8 = b'\x02';
const UNINTERESTED_BYTE: u8 = b'\x03';
const HAVE_BYTE: u8 = b'\x04';
const BITFIELD_BYTE: u8 = b'\x05';
const REQUEST_BYTE: u8 = b'\x06';
const PIECE_BYTE: u8 = b'\x07';
const CANCEL_BYTE: u8 = b'\x08';
const PORT_BYTE: u8 = b'\x09';

pub fn deserialize(from: &mut BytesMut) -> IResult<Message<'static>, MagnetiteError> {
    if from.len() < 4 {
        return IResult::ReadMore(4 - from.len());
    }
    let size = BigEndian::read_u32(&from[..]) as usize;
    let total_size = size + 4; // with header
    if from.len() < total_size {
        return IResult::ReadMore(total_size - from.len());
    }
    if size == 0 {
        drop(from.split_to(4));
        return IResult::Done(Message::Keepalive);
    }
    let rest = &mut from[4..];
    match rest[0] {
        CHOKE_BYTE => {
            if size != 1 {
                return IResult::Err(funky_size());
            }
            drop(from.split_to(size + 4));
            IResult::Done(Message::Choke)
        }
        UNCHOKE_BYTE => {
            if size != 1 {
                return IResult::Err(funky_size());
            }
            drop(from.split_to(size + 4));
            IResult::Done(Message::Unchoke)
        }
        INTERESTED_BYTE => {
            if size != 1 {
                return IResult::Err(funky_size());
            }
            drop(from.split_to(size + 4));
            IResult::Done(Message::Interested)
        }
        UNINTERESTED_BYTE => {
            if size != 1 {
                return IResult::Err(funky_size());
            }
            drop(from.split_to(size + 4));
            IResult::Done(Message::Uninterested)
        }
        HAVE_BYTE => {
            if size != 5 {
                return IResult::Err(funky_size());
            }
            let piece_id = BigEndian::read_u32(&rest[1..]);
            drop(from.split_to(size + 4));
            IResult::Done(Message::Have { piece_id })
        }
        BITFIELD_BYTE => {
            let mut message = from.split_to(size + 4);
            drop(message.split_to(5)); // remove header
            IResult::Done(Message::Bitfield {
                field_data: BytesCow::Owned(message.freeze()),
            })
        }
        REQUEST_BYTE => {
            if size != 13 {
                return IResult::Err(funky_size());
            }
            let index = BigEndian::read_u32(&rest[1..]);
            let begin = BigEndian::read_u32(&rest[5..]);
            let length = BigEndian::read_u32(&rest[9..]);
            drop(from.split_to(size + 4));
            IResult::Done(Message::Request(PieceSlice {
                index,
                begin,
                length,
            }))
        }
        PIECE_BYTE => {
            let index = BigEndian::read_u32(&rest[1..]);
            let begin = BigEndian::read_u32(&rest[5..]);
            let mut message = from.split_to(size + 4);
            drop(message.split_to(5 + 4 + 4)); // remove header, index and begin

            IResult::Done(Message::Piece {
                index,
                begin,
                data: BytesCow::Owned(message.freeze()),
            })
        }
        CANCEL_BYTE => {
            if size != 13 {
                return IResult::Err(funky_size());
            }
            let index = BigEndian::read_u32(&rest[1..]);
            let begin = BigEndian::read_u32(&rest[5..]);
            let length = BigEndian::read_u32(&rest[9..]);
            drop(from.split_to(size + 4));
            IResult::Done(Message::Cancel(PieceSlice {
                index,
                begin,
                length,
            }))
        }
        PORT_BYTE => {
            if size != 3 {
                return IResult::Err(funky_size());
            }
            let dht_port = BigEndian::read_u16(&rest[1..]);
            drop(from.split_to(size + 4));
            IResult::Done(Message::Port { dht_port })
        }
        _ => IResult::Err(MagnetiteError::ProtocolViolation),
    }
}

pub fn serialize<'a>(scratch: &'a mut [u8], msg: &Message) -> Result<&'a [u8], MagnetiteError> {
    match *msg {
        Message::Keepalive => {
            if scratch.len() < 4 {
                return Err(too_short());
            }
            BigEndian::write_u32(scratch, 0);
            Ok(&scratch[..4])
        }
        Message::Choke => serialize_short(scratch, CHOKE_BYTE, &[]),
        Message::Unchoke => serialize_short(scratch, UNCHOKE_BYTE, &[]),
        Message::Interested => serialize_short(scratch, INTERESTED_BYTE, &[]),
        Message::Uninterested => serialize_short(scratch, UNINTERESTED_BYTE, &[]),
        Message::Have { piece_id } => {
            let mut buf = [0; 4];
            BigEndian::write_u32(&mut buf[..], piece_id);
            serialize_short(scratch, HAVE_BYTE, &buf)
        }
        Message::Bitfield { ref field_data } => {
            // FIXME: convert to iovec
            serialize_short(scratch, BITFIELD_BYTE, field_data.as_slice())
        }
        Message::Request(ref ps) => {
            let mut buf = [0; 12];
            BigEndian::write_u32(&mut buf[0..], ps.index);
            BigEndian::write_u32(&mut buf[4..], ps.begin);
            BigEndian::write_u32(&mut buf[8..], ps.length);
            serialize_short(scratch, REQUEST_BYTE, &buf)
        }
        Message::Cancel(ref ps) => {
            let mut buf = [0; 12];
            BigEndian::write_u32(&mut buf[0..], ps.index);
            BigEndian::write_u32(&mut buf[4..], ps.begin);
            BigEndian::write_u32(&mut buf[8..], ps.length);
            serialize_short(scratch, CANCEL_BYTE, &buf)
        }
        Message::Piece {
            index,
            begin,
            ref data,
        } => {
            // FIXME: convert to iovec
            let mut buf = [0; 8];
            BigEndian::write_u32(&mut buf[0..], index);
            BigEndian::write_u32(&mut buf[4..], begin);
            serialize_long(scratch, PIECE_BYTE, &buf, data.as_slice())
        }
        Message::Port { dht_port } => {
            let mut buf = [0; 2];
            BigEndian::write_u16(&mut buf[..], dht_port);
            serialize_short(scratch, PORT_BYTE, &buf)
        }
    }
}

#[derive(Debug)]
pub struct Handshake {
    pub reserved: [u8; RESERVED_SIZE],
    pub info_hash: TorrentID,
    pub peer_id: TorrentID,
}

impl Handshake {
    fn zero() -> Handshake {
        Handshake {
            reserved: [0; RESERVED_SIZE],
            info_hash: TorrentID::zero(),
            peer_id: TorrentID::zero(),
        }
    }

    pub fn serialize_bytes<'a>(
        scratch: &'a mut [u8],
        info_hash: &TorrentID,
        peer_id: &TorrentID,
        _options: &[()],
    ) -> Option<&'a [u8]> {
        let mut _reserve_bytes = 0;
        if scratch.len() < HANDSHAKE_SIZE {
            return None;
        }
        let mut scratch_iter = scratch.iter_mut();
        let mut acc = 0;
        for (i, o) in HANDSHAKE_PREFIX.iter().zip(&mut scratch_iter) {
            acc += 1;
            *o = *i;
        }
        assert_eq!(acc, 20);

        let reserved_nobits = [0; RESERVED_SIZE];
        for (i, o) in reserved_nobits.iter().zip(&mut scratch_iter) {
            acc += 1;
            *o = *i;
        }
        assert_eq!(acc, 28);

        for (i, o) in info_hash.as_bytes().iter().zip(&mut scratch_iter) {
            acc += 1;
            *o = *i;
        }
        assert_eq!(acc, 48);

        for (i, o) in peer_id.as_bytes().iter().zip(&mut scratch_iter) {
            acc += 1;
            *o = *i;
        }
        assert_eq!(acc, 68);
        assert_eq!(acc, HANDSHAKE_SIZE);
        Some(&scratch[..HANDSHAKE_SIZE])
    }

    pub fn parse2(data: &mut BytesMut) -> IResult<Handshake, MagnetiteError> {
        match Self::parse(&data[..]) {
            IResult::Done((length, v)) => {
                drop(data.split_to(length));
                IResult::Done(v)
            }
            IResult::ReadMore(v) => IResult::ReadMore(v),
            IResult::Err(err) => IResult::Err(err),
        }
    }

    pub fn parse(data: &[u8]) -> IResult<(usize, Handshake), MagnetiteError> {
        if data.len() < HANDSHAKE_SIZE {
            return IResult::ReadMore(HANDSHAKE_SIZE - data.len());
        }

        let (prefix, rest) = data.split_at(HANDSHAKE_PREFIX.len());
        if prefix != HANDSHAKE_PREFIX {
            return IResult::Err(BadHandshake::junk_data().into());
        }

        let (reserve_buf, rest) = rest.split_at(RESERVED_SIZE);
        let (info_hash_buf, rest) = rest.split_at(TorrentID::LENGTH);
        let (peer_id_buf, _) = rest.split_at(TorrentID::LENGTH);

        let mut hs = Handshake::zero();
        for (o, i) in hs.reserved.iter_mut().zip(reserve_buf) {
            *o = *i;
        }
        for (o, i) in hs.info_hash.as_mut_bytes().iter_mut().zip(info_hash_buf) {
            *o = *i;
        }
        for (o, i) in hs.peer_id.as_mut_bytes().iter_mut().zip(peer_id_buf) {
            *o = *i;
        }
        IResult::Done((HANDSHAKE_SIZE, hs))
    }
}
