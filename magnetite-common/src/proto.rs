use std::fmt;

use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BytesMut};
use thiserror::Error;

use crate::{TorrentId, BytesCow};

pub type IResult<T, E> = Result<(usize, T), IErr<E>>;

pub enum IErr<E> {
    // 0 means unknown, as it is invalid to need 0 more bytes.
    Incomplete(usize),
    Error(E),
}

impl<E> From<E> for IErr<E> {
    fn from(e: E) -> IErr<E> {
        IErr::Error(e)
    }
}

// Use `Suggest Piece` later to optimize storage access?

// https://wiki.theory.org/BitTorrentSpecification#request:_.3Clen.3D0013.3E.3Cid.3D6.3E.3Cindex.3E.3Cbegin.3E.3Clength.3E
pub const DEFAULT_BLOCK_SIZE: u32 = 1 << 14;  // 16KiB - 13.1ms @ 10Mbps

const RESERVED_SIZE: usize = 8;

// the pstrlen and pstr bytes.
const HANDSHAKE_PREFIX: &[u8] = b"\x13BitTorrent protocol";
pub const HANDSHAKE_SIZE: usize =
    HANDSHAKE_PREFIX.len() + RESERVED_SIZE + TorrentId::LENGTH + TorrentId::LENGTH;

#[inline]
fn funky_size(size: usize, expectation: &'static str) -> WireProtocolViolation {
    WireProtocolViolation {
        got: WireProtocolViolationUnexpected::RecordSize(size),
        expectation,
    }
}

fn size_check(got_size: usize, expected_size: usize, expectation_msg: &'static str)
    -> Result<(), WireProtocolViolation>
{
    if got_size != expected_size {
        return Err(funky_size(got_size, expectation_msg));
    }
    Ok(())
}

#[derive(Debug)]
pub enum WireProtocolViolationUnexpected {
    RecordKind(u8),
    RecordSize(usize),
}

#[derive(Debug)]
pub struct WireProtocolViolation {
    pub got: WireProtocolViolationUnexpected,
    pub expectation: &'static str,
}

impl fmt::Display for WireProtocolViolation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "WireProtocolViolation")
    }
}

impl std::error::Error for WireProtocolViolation {}

// --

#[derive(Debug)]
pub struct BadHandshake {
    pub reason: BadHandshakeReason,
}

#[derive(Error, Debug, Clone)]
pub enum BadHandshakeReason {
    #[error("junk data")]
    JunkData,
    #[error("unexpected info hash, expected: {expected:?}, got: {got:?}")]
    UnexpectedInfoHash {
        got: TorrentId,
        expected: Option<TorrentId>,
    },
    #[error("unexpected peer ID, expected: {expected:?}, got: {got:?}")]
    UnexpectedPeerId {
        got: TorrentId,
        expected: Option<TorrentId>,
    },
}

impl BadHandshake {
    pub fn junk_data() -> BadHandshake {
        BadHandshake {
            reason: BadHandshakeReason::JunkData,
        }
    }

    pub fn unknown_info_hash(ih: &TorrentId) -> BadHandshake {
        BadHandshake {
            reason: BadHandshakeReason::UnexpectedInfoHash {
                got: *ih,
                expected: None,
            },
        }
    }

    pub fn unexpected_info_hash(got: &TorrentId, exp: Option<&TorrentId>) -> BadHandshake {
        BadHandshake {
            reason: BadHandshakeReason::UnexpectedInfoHash {
                got: *got,
                expected: exp.map(|x| *x),
            },
        }
    }

    pub fn unexpected_peer_id(got: &TorrentId, exp: Option<&TorrentId>) -> BadHandshake {
        BadHandshake {
            reason: BadHandshakeReason::UnexpectedPeerId {
                got: *got,
                expected: exp.map(|x| *x),
            },
        }
    }
}

impl fmt::Display for BadHandshake {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for BadHandshake {}

// --

pub struct PieceSliceFormatSmall<'a>(&'a PieceSlice);

impl<'a> fmt::Display for PieceSliceFormatSmall<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "data[{}*sz+{}..][..{}]", self.0.index, self.0.begin, self.0.length)
    }
}


#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct PieceSlice {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

impl PieceSlice {
    pub fn fmt_small(&self) -> PieceSliceFormatSmall {
        PieceSliceFormatSmall(self)
    }
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

pub struct MessageDebugSmall<'b, 'a>(&'b Message<'a>);

impl<'a, 'b> fmt::Debug for MessageDebugSmall<'a, 'b> where 'a: 'b {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            Message::Keepalive
            | Message::Choke
            | Message::Unchoke
            | Message::Interested
            | Message::Uninterested
            | Message::Have { .. }
            | Message::Piece { .. }
            | Message::Port { .. } => {
                write!(f, "{:?}", self.0)
            }
            Message::Request(ref ps) => {
                write!(f, "Request({})", ps.fmt_small())
            }
            Message::Cancel(ref ps) => {
                write!(f, "Cancel({})", ps.fmt_small())
            }
            Message::Bitfield { .. } => {
                f.debug_struct("Bitfield")
                    .field("field_data", &(..))
                    .finish()
            }
        }
    }
}

impl<'a> Message<'a> {
    pub fn bitfield(bf: &'a [u8]) -> Self {
        Message::Bitfield {
            field_data: BytesCow::Borrowed(bf),
        }
    } 

    pub fn debug_small<'b>(&'b self) -> MessageDebugSmall<'b, 'a> where 'a: 'b {
        MessageDebugSmall(self)
    }

    pub fn into_owned(self) -> Message<'static> {
        match self {
            Message::Keepalive => Message::Keepalive,
            Message::Choke => Message::Choke,
            Message::Unchoke => Message::Unchoke,
            Message::Interested => Message::Interested,
            Message::Uninterested => Message::Uninterested,
            Message::Have { piece_id } => Message::Have { piece_id },
            Message::Bitfield { field_data } => Message::Bitfield {
                field_data: field_data.into_owned(),
            },
            Message::Request(ps) => Message::Request(ps),
            Message::Cancel(ps) => Message::Cancel(ps),
            Message::Piece { index, begin, data } => Message::Piece {
                index,
                begin,
                data: data.into_owned(),
            },
            Message::Port { dht_port } => Message::Port { dht_port },
        }
    }
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

pub fn deserialize_peek<'a>(from: &'a [u8]) -> IResult<Message<'a>, WireProtocolViolation> {
    if from.len() < 4 {
        return Err(IErr::Incomplete(4 - from.len()));
    }
    let size = BigEndian::read_u32(&from[..]) as usize;
    let total_size = size + 4; // with header
    if from.len() < total_size {
        return Err(IErr::Incomplete(total_size - from.len()));
    }
    if size == 0 {
        return Ok((4, Message::Keepalive));
    }
    let rest = &from[4..];
    match rest[0] {
        CHOKE_BYTE => {
            size_check(size, 1, "a message of 1 byte")?;
            Ok((total_size, Message::Choke))
        }
        UNCHOKE_BYTE => {
            size_check(size, 1, "a message of 1 byte")?;
            Ok((total_size, Message::Unchoke))
        }
        INTERESTED_BYTE => {
            size_check(size, 1, "a message of 1 byte")?;
            Ok((total_size, Message::Interested))
        }
        UNINTERESTED_BYTE => {
            size_check(size, 1, "a message of 1 byte")?;
            Ok((total_size, Message::Uninterested))
        }
        HAVE_BYTE => {
            size_check(size, 5, "a message of 5 byte")?;
            let piece_id = BigEndian::read_u32(&rest[1..]);
            Ok((total_size, Message::Have { piece_id }))
        }
        BITFIELD_BYTE => {
            let bitfield = &from[..total_size][5..];
            Ok((
                total_size,
                Message::Bitfield {
                    field_data: BytesCow::Borrowed(bitfield),
                },
            ))
        }
        REQUEST_BYTE => {
            size_check(size, 13, "a message of 13 byte")?;
            let index = BigEndian::read_u32(&rest[1..]);
            let begin = BigEndian::read_u32(&rest[5..]);
            let length = BigEndian::read_u32(&rest[9..]);
            Ok((
                total_size,
                Message::Request(PieceSlice {
                    index,
                    begin,
                    length,
                }),
            ))
        }
        PIECE_BYTE => {
            let index = BigEndian::read_u32(&rest[1..]);
            let begin = BigEndian::read_u32(&rest[5..]);
            Ok((
                total_size,
                Message::Piece {
                    index,
                    begin,
                    data: BytesCow::Borrowed(&from[..total_size][5 + 4 + 4..]),
                },
            ))
        }
        CANCEL_BYTE => {
            size_check(size, 13, "a message of 13 byte")?;
            let index = BigEndian::read_u32(&rest[1..]);
            let begin = BigEndian::read_u32(&rest[5..]);
            let length = BigEndian::read_u32(&rest[9..]);
            Ok((
                total_size,
                Message::Cancel(PieceSlice {
                    index,
                    begin,
                    length,
                }),
            ))
        }
        PORT_BYTE => {
            size_check(size, 3, "a message of 3 byte")?;
            let dht_port = BigEndian::read_u16(&rest[1..]);
            Ok((total_size, Message::Port { dht_port }))
        }
        _ => Err(WireProtocolViolation {
            got: WireProtocolViolationUnexpected::RecordKind(rest[0]),
            expectation: "a valid bittorrent command byte",
        }.into()),
    }
}

pub fn deserialize_pop(from: &mut BytesMut) -> IResult<Message<'static>, WireProtocolViolation> {
    let (sz, v) = deserialize_peek(&from[..])?;
    let v = v.into_owned();
    from.advance(sz);
    Ok((sz, v))
}

pub fn deserialize_pop_opt(from: &mut BytesMut) -> Result<Option<Message<'static>>, WireProtocolViolation> {
    let (sz, v) = match deserialize_peek(&from[..]) {
        Ok((sz, v)) => (sz, v),
        Err(IErr::Incomplete(..)) => return Ok(None),
        Err(IErr::Error(e)) => return Err(e),
    };
    let v = v.into_owned();
    from.advance(sz);
    Ok(Some(v))
}

pub fn serialize(wbuf: &mut BytesMut, msg: &Message) {
    fn size_and_id(scratch: &mut [u8], size: u32, id: u8) -> &[u8] {
        BigEndian::write_u32(&mut scratch[..], size);
        scratch[4] = id;
        &scratch[..5]
    }

    let mut header_scratch = [0; 5];
    match *msg {
        Message::Keepalive => {
            BigEndian::write_u32(&mut header_scratch[..], 0);
            wbuf.extend_from_slice(&header_scratch[..4]);
        }
        Message::Choke => {
            let header_data = size_and_id(&mut header_scratch, 1, CHOKE_BYTE);
            wbuf.extend_from_slice(header_data);
        }
        Message::Unchoke => {
            let header_data = size_and_id(&mut header_scratch, 1, UNCHOKE_BYTE);
            wbuf.extend_from_slice(header_data);
        }
        Message::Interested => {
            let header_data = size_and_id(&mut header_scratch, 1, INTERESTED_BYTE);
            wbuf.extend_from_slice(header_data);
        }
        Message::Uninterested => {
            let header_data = size_and_id(&mut header_scratch, 1, UNINTERESTED_BYTE);
            wbuf.extend_from_slice(header_data);
        }
        Message::Have { piece_id } => {
            let header_data = size_and_id(&mut header_scratch, 1, HAVE_BYTE);
            let mut have_buf = [0; 4];
            BigEndian::write_u32(&mut have_buf, piece_id);
            wbuf.extend_from_slice(header_data);
            wbuf.extend_from_slice(&have_buf);
        }
        Message::Bitfield { ref field_data } => {
            let field_length = field_data.as_slice().len() as u32;
            let header_data = size_and_id(&mut header_scratch, 1 + field_length, BITFIELD_BYTE);
            wbuf.extend_from_slice(&header_data);
            wbuf.extend_from_slice(field_data.as_slice());
        }
        Message::Request(ref ps) => {
            let mut buf = [0; 12];
            BigEndian::write_u32(&mut buf[0..], ps.index);
            BigEndian::write_u32(&mut buf[4..], ps.begin);
            BigEndian::write_u32(&mut buf[8..], ps.length);
            let header_data = size_and_id(&mut header_scratch, 13, REQUEST_BYTE);
            wbuf.extend_from_slice(&header_data);
            wbuf.extend_from_slice(&buf);
        }
        Message::Cancel(ref ps) => {
            let mut buf = [0; 12];
            BigEndian::write_u32(&mut buf[0..], ps.index);
            BigEndian::write_u32(&mut buf[4..], ps.begin);
            BigEndian::write_u32(&mut buf[8..], ps.length);
            let header_data = size_and_id(&mut header_scratch, 13, CANCEL_BYTE);
            wbuf.extend_from_slice(&header_data);
            wbuf.extend_from_slice(&buf);
        }
        Message::Piece {
            index,
            begin,
            ref data,
        } => {
            let mut buf = [0; 8];
            BigEndian::write_u32(&mut buf[0..], index);
            BigEndian::write_u32(&mut buf[4..], begin);
            let header_data = size_and_id(
                &mut header_scratch,
                (1 + buf.len() + data.as_slice().len()) as u32,
                PIECE_BYTE,
            );

            wbuf.extend_from_slice(&header_data);
            wbuf.extend_from_slice(&buf);
            wbuf.extend_from_slice(&data.as_slice());
        }
        Message::Port { dht_port } => {
            let mut buf = [0; 2];
            BigEndian::write_u16(&mut buf[..], dht_port);

            let header_data = size_and_id(&mut header_scratch, 1 + buf.len() as u32, PORT_BYTE);
            wbuf.extend_from_slice(&header_data);
            wbuf.extend_from_slice(&buf);
        }
    }
}

#[derive(Debug)]
pub struct Handshake {
    pub reserved: [u8; RESERVED_SIZE],
    pub info_hash: TorrentId,
    pub peer_id: TorrentId,
}

impl Handshake {
    pub fn zero() -> Handshake {
        Handshake {
            reserved: [0; RESERVED_SIZE],
            info_hash: TorrentId::zero(),
            peer_id: TorrentId::zero(),
        }
    }

    pub fn serialize_bytes<'a>(
        scratch: &'a mut [u8],
        info_hash: &TorrentId,
        peer_id: &TorrentId,
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

    pub fn parse(data: &[u8]) -> IResult<Handshake, BadHandshake> {
        if data.len() < HANDSHAKE_SIZE {
            return Err(IErr::Incomplete(HANDSHAKE_SIZE - data.len()));
        }

        let (prefix, rest) = data.split_at(HANDSHAKE_PREFIX.len());
        if prefix != HANDSHAKE_PREFIX {
            return Err(BadHandshake::junk_data().into());
        }

        let (reserve_buf, rest) = rest.split_at(RESERVED_SIZE);
        let (info_hash_buf, rest) = rest.split_at(TorrentId::LENGTH);
        let (peer_id_buf, _) = rest.split_at(TorrentId::LENGTH);

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
        Ok((HANDSHAKE_SIZE, hs))
    }
}

