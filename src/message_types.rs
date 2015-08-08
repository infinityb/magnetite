use std::slice::bytes::copy_memory;
use byteorder::{BigEndian, ByteOrder};

use super::bitfield::UnmeasuredBitfield;


// keep-alive is implemented with a zero-sized message.
// It will be intrinsic to the parser.

#[repr(u8)]
enum MessageType {
    // data: none
    Choke = b'0',
    // data: none
    Unchoke = b'1',
    // data: none
    Interested = b'2',
    // data: none
    NotInterested = b'3',
    // data: (piece_num: u32),
    Have = b'4',
    // data: the bitfield (TBD)
    Bitfield = b'5',
    // data: (piece_num: u32, begin: u32, length: u32)
    //       begin is the offset within the piece. 
    Request = b'6',
    // data: (piece_num: u32, begin: u32, block: [u8; 0x4000])
    //       begin is the offset within the piece.
    Piece = b'7',
    // data: (piece_num: u32, begin: u32, length: u32)
    //       begin is the offset within the piece. 
    Cancel = b'8',
    // data: (dht_port: u16)
    Port = b'9',
}

fn parse_have(buf: &[u8]) -> Result<u32, Error> {
    if buf.len() < 4 {
        return Err(Error::Truncated);
    }
    if buf.len() != 4 {
        return Err(Error::Invalid);
    }
    Ok(BigEndian::read_u32(&buf[0..4]))
}

fn parse_bitfield(buf: &[u8]) -> Result<UnmeasuredBitfield, Error> {
    Ok(UnmeasuredBitfield::new(buf.to_vec()))
}

fn parse_port(buf: &[u8]) -> Result<u16, Error> {
    if buf.len() < 2 {
        return Err(Error::Truncated);
    }
    if buf.len() != 2 {
        return Err(Error::Invalid);
    }
    Ok(BigEndian::read_u16(&buf[0..2]))
}


impl MessageType {
    unsafe fn from_u8_unchecked(val: u8) -> MessageType {
        ::std::mem::transmute(val)
    }

    pub fn from_u8(val: u8) -> Option<MessageType> {
        match val {
            v if b'0' <= v && v <= b'9' => {
                Some(unsafe { MessageType::from_u8_unchecked(v) })
            },
            _ => None
        }
    }

    pub fn read_message(&self, buf: &[u8]) -> Result<Message, Error> {
        use self::MessageType as MT;

        match *self {
            MT::Choke => {
                assert_eq!(buf.len(), 0);
                Ok(Message::Choke)
            },
            MT::Unchoke => {
                assert_eq!(buf.len(), 0);
                Ok(Message::Unchoke)
            },
            MT::Interested => {
                assert_eq!(buf.len(), 0);
                Ok(Message::Interested)
            },
            MT::NotInterested => {
                assert_eq!(buf.len(), 0);
                Ok(Message::NotInterested)
            },
            MT::Have => parse_have(buf).map(Message::Have),
            MT::Bitfield => parse_bitfield(buf).map(Message::Bitfield),
            MT::Request => Request::parse(buf).map(Message::Request),
            MT::Piece => Piece::parse(buf).map(Message::Piece),
            MT::Cancel => Cancel::parse(buf).map(Message::Cancel),
            MT::Port => parse_port(buf).map(Message::Port),
        }
    }
}

pub struct Request {
    piece_num: u32,
    begin: u32,
    length: u32,
}

impl Request {
    fn parse(buf: &[u8]) -> Result<Self, Error> {
        if buf.len() < 12 {
            return Err(Error::Truncated);
        }
        if buf.len() != 12 {
            return Err(Error::Invalid);
        }
        Ok(Request {
            piece_num: BigEndian::read_u32(&buf[0..4]),
            begin: BigEndian::read_u32(&buf[4..8]),
            length: BigEndian::read_u32(&buf[8..12]),
        })
    }
}

pub struct Piece {
    piece_num: u32,
    begin: u32,
    length: usize,
    block: [u8; 0x4000],
}

impl Piece {
    fn parse(buf: &[u8]) -> Result<Self, Error> {
        if buf.len() < 8 {
            return Err(Error::Truncated);
        }
        let piece_num = BigEndian::read_u32(&buf[0..4]);
        let begin = BigEndian::read_u32(&buf[4..8]);
        let length = buf.len() - 8;

        let data = &buf[8..];
        let mut block = [0; 0x4000];
        if block.len() < data.len() {
            return Err(Error::LargeBlock);
        }
        copy_memory(data, &mut block);

        Ok(Piece {
            piece_num: piece_num,
            begin: begin,
            length: length,
            block: block,
        })
    }
}

pub struct Cancel {
    piece_num: u32,
    begin: u32,
    length: u32,
}

impl Cancel {
    fn parse(buf: &[u8]) -> Result<Self, Error> {
        if buf.len() < 12 {
            return Err(Error::Truncated);
        }
        if buf.len() != 12 {
            return Err(Error::Invalid);
        }
        Ok(Cancel {
            piece_num: BigEndian::read_u32(&buf[0..4]),
            begin: BigEndian::read_u32(&buf[4..8]),
            length: BigEndian::read_u32(&buf[8..12]),
        })
    }
}

const PIECE: u8 = b'7';

pub enum Message {
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(UnmeasuredBitfield),
    Request(Request),
    Piece(Piece),
    Cancel(Cancel),
    Port(u16),
}

pub enum Error {
    Unknown(u8),
    Truncated,
    Invalid,
    LargeBlock,
}

fn parse(buf: &[u8]) -> Result<Message, Error> {
    if buf.len() == 0 {
        return Err(Error::Truncated);
    }
    let mtype = try!(MessageType::from_u8(buf[0]).ok_or(Error::Invalid));
    mtype.read_message(&buf[1..])
}