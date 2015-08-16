use std::fmt;
use std::io::{self, Read, Write};
use std::slice::bytes::copy_memory;

use byteorder::{BigEndian, ByteOrder, WriteBytesExt, ReadBytesExt};
use super::bitfield::UnmeasuredBitfield;

pub enum Error {
    Unknown(u8),
    Truncated,
    Invalid,
    LargeBlock,
}

trait BitTorrentMessage {
    fn write_len(&self) -> usize;
    fn write(&self, wri: &mut io::Cursor<Vec<u8>>);
}

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
            MT::Choke => Ok(Message::Choke),
            MT::Unchoke => Ok(Message::Unchoke),
            MT::Interested => Ok(Message::Interested),
            MT::NotInterested => Ok(Message::NotInterested),
            MT::Have => parse_have(buf).map(Message::Have),
            MT::Bitfield => parse_bitfield(buf).map(Message::Bitfield),
            MT::Request => Request::parse(buf).map(Message::Request),
            MT::Piece => Piece::parse(buf).map(Message::Piece),
            MT::Cancel => Cancel::parse(buf).map(Message::Cancel),
            MT::Port => parse_port(buf).map(Message::Port),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Request {
    piece_num: u32,
    begin: u32,
    length: u32,
}

impl BitTorrentMessage for Request {
    fn write_len(&self) -> usize { 12 }

    fn write(&self, wri: &mut io::Cursor<Vec<u8>>) {
        use byteorder::WriteBytesExt;
        wri.write_u32::<BigEndian>(self.piece_num).unwrap();
        wri.write_u32::<BigEndian>(self.begin).unwrap();
        wri.write_u32::<BigEndian>(self.length).unwrap();
    }
}

impl Request {
    fn parse(buf: &[u8]) -> Result<Self, Error> {
        use byteorder::ReadBytesExt;
        if buf.len() < 12 {
            return Err(Error::Truncated);
        }
        if buf.len() != 12 {
            return Err(Error::Invalid);
        }
        let mut rdr = io::Cursor::new(buf);
        Ok(Request {
            piece_num: rdr.read_u32::<BigEndian>().unwrap(),
            begin: rdr.read_u32::<BigEndian>().unwrap(),
            length: rdr.read_u32::<BigEndian>().unwrap(),
        })
    }
}

const PIECE_LENGTH_SHL: usize = 17;

#[derive(Clone)]
pub struct Piece {
    piece_num: u32,
    begin: u32,
    block: Vec<u8>,
}

impl fmt::Debug for Piece {
    fn fmt(&self, wri: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(wri, "Piece {{ {}, {}, {}, ... }}",
            self.piece_num, self.begin, self.block.len())
    }
}

impl BitTorrentMessage for Piece {
    fn write_len(&self) -> usize {
        8 + self.block.len()
    }

    fn write(&self, wri: &mut io::Cursor<Vec<u8>>) {
        use byteorder::WriteBytesExt;
        wri.write_u32::<BigEndian>(self.piece_num).unwrap();
        wri.write_u32::<BigEndian>(self.begin).unwrap();
        wri.get_mut().extend(&self.block);
    }
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
        let mut block = Vec::new();
        if (1 << PIECE_LENGTH_SHL) < data.len() {
            return Err(Error::LargeBlock);
        }
        block.extend(data);

        Ok(Piece {
            piece_num: piece_num,
            begin: begin,
            block: block,
        })
    }
}

#[derive(Clone, Copy, Debug)]
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

impl BitTorrentMessage for Cancel {
    fn write_len(&self) -> usize { 12 }

    fn write(&self, wri: &mut io::Cursor<Vec<u8>>) {
        use byteorder::WriteBytesExt;
        wri.write_u32::<BigEndian>(self.piece_num).unwrap();
        wri.write_u32::<BigEndian>(self.begin).unwrap();
        wri.write_u32::<BigEndian>(self.length).unwrap();
    }
}

#[derive(Debug)]
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

fn tiny_message(msg_type: MessageType) -> Vec<u8> {
    let mut wri = io::Cursor::new(Vec::with_capacity(5));
    wri.write_u32::<BigEndian>(1).unwrap();
    wri.write(&[msg_type as u8]).unwrap();
    wri.into_inner()
}

impl Message {
    pub fn parse(buf: &[u8]) -> Result<Self, Error> {
        if buf.len() == 0 {
            return Err(Error::Truncated);
        }
        let mtype = try!(MessageType::from_u8(buf[0]).ok_or(Error::Invalid));
        mtype.read_message(&buf[1..])
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match *self {
            Message::Choke => tiny_message(MessageType::Choke),
            Message::Unchoke => tiny_message(MessageType::Unchoke),
            Message::Interested => tiny_message(MessageType::Interested),
            Message::NotInterested => tiny_message(MessageType::NotInterested),
            Message::Have(val) => {
                let mut wri = io::Cursor::new(Vec::with_capacity(9));
                wri.write_u32::<BigEndian>(5).unwrap();
                wri.write(&[MessageType::Have as u8]).unwrap();
                wri.write_u32::<BigEndian>(val).unwrap();
                wri.into_inner()
            },
            Message::Bitfield(ref bf) => {
                bf.as_bytes().map(|by| {
                    let mut out = Vec::with_capacity(1 + by.len());
                    out.push(MessageType::Bitfield as u8);
                    out.extend(by);
                    out
                }).unwrap_or(Vec::new())
            },
            Message::Request(req) => {
                let msg_length = 1 + BitTorrentMessage::write_len(&req);
                let mut wri = io::Cursor::new(Vec::with_capacity(4 + msg_length));
                wri.write_u32::<BigEndian>(msg_length as u32).unwrap();
                wri.write(&[MessageType::Request as u8]).unwrap();
                BitTorrentMessage::write(&req, &mut wri);
                wri.into_inner()
            },
            Message::Piece(ref piece) => {
                let msg_length = 1 + BitTorrentMessage::write_len(piece);
                let mut wri = io::Cursor::new(Vec::with_capacity(4 + msg_length));
                wri.write_u32::<BigEndian>(msg_length as u32).unwrap();
                wri.write(&[MessageType::Request as u8]).unwrap();
                BitTorrentMessage::write(piece, &mut wri);
                wri.into_inner()
            },
            Message::Cancel(can) => {
                let msg_length = 1 + BitTorrentMessage::write_len(&can);
                let mut wri = io::Cursor::new(Vec::with_capacity(4 + msg_length));
                wri.write_u32::<BigEndian>(msg_length as u32).unwrap();
                wri.write(&[MessageType::Request as u8]).unwrap();
                BitTorrentMessage::write(&can, &mut wri);
                wri.into_inner()
            },
            Message::Port(port) => {
                let mut wri = io::Cursor::new(Vec::with_capacity(7));
                wri.write_u32::<BigEndian>(3).unwrap();
                wri.write(&[MessageType::Port as u8]).unwrap();
                wri.write_u16::<BigEndian>(port).unwrap();
                wri.into_inner()
            },
        }
    }
}
