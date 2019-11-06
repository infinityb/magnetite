use std::str;
use std::borrow::Cow;
use std::error::Error as StdError;

// use serde::de;

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

#[derive(Debug)]
pub struct Parser<'de> {
    scratch_offset: usize,
    scratch: Vec<u8>,
    decode_chunk: &'de [u8],
}

#[derive(Debug)]
pub enum Node<'a> {
    DictionaryStart,
    ArrayStart,
    ContainerEnd,
    Integer(Cow<'a, str>),
    Bytes(Cow<'a, [u8]>),
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
pub enum IResult<T, E> {
    // we need to split the scratch offset handling off from the protocol decode
    // so we can avoid filling scratch as much as possible  in order to support zero-copy 
    Done(T), 
    // use this information to fill scratch
    ReadMore(usize),
    Err(E),
}

#[derive(Debug)]
pub struct Tokenizer<'de> {
    scratch_offset: usize,
    scratch: Vec<u8>,
    data: &'de [u8],
}

impl<'de> Tokenizer<'de> {
    pub fn new(data: &'de [u8]) -> Tokenizer<'de> {
        Tokenizer {
            scratch_offset: 0,
            scratch: Vec::new(),
            data,
        }
    }

    fn add_data(&mut self, data: &'de [u8]) {
        // flush remainder of data into scratch buffer.
        self.scratch.extend(self.data);
        self.data = data;
    }

    pub fn next(&mut self) -> IResult<Node<'de>, Box<dyn StdError>> {
        fn fixup_scratch<T>(data: &mut Vec<T>, split_point: usize) where T: Copy {
            let data_valid_length = data.len() - split_point;
            let (into, from) = data.split_at_mut(split_point);
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
                        self.scratch_offset += length;
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
                }
            } if !buf.is_empty() {
                return match parse_next_item(buf) {
                    IResult::Done((length, v)) => {
                        self.data = &self.data[length..];
                        IResult::Done(v)
                    },
                    IResult::ReadMore(length) => {
                        self.scratch.extend(self.data);
                        self.data = &[];
                        IResult::ReadMore(length)
                    }
                    IResult::Err(err) => IResult::Err(err),
                }
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
                let value = check_is_digits(&rem_slice[..pos-1]).expect("xTODO: error handling");
                rem_iter = rem_slice[pos..].iter();
                IResult::Done((1 + pos, Node::Integer(Cow::Borrowed(value))))
            },
            None => {
                IResult::ReadMore(1)
            }
        }
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
                IResult::Done((pos + length + 1, Node::Bytes(Cow::Borrowed(&rest[..length]))))
            },
            None => {
                IResult::ReadMore(1)
            }
        }
        v => panic!("zTODO: error handling - bad byte {}", *v as char),
    }
}


#[cfg(test)]
mod test {
    use std::borrow::Cow;
    use super::{IResult, Node, Tokenizer};

    fn allocation_how(node: &Node) -> &'static str {
        match *node {
            Node::DictionaryStart
            | Node::ArrayStart
            | Node::ContainerEnd => "non-allocating value",
            
            Node::Integer(Cow::Owned(_))
            | Node::Bytes(Cow::Owned(_)) => "allocated",

            Node::Integer(Cow::Borrowed(_))
            | Node::Bytes(Cow::Borrowed(_)) => "non-allocating borrow: &'de",
        }
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
            match tokenizer.next() {
                IResult::Done(ref v) => {
                    println!("got token {:?}, {}", v, allocation_how(v));
                },
                IResult::ReadMore(_) => {
                    if let Some(next) = next_bufs.next() {
                        tokenizer.add_data(next);
                    } else {
                        break;
                    }
                },
                IResult::Err(err) => panic!("error: {:?}", err),
            }
        }
    }

    #[test]
    fn test_one_buffer() {
        let mut tokenizer = Tokenizer::new(b"i441e5:quackd5:quack4:boop10:somefrogs!0:e");
        loop {
            match tokenizer.next() {
                IResult::Done(ref v) => {
                    println!("got token {:?}, {}", v, allocation_how(v));
                },
                IResult::ReadMore(_) => break,
                IResult::Err(err) => panic!("error: {:?}", err),
            }            
        }
    }

}
