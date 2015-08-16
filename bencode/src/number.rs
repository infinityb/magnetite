#[inline]
fn is_digit(val: u8) -> bool {
    b'0' <= val && val <= b'9'
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum NumberError {
    LeadingZero,
    NegativeZero,
    InvalidByte,
    Overflow,
    Truncated,
    Trailing,
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum NumberState {
    Initial,
    Negation,
    Zero,
    Digit,
    NoRoom,
    Final,
    Error(NumberError),
}

#[derive(Copy, Clone)]
struct SmallVec20 {
    len: usize,
    // big enough to hold a negative i64 or u64
    buf: [u8; 20],
}

impl SmallVec20 {
    fn new() -> Self {
        SmallVec20 {
            len: 0,
            buf: [0; 20],
        }
    }

    // Returns true iff we can push at least one more byte
    fn push(&mut self, val: u8) -> bool {
        if self.len == self.buf.len() {
            panic!("overflow");
        }
        self.buf[self.len] = val;
        self.len += 1;
        self.len < self.buf.len()
    }

    fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

#[derive(Clone)]
pub struct NumberParser {
    value_buf: SmallVec20,
    state: NumberState,
    stop: u8,
}

impl NumberParser {
    pub fn with_stop(stop: u8) -> NumberParser {
        NumberParser {
            value_buf: SmallVec20::new(),
            state: NumberState::Initial,
            stop: stop,
        }
    }

    pub fn new() -> NumberParser {
        NumberParser::with_stop(b'e')
    }

    #[inline]
    pub fn push_byte(&mut self, val: u8) -> bool {
        use self::NumberState::*;
        self.state = match self.state {
            Initial => match val {
                b'-' => {
                    self.value_buf.push(val);
                    Negation
                },
                b'0' => {
                    self.value_buf.push(val);
                    Zero
                },
                byte if is_digit(byte) => {
                    assert!(self.value_buf.push(byte));
                    Digit
                },
                _ => Error(NumberError::InvalidByte)
            },

            Negation => match val {
                b'0' => Error(NumberError::NegativeZero),
                byte if is_digit(byte) => {
                    assert!(self.value_buf.push(byte));
                    Digit
                },
                _ => Error(NumberError::InvalidByte)
            },

            Zero => match val {
                byte if byte == self.stop => Final,
                byte if is_digit(byte) => Error(NumberError::LeadingZero),
                _ => Error(NumberError::InvalidByte)
            },

            Digit => match val {
                byte if byte == self.stop => Final,
                byte if is_digit(byte) => {
                    if self.value_buf.push(byte) {
                        Digit
                    } else {
                        NoRoom
                    }
                }
                _ => Error(NumberError::InvalidByte),
            },

            NoRoom => match val {
                byte if byte == self.stop => Final,
                byte if is_digit(byte) => Error(NumberError::Overflow),
                _ => Error(NumberError::InvalidByte),
            },

            Final => Error(NumberError::Trailing),
            Error(err) => Error(err),
        };
        self.state == NumberState::Final
    }

    pub fn end<T>(&self) -> Result<T, NumberError> where T: FromDigits {
        match self.state {
            NumberState::Final => {
                FromDigits::from_digits(self.value_buf.as_slice())
            }
            NumberState::Error(err) => Err(err),
            _ => Err(NumberError::Truncated),
        }
    }
}

pub trait FromDigits {
    fn from_digits(digits: &[u8]) -> Result<Self, NumberError>;
}

macro_rules! impl_from_digits {
    ($id:ident) => {
        impl FromDigits for $id {
            fn from_digits(digits: &[u8]) -> Result<Self, NumberError> {
                use num::traits::FromPrimitive;

                assert!(digits.len() != 0);
                let is_signed_ty = Self::from_u32(0).unwrap() > Self::min_value();
                if !is_signed_ty && digits[0] == b'-' {
                    return Err(NumberError::InvalidByte);
                }

                // guaranteed to be only 0-9 and '-', and therefore a valid string.
                ::std::str::from_utf8(digits).unwrap().parse()
                    .map_err(|_| NumberError::Overflow)
            }
        }
    }
}

impl_from_digits!(i8);
impl_from_digits!(u8);
impl_from_digits!(i16);
impl_from_digits!(u16);
impl_from_digits!(i32);
impl_from_digits!(u32);
impl_from_digits!(i64);
impl_from_digits!(u64);
impl_from_digits!(isize);
impl_from_digits!(usize);

#[cfg(test)]
mod test {
    use super::{NumberParser, NumberError, FromDigits};

    pub fn parse_bytes<T>(buf: &[u8]) -> Result<T, NumberError> where T: FromDigits {
        let mut p = NumberParser::new();
        for byte in buf.iter() {
            p.push_byte(*byte);
        }
        p.end()
    }

    #[test]
    fn test_u8() {
        type R<T> = Result<T, NumberError>;
        let res_table: &[(&'static str, R<u8>)] = &[
            ("-2e", Err(NumberError::InvalidByte)),
            ("-1e", Err(NumberError::InvalidByte)),
            ("0e", Ok(0)),
            ("1e", Ok(1)),
            ("2e", Ok(2)),
            ("255e", Ok(255)),
            ("256e", Err(NumberError::Overflow)),

            ("01e", Err(NumberError::LeadingZero)),
            ("10ed", Err(NumberError::Trailing)),
            ("-0e", Err(NumberError::NegativeZero)),
            ("111$", Err(NumberError::InvalidByte)),
        ];

        for &(ref ser, ref expect) in res_table.iter() {
            let got = parse_bytes(ser.as_bytes());
            assert_eq!(&got, expect);
        }  
    }

    #[test]
    fn test_i64() {
        type R<T> = Result<T, NumberError>;
        let res_table: &[(&'static str, R<i64>)] = &[
            ("-2e", Ok(-2)),
            ("-1e", Ok(-1)),
            ("0e", Ok(0)),
            ("1e", Ok(1)),
            ("2e", Ok(2)),

            ("-9223372036854775809e", Err(NumberError::Overflow)),
            ("-9223372036854775808e", Ok(-9223372036854775808)),
            ("-9223372036854775807e", Ok(-9223372036854775807)),
            ("9223372036854775806e", Ok(9223372036854775806)),
            ("9223372036854775807e", Ok(9223372036854775807)),
            ("9223372036854775808e", Err(NumberError::Overflow)),

            ("01e", Err(NumberError::LeadingZero)),
            ("10ed", Err(NumberError::Trailing)),
            ("-0e", Err(NumberError::NegativeZero)),
            ("111$", Err(NumberError::InvalidByte)),
        ];

        for &(ref ser, ref expect) in res_table.iter() {
            let got = parse_bytes(ser.as_bytes());
            assert_eq!(&got, expect);
        }  
    }
}