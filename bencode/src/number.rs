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
    Final,
    Error(NumberError),
}

#[derive(Clone)]
pub struct NumberParser {
    value: i64,
    negate: bool,
    state: NumberState,
    stop: u8,
}

impl NumberParser {
    pub fn parse_bytes(buf: &[u8]) -> Result<i64, NumberError> {
        let mut p = NumberParser::new();
        for byte in buf.iter() {
            p.push_byte(*byte);
        }
        p.end()
    }

    pub fn with_stop(stop: u8) -> NumberParser {
        NumberParser {
            value: 0,
            negate: false,
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
                    self.negate = true;
                    Negation
                },
                b'0' => Zero,
                byte if is_digit(byte) => {
                    self.value = (byte as i64) - ('0' as i64);
                    Digit
                },
                _ => Error(NumberError::InvalidByte)
            },

            Negation => match val {
                b'0' => Error(NumberError::NegativeZero),
                byte if is_digit(byte) => {
                    self.value = ('0' as i64) - (byte as i64);
                    Digit
                },
                _ => Error(NumberError::InvalidByte)
            },

            Zero => match val {
                byte if byte == self.stop => {
                    assert_eq!(self.value, 0);
                    Final
                },
                byte if is_digit(byte) => Error(NumberError::LeadingZero),
                _ => Error(NumberError::InvalidByte)
            },

            Digit => match val {
                byte if byte == self.stop => Final,
                byte if is_digit(byte) => {
                    let mut digit = (byte as i64) - ('0' as i64);
                    if self.negate {
                        digit = -digit;
                    }
                    let new_val = self.value
                        .checked_mul(10)
                        .and_then(|val| val.checked_add(digit));
                    match new_val {
                        Some(val) => {
                            self.value = val;
                            Digit
                        },
                        None => Error(NumberError::Overflow),
                    }
                }
                _ => Error(NumberError::InvalidByte),
            },

            Final => Error(NumberError::Trailing),
            Error(err) => Error(err),
        };
        self.state == NumberState::Final
    }

    pub fn end(&self) -> Result<i64, NumberError> {
        match self.state {
            NumberState::Final => Ok(self.value),
            NumberState::Error(err) => Err(err),
            _ => Err(NumberError::Truncated),
        }
    }
}

#[test]
fn main() {
    let res_table = [
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
        let got = NumberParser::parse_bytes(ser.as_bytes());
        assert_eq!(&got, expect);
    }  
}