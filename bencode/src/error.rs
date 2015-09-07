use std::io;
use std::result;
use super::number::NumberError;
use serde::de;

#[derive(Debug)]
pub enum ErrorCode {
    UnexpectedEOF,
    ExpectedSomeValue,
    EOFWhileParsingValue,
    EOFWhileParsingInteger,
    EOFWhileParsingList,
    EOFWhileParsingDict,
    EOFWhileParsingString,
    KeyMustBeABytes,
    UnsupportedType,
    InvalidByte,
    InvalidNumber(NumberError),
    UnknownField(String),
    TrailingCharacters,
    ExcessiveAllocation,
}

#[derive(Debug)]
pub enum Error {
    SyntaxError(ErrorCode, usize, usize),
    IoError(io::Error),
    MissingFieldError(&'static str),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        Error::IoError(error)
    }
}

impl From<NumberError> for Error {
    fn from(e: NumberError) -> Error {
        Error::SyntaxError(ErrorCode::InvalidNumber(e), 0, 0)
    }
}

impl From<de::value::Error> for Error {
    fn from(error: de::value::Error) -> Error {
        match error {
            de::value::Error::SyntaxError => {
                Error::SyntaxError(ErrorCode::ExpectedSomeValue, 0, 0)
            }
            de::value::Error::EndOfStreamError => {
                de::Error::end_of_stream()
            }
            de::value::Error::UnknownFieldError(field) => {
                Error::SyntaxError(ErrorCode::UnknownField(field), 0, 0)
            }
            de::value::Error::MissingFieldError(field) => {
                de::Error::missing_field(field)
            }
        }
    }
}

impl de::Error for Error {
    fn syntax(_: &str) -> Error {
        Error::SyntaxError(ErrorCode::ExpectedSomeValue, 0, 0)
    }

    fn end_of_stream() -> Error {
        Error::SyntaxError(ErrorCode::UnexpectedEOF, 0, 0)
    }

    fn unknown_field(field: &str) -> Error {
        Error::SyntaxError(ErrorCode::UnknownField(field.to_string()), 0, 0)
    }

    fn missing_field(field: &'static str) -> Error {
        Error::MissingFieldError(field)
    }
}

/// Helper alias for `Result` objects that return a Bencode `Error`.
pub type Result<T> = result::Result<T, Error>;