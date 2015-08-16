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
    KeyMustBeAString,
    InvalidByte,
    InvalidNumber(NumberError),
    UnknownField(String),
    TrailingCharacters,
    ExcessiveAllocation,
}

#[derive(Debug)]
pub enum Error {
    SyntaxError(ErrorCode),
    IoError(io::Error),
    MissingFieldError(&'static str),
}

impl From<NumberError> for Error {
    fn from(e: NumberError) -> Error {
        Error::SyntaxError(ErrorCode::InvalidNumber(e))
    }
}

impl From<de::value::Error> for Error {
    fn from(error: de::value::Error) -> Error {
        match error {
            de::value::Error::SyntaxError => {
                Error::SyntaxError(ErrorCode::ExpectedSomeValue)
            }
            de::value::Error::EndOfStreamError => {
                de::Error::end_of_stream()
            }
            de::value::Error::UnknownFieldError(field) => {
                Error::SyntaxError(ErrorCode::UnknownField(field))
            }
            de::value::Error::MissingFieldError(field) => {
                de::Error::missing_field(field)
            }
        }
    }
}

impl de::Error for Error {
    fn syntax(_: &str) -> Error {
        Error::SyntaxError(ErrorCode::ExpectedSomeValue)
    }

    fn end_of_stream() -> Error {
        Error::SyntaxError(ErrorCode::UnexpectedEOF)
    }

    fn unknown_field(field: &str) -> Error {
        Error::SyntaxError(ErrorCode::UnknownField(field.to_string()))
    }

    fn missing_field(field: &'static str) -> Error {
        Error::MissingFieldError(field)
    }
}


pub type Result<T> = result::Result<T, Error>;