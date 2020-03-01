use std::result;

pub trait TruncationError {
    fn truncated() -> Self;
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

impl<T, E1> IResult<T, E1> {
    pub fn into_result<E2>(self) -> result::Result<T, E2>
    where
        E2: From<E1> + TruncationError,
    {
        match self {
            IResult::Done(v) => Ok(v),
            IResult::ReadMore(..) => Err(TruncationError::truncated()),
            IResult::Err(e) => Err(e.into()),
        }
    }
}
