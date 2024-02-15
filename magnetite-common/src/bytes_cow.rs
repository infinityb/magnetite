use bytes::Bytes;

#[derive(Debug)]
pub enum BytesCow<'a> {
    Owned(Bytes),
    Borrowed(&'a [u8]),
}

impl<'a> BytesCow<'a> {
    pub fn into_owned(self) -> BytesCow<'static> {
        match self {
            BytesCow::Owned(s) => BytesCow::Owned(s),
            BytesCow::Borrowed(b) => BytesCow::Owned(Bytes::copy_from_slice(b)),
        }
    }

    pub fn as_slice<'z>(&'z self) -> &'z [u8]
    where
        'a: 'z,
    {
        match *self {
            BytesCow::Owned(ref by) => by,
            BytesCow::Borrowed(by) => by,
        }
    }
}

impl From<Vec<u8>> for BytesCow<'static> {
    fn from(e: Vec<u8>) -> BytesCow<'static> {
        BytesCow::Owned(e.into())
    }
}
