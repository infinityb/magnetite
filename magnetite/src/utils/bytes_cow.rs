use bytes::Bytes;

#[derive(Debug)]
pub enum BytesCow<'a> {
    Owned(Bytes),
    Borrowed(&'a [u8]),
}

impl<'a> BytesCow<'a> {
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
