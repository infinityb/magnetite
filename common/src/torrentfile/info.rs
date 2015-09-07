use serde;
use serde::de;
use std::str::FromStr;
use super::EnumErr;
use std::path::PathBuf;
use metorrent_util::Sha1;

const CANNOT_DETERMINE_TORRENT_TYPE: &'static str = 
    "Cannot determine torrent type: both files and length";

mod values {
    pub const FILES: &'static str = "files";
    pub const LENGTH: &'static str = "length";
    pub const NAME: &'static str = "name";
    pub const PATH: &'static str = "path";
    pub const PIECE_LENGTH: &'static str = "piece length";
    pub const PIECES: &'static str = "pieces";
}

enum InfoKey {
    Files,
    Length,
    Name,
    PieceLength,
    Pieces,
}

impl FromStr for InfoKey {
    type Err = EnumErr;

    fn from_str(val: &str) -> Result<Self, EnumErr> {
        use self::InfoKey::*;
        match val {
            values::FILES => Ok(Files),
            values::LENGTH => Ok(Length),
            values::NAME => Ok(Name),
            values::PIECE_LENGTH => Ok(PieceLength),
            values::PIECES => Ok(Pieces),
            _ => Err(EnumErr::UnknownVariant),
        }   
    }   
}

impl InfoKey {
    pub fn as_str(&self) -> &'static str {
        use self::InfoKey::*;
        match *self {
            Files => values::FILES,
            Length => values::LENGTH,
            Name => values::NAME,
            PieceLength => values::PIECE_LENGTH,
            Pieces => values::PIECES,            
        }
    }
}

impl serde::de::Deserialize for InfoKey {
    fn deserialize<D>(deserializer: &mut D) -> Result<InfoKey, D::Error>
        where D: serde::Deserializer,
    {
        deserializer.visit(InfoKeyVisitor)
    }
}


struct InfoKeyVisitor;

impl serde::de::Visitor for InfoKeyVisitor {
    type Value = InfoKey;

    fn visit_str<E>(&mut self, val: &str) -> Result<InfoKey, E>
        where E: ::serde::de::Error,
    {   
        FromStr::from_str(val)
            .map_err(|_| de::Error::unknown_field(val))
    }   
}


enum InfoFileKey {
    Path,
    Length,
}

impl FromStr for InfoFileKey {
    type Err = EnumErr;

    fn from_str(val: &str) -> Result<Self, EnumErr> {
        match val {
            values::PATH => Ok(InfoFileKey::Path),
            values::LENGTH => Ok(InfoFileKey::Length),
            _ => Err(EnumErr::UnknownVariant),
        }   
    }   
}

impl InfoFileKey {
    pub fn as_str(&self) -> &'static str {
        use self::InfoFileKey::*;
        match *self {
            Path => values::PATH,
            Length => values::LENGTH,          
        }
    }
}

impl serde::de::Deserialize for InfoFileKey {
    fn deserialize<D>(deserializer: &mut D) -> Result<InfoFileKey, D::Error>
        where D: serde::Deserializer,
    {
        deserializer.visit(InfoFileKeyVisitor)
    }
}

struct InfoFileKeyVisitor;

impl serde::de::Visitor for InfoFileKeyVisitor {
    type Value = InfoFileKey;

    fn visit_str<E>(&mut self, val: &str) -> Result<InfoFileKey, E>
        where E: ::serde::de::Error,
    {   
        // FIXME: unknown field? We should have a real error here...
        FromStr::from_str(val)
            .map_err(|_| de::Error::unknown_field(val))
    }   
}

pub struct InfoFile {
    path: PathBuf,
    length: u64,
}

impl serde::de::Deserialize for InfoFile {
    fn deserialize<D>(deserializer: &mut D) -> Result<InfoFile, D::Error>
        where D: serde::Deserializer,
    {
        deserializer.visit(InfoFileVisitor)
    }
}

struct InfoFileVisitor;

impl serde::de::Visitor for InfoFileVisitor {
    type Value = InfoFile;

    fn visit_map<V>(&mut self, mut visitor: V) -> Result<InfoFile, V::Error>
        where V: serde::de::MapVisitor
    {   
        use self::InfoFileKey as Field;
        let mut path = None;
        let mut length = None;

        loop {
            match try!(visitor.visit_key()) {
                Some(Field::Path) => {
                    path = Some(try!(visitor.visit_value()));
                },
                Some(Field::Length) => {
                    length = Some(try!(visitor.visit_value()));
                }
                None => { break; }
            }
        }

        let path = match path {
            Some(path) => path,
            None => try!(visitor.missing_field("path")),
        };
        let length = match length {
            Some(length) => length,
            None => try!(visitor.missing_field("length")),
        };  
        Ok(InfoFile {
            path: path,
            length: length,
        })
    }
}

enum FilesOrLength {
    MultiFile { files: Vec<InfoFile> },
    SingleFile { length: u64 },
}

// Wrapping to workaround coherence rules
struct ShaBuf(Vec<Sha1>);

impl serde::de::Deserialize for ShaBuf {
    fn deserialize<D>(deserializer: &mut D) -> Result<ShaBuf, D::Error>
        where D: serde::Deserializer,
    {
        deserializer.visit(ShaBufVisitor)
    }
}

struct ShaBufVisitor;

impl serde::de::Visitor for ShaBufVisitor {
    type Value = ShaBuf;

    fn visit_bytes<E>(&mut self, val: &[u8]) -> Result<ShaBuf, E> where E: de::Error {
        // FIXME: E::syntax? We should have a real error here...
        match Sha1::from_bytes(val) {
            Ok(sli) => Ok(ShaBuf(sli.to_vec())),
            Err(()) => {
                Err(E::syntax("slice size not multiple of 20"))
            }
        }
    }

    fn visit_byte_buf<E>(&mut self, val: Vec<u8>) -> Result<ShaBuf, E> where E: de::Error {
        // FIXME: E::syntax? We should have a real error here...
        match Sha1::from_bytes(&val) {
            Ok(sli) => Ok(ShaBuf(sli.to_vec())),
            Err(()) => {
                Err(E::syntax("slice size not multiple of 20"))
            }
        }
    }
}


struct Info {
    fileinfo: FilesOrLength,
    piece_length: u64,
    pieces: ShaBuf,
    name: String,
}

impl serde::de::Deserialize for Info {
    fn deserialize<D>(deserializer: &mut D) -> Result<Info, D::Error>
        where D: serde::Deserializer,
    {
        deserializer.visit(InfoVisitor)
    }
}

struct InfoVisitor;

impl serde::de::Visitor for InfoVisitor {
    type Value = Info;

    fn visit_map<V>(&mut self, mut visitor: V) -> Result<Info, V::Error>
        where V: serde::de::MapVisitor
    {
        use self::InfoKey as Field;
        let mut fileinfo = None;
        let mut piece_length = None;
        let mut pieces = None;
        let mut name = None;

        loop {
            match try!(visitor.visit_key()) {
                Some(Field::Files) => {
                    if fileinfo.is_some() {
                        return Err(de::Error::syntax(
                            CANNOT_DETERMINE_TORRENT_TYPE));
                    }
                    fileinfo = Some(FilesOrLength::MultiFile {
                        files: try!(visitor.visit_value()),
                    });
                },
                Some(Field::Length) => {
                    if fileinfo.is_some() {
                        return Err(de::Error::syntax(
                            CANNOT_DETERMINE_TORRENT_TYPE));
                    }
                    fileinfo = Some(FilesOrLength::SingleFile {
                        length: try!(visitor.visit_value()),
                    });
                }
                Some(Field::Name) => {
                    name = Some(try!(visitor.visit_value()));
                },
                Some(Field::PieceLength) => {
                    piece_length = Some(try!(visitor.visit_value()));
                }
                Some(Field::Pieces) => {
                    pieces = Some(try!(visitor.visit_value()));
                },
                None => { break; }
            }
        }

        let fileinfo = match fileinfo {
            Some(fileinfo) => fileinfo,
            None => {
                let () = try!(visitor.missing_field("length or files"));
                panic!("must have errored");
            }
        };
        let piece_length = match piece_length {
            Some(piece_length) => piece_length,
            None => {
                let () = try!(visitor.missing_field("piece_length"));
                panic!("must have errored");
            }
        };
        let pieces = match pieces {
            Some(pieces) => pieces,
            None => {
                let () = try!(visitor.missing_field("pieces"));
                panic!("must have errored");
            }
        };
        let name = match name {
            Some(name) => name,
            None => {
                let () = try!(visitor.missing_field("name"));
                panic!("must have errored");
            }
        };

        Ok(Info {
            fileinfo: fileinfo,
            piece_length: piece_length,
            pieces: pieces,
            name: name,
        })
    }
}