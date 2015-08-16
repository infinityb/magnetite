use serde;
use serde::de;
use std::str::FromStr;
use super::EnumErr;

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

impl FromStr for InfoKey {
    type Err = EnumErr;

    fn from_str(val: &str) -> Result<Self, EnumErr> {
        use self::InfoKey::*;
        match val {
            values::PATH => Ok(Path),
            values::LENGTH => Ok(Length),
            _ => Err(EnumErr::UnknownVariant),
        }   
    }   
}

impl InfoFileKey {
    pub fn as_str(&self) -> &'static str {
        use self::InfoKey::*;
        match *self {
            Path => values::PATH,
            Length => values::LENGTH,          
        }
    }
}

struct InfoFileKeyVisitor;

impl serde::de::Visitor for InfoFileKeyVisitor {
    type Value = InfoFileKey;

    fn visit_str<E>(&mut self, val: &str) -> Result<InfoFileKey, E>
        where E: ::serde::de::Error,
    {   
        FromStr::from_str(val)
            .map_err(|_| de::Error::unknown_field(val))
    }   
}

struct InfoFile {
    path: Vec<String>,
    length: u64,
}

struct InfoFileVisitor;

impl serde::de::Visitor for InfoFileVisitor {
    type Value = Change;

    fn visit_map<V>(&mut self, mut visitor: V) -> Result<Change, V::Error>
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
    MultiFile { files: Vec<TorrentFile> },
    SingleFile { length: u64 },
}

// Wrapping to workaround coherence rules
struct ShaBuf(Vec<Sha1>);

struct Info {
    fileinfo: FilesOrLength,
    piece_length: u64,
    pieces: ShaBuf,
    name: String,
}

struct InfoVisitor;

impl serde::de::Visitor for InfoVisitor {
    type Value = Change;

    fn visit_map<V>(&mut self, mut visitor: V) -> Result<Change, V::Error>
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
            None => try!(visitor.missing_field("length or files")),
        };
        let piece_length = match piece_length {
            Some(piece_length) => piece_length,
            None => try!(visitor.missing_field("piece_length")),
        };
        let pieces = match pieces {
            Some(pieces) => pieces,
            None => try!(visitor.missing_field("pieces")),
        };
        let name = match name {
            Some(name) => name,
            None => try!(visitor.missing_field("name")),
        };

        Ok(InfoFile {
            fileinfo: fileinfo,
            piece_length: piece_length,
            pieces: pieces,
            name: name,
        })
    }
}