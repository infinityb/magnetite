use std::ffi::OsString;

use serde;
use sha1;
use time::Timespec;
use url::Url;

use bencode;
use metorrent_util::Sha1;

struct TorrentFile {
    path: Vec<String>,
    length: u64,
}

enum FilesOrLength {
    MultiFile { files: Vec<TorrentFile> },
    SingleFile { length: u64 },
}

struct InfoTmp {
    fileinfo: FilesOrLength,
    piece_length: u64,
    pieces: Vec<Sha1>,
    name: String,
}

impl serde::de::Deserialize for InfoTmp {
    fn deserialize<D>(deserializer: &mut D) -> Result<Event, D::Error>
        where D: serde::Deserializer,
    {
        //
    }
}

struct TorrentInfo {
    info_hash: Sha1,
    fileinfo: FilesOrLength,
    piece_length: u64,
    pieces: Vec<Sha1>,
    name: String,
}

impl TorrentInfo {
    fn from_tmp(info_hash: Sha1, info_tmp: InfoTmp) -> Self {
        TorrentInfo {
            info_hash: info_hash,
            fileinfo: info_tmp.fileinfo,
            piece_length: info_tmp.piece_length,
            name: info_tmp.name,
            pieces: info_tmp.pieces,
        }
    }

    pub fn from_buffer(buf: &[u8]) -> Result<Self, &'static str> {
        let info_res = bencode::from_slice(buf)
            .map_err(|_| "Decode failure");

        let info: InfoTmp = try!(info_res);

        let mut hasher = sha1::Sha1::new();
        hasher.update(buf);

        let mut info_hash = Sha1::zero();
        hasher.output(info_hash.as_bytes_mut());

        Ok(TorrentInfo::from_tmp(info_hash, info))

    }

    pub fn get_pieces(&self) -> &[Sha1] {
        &self.pieces
    }

    pub fn is_multifile(&self) -> bool {
        self.files.is_some()
    }
}

pub struct Torrent {
    announce: Option<Url>,
    encoding: Option<String>,
    announce_list: Vec<Vec<Url>>,
    creation_date: Timespec,
    info: TorrentInfo,
}

impl Torrent {
    pub fn is_multifile(&self) -> bool {
        self.info.is_multifile()
    }
}