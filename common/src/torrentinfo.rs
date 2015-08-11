use metorrent_util::Sha1;

#[derive(Copy, Clone)]
pub struct TorrentInfo {
    // The peer ID we will use for this torrent.
    pub client_id: Sha1,
    pub info_hash: Sha1,

    // The number of pieces in the torrent
    pub num_pieces: u32,

    // The size of each piece
    pub piece_len_shl: u8,
}

impl TorrentInfo {
    pub fn zero() -> TorrentInfo {
        TorrentInfo {
            client_id: Sha1::new([0; 20]),
            info_hash: Sha1::new([0; 20]),
            num_pieces: 0,
            piece_len_shl: 0,
        }
    }
}
