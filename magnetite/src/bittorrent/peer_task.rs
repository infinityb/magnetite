
pub fn start_peer_task(
    init: CommonInitTasklet,
    // if Some, it's an incoming connection and we have the handshake in the read
    // buffer present here.
    conn_state: Option<BytesMut>,
    peer_id: TorrentId,
    info_hash: TorrentId,
    bitfield: BitField,
) -> Pin<Box<dyn std::future::Future<Output = Result<(), MagnetiteError>> + Send + 'static>> {
    let is_incoming = conn_state.is_some();

    let mut rbuf = conn_state.unwrap_or_else(|| BytesMut::with_capacity(PEER_RBUF_SIZE_BYTES));

    let mut handshake = Handshake::zero();
    handshake.info_hash = info_hash;
    handshake.peer_id = peer_id;

    // write bitfield to wbuf
    // read bitfield from rbuf

    let mut ps = PeerState::new();
}
