use std::net::SocketAddr;
use std::time::Instant;

use tracing::{event, Level};

use magnetite_common::TorrentId;

use crate::model::proto::{Handshake, Message};
use crate::model::{ProtocolViolation, BitField, MagnetiteError};

#[deprecated]  // use timers in the peer task.
enum StreamReaderItem {
    Message(Message<'static>),
    AntiIdle,
}

pub struct Session {
    pub id: u64,
    pub addr: SocketAddr,
    pub handshake: Handshake,
    pub target: TorrentId,
    // Arc<TorrentMetaWrapped>,
    pub state: PeerState,
    // storage_engine: PieceFileStorageEngine,
}

#[derive(Clone)]
pub struct PeerState {
    pub last_read: Instant,
    pub next_keepalive: Instant,
    pub peer_bitfield: BitField,
    pub choking: bool,
    pub interesting: bool,
    pub choked: bool,
    pub interested: bool,
}

impl PeerState {
    pub fn new(bf_length: u32) -> PeerState {
        let now = Instant::now();
        PeerState {
            last_read: now,
            next_keepalive: now,
            peer_bitfield: BitField::none(bf_length),
            choking: true,
            interesting: false,
            choked: true,
            interested: false,
        }
    }
}


fn apply_work_state(
    ps: &mut PeerState,
    work: &StreamReaderItem,
    now: Instant,
    session_id: u64,
) -> Result<(), MagnetiteError> {
    if let StreamReaderItem::Message(..) = work {
        ps.last_read = now;
    }

    match work {
        StreamReaderItem::Message(Message::Choke) => {
            ps.choked = true;
            Ok(())
        }
        StreamReaderItem::Message(Message::Unchoke) => {
            ps.choked = false;
            Ok(())
        }
        StreamReaderItem::Message(Message::Interested) => {
            ps.interested = true;
            Ok(())
        }
        StreamReaderItem::Message(Message::Uninterested) => {
            ps.interested = false;
            Ok(())
        }
        StreamReaderItem::Message(Message::Have { piece_id }) => {
            if *piece_id < ps.peer_bitfield.bit_length {
                ps.peer_bitfield.set(*piece_id, true);
            }
            Ok(())
        }
        StreamReaderItem::Message(Message::Bitfield { ref field_data }) => {
            if field_data.as_slice().len() != ps.peer_bitfield.data.len() {
                return Err(ProtocolViolation.into());
            }
            ps.peer_bitfield.data = field_data.as_slice().to_vec().into_boxed_slice();
            let completed = ps.peer_bitfield.count_ones();
            let total_pieces = ps.peer_bitfield.bit_length;
            event!(
                Level::TRACE,
                name = "update-bitfield",
                session_id = session_id,
                completed_pieces = completed,
                total_pieces = ps.peer_bitfield.bit_length,
                "#{}: {:.2}% confirmed",
                session_id,
                100.0 * completed as f64 / total_pieces as f64
            );
            Ok(())
        }
        _ => Ok(()),
    }
}