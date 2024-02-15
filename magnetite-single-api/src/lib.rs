pub use futures;
pub use tokio;
pub use tokio_stream;
pub use tonic;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/magnetite.single.rs"));
}

#[derive(Debug)]
pub struct TorrentSessionStartItem {
    pub item: crate::proto::torrent_session_start_item::Item,
}

impl TryFrom<crate::proto::TorrentSessionStartItem> for TorrentSessionStartItem {
    type Error = tonic::Status;

    fn try_from(i: crate::proto::TorrentSessionStartItem) -> Result<Self, tonic::Status> {
        let item = i.item.ok_or_else(|| tonic::Status::invalid_argument("item is required"))?;
        Ok(TorrentSessionStartItem {
            item,
        })
    }
}

impl From<TorrentSessionStartItem> for crate::proto::TorrentSessionStartItem {
    fn from(i: TorrentSessionStartItem) -> Self {
        crate::proto::TorrentSessionStartItem {
            item: Some(i.item),
        }
    }
}

pub type TorrentSessionStartItemInit = crate::proto::TorrentSessionStartItemInit;

impl From<TorrentSessionStartItemInit> for crate::proto::TorrentSessionStartItem {
    fn from(i: TorrentSessionStartItemInit) -> Self {
        crate::proto::TorrentSessionStartItem {
            item: Some(crate::proto::torrent_session_start_item::Item::Init(i)),
        }
    }
}

// struct TorrentSessionStartItemInit {
//     pub session_id: String,
//     pub bitfield: Vec<u8>,
// }

// // impl From<TorrentSessionStartItemInit> for crate::proto::TorrentSessionStartItemInit {
// //     fn from(i: TorrentSessionStartItemInit) -> Self {
// //         crate::proto::TorrentSessionStartItemInit {
// //             session_id: Some(i.session_id),
// //             bitfield: Some(i.bitfield),
// //         }
// //     }
// // }

// // impl TryFrom<crate::proto::TorrentSessionStartItemInit> for TorrentSessionStartItemInit {
// //     type Error = tonic::Status;

// //     fn try_from(i: crate::proto::TorrentSessionStartItemInit) -> Result<Self, tonic::Status> {
// //         let session_id = i.session_id.ok_or_else(|| tonic::Status::invalid_argument("session_id is required"))?;
// //         let bitfield = i.bitfield.ok_or_else(|| tonic::Status::invalid_argument("bitfield is required"))?;
// //         Ok(TorrentSessionStartItemInit {
// //             session_id,
// //             bitfield,
// //         })
// //     }
// // }

