use std::any::Any;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use futures::future::{Future, FutureExt};
use tokio::sync::broadcast;
use tracing::{event, Level};

use magnetite_common::TorrentId;

use crate::control::api::TorrentEntry;
use crate::control::messages::{BusAddTorrent, BusListTorrents, BusRemoveTorrent};
use crate::model::TorrentMetaWrapped;
use crate::{BusMessage, CommonInit};

pub type State = BTreeMap<TorrentId, ContentInfo>;

#[derive(Clone)]
pub struct BusNewState {
    pub state: Arc<State>,
}

#[derive(Clone, Debug)]
pub struct ContentInfo {
    pub name: String,
    pub total_length: u64,
    pub piece_length: u32,
    pub piece_shas: Arc<[TorrentId]>,
}

impl ContentInfo {
    fn from_torrent_meta_wrapped(w: &TorrentMetaWrapped) -> ContentInfo {
        ContentInfo {
            name: w.meta.info.name.clone(),
            total_length: w.total_length,
            piece_length: w.meta.info.piece_length,
            piece_shas: w.piece_shas.clone().into_boxed_slice().into(),
        }
    }
}

pub fn state_holder_system(
    common: CommonInit,
) -> Pin<Box<dyn Future<Output = Result<(), failure::Error>> + Send + 'static>> {
    let CommonInit {
        ebus,
        init_sig,
        mut term_sig,
    } = common;

    let mut ebus_incoming = ebus.subscribe();
    drop(init_sig); // we've finished initializing.
    let mut state: State = Default::default();

    async move {
        loop {
            tokio::select! {
                _ = &mut term_sig => {
                    drop(ebus_incoming);
                    break;
                }
                control_req = ebus_incoming.recv() => {
                    let creq = match control_req {
                        Ok(v) => v,
                        Err(broadcast::RecvError::Closed) => {
                            break;
                        }
                        Err(broadcast::RecvError::Lagged(..)) => {
                            event!(Level::ERROR, "piece fetch loop lagged - we're dropping requests");
                            continue;
                        },
                    };

                    let mut emit_state = false;
                    if let Some(at) = creq.downcast_ref::<BusAddTorrent>() {
                        let ci = ContentInfo::from_torrent_meta_wrapped(&at.torrent);
                        state.insert(at.torrent.info_hash, ci);
                        emit_state = true;
                    }

                    if let Some(rt) = creq.downcast_ref::<BusRemoveTorrent>() {
                        state.remove(&rt.info_hash);
                        emit_state = true;
                    }

                    if emit_state {
                        let s = BusNewState { state: Arc::new(state.clone()) };
                        let s: Box<dyn Any + Send + Sync> = Box::new(s);
                        let s: BusMessage = s.into();

                        if let Err(..) = ebus.send(s) {
                            event!(Level::ERROR, "send error");
                            break;
                        }
                    }

                    if let Some(lt) = creq.downcast_ref::<BusListTorrents>() {
                        let mut lt: BusListTorrents = lt.clone();
                        let mut out = Vec::new();
                        for (k, v) in &state {
                            out.push(TorrentEntry {
                                info_hash: k.to_vec(),
                                name: v.name.clone(),
                                size_bytes: v.total_length,
                            });
                        }
                        tokio::task::spawn(async move {
                            let _ = lt.response.send(out).await;
                        });
                    }
                }
            };
        }

        event!(Level::INFO, "state holder system has shut down");

        Ok(())
    }.boxed()
}
