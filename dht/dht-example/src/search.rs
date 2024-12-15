use std::collections::BTreeSet;
use std::fmt;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use failure::Fail;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use smallvec::SmallVec;
use tokio::sync::mpsc;
use tokio::task;

use dht::wire::{DhtMessageQueryFindNode, DhtMessageQueryGetPeers};
use dht::{GeneralEnvironment, RecursionState, ThinNode, TransactionCompletion};
use magnetite_common::{TorrentId, TorrentIdPrefix};

use crate::{dht_query_apply_txid, send_to_node, DhtContext};

pub enum SearchKind {
    FindNode,
    GetPeers,
}

#[derive(Debug, Fail)]
pub struct Unavailable;

impl fmt::Display for Unavailable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Unavailable")
    }
}

#[derive(Debug, Fail)]
pub struct Disconnected;

impl fmt::Display for Disconnected {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Disconnected")
    }
}

#[derive(Debug)]
pub enum SearchEvent {
    Started,
    StatusUpdate(SearchEventStatusUpdate),
    FoundNodes(Vec<ThinNode>),
    FoundPeers(Vec<SocketAddr>),
}

#[derive(Debug)]
pub struct SearchEventStatusUpdate {
    pub search_eye: TorrentIdPrefix,
    pub candidates: usize,
    pub inflight: usize,
}

async fn sender_wrapper<T>(
    s: &tokio::sync::mpsc::Sender<T>,
    item: T,
) -> Result<(), failure::Error> {
    s.send(item).await.map_err(|_e| Disconnected.into())
}

pub fn start_search(
    context: DhtContext,
    target: TorrentId,
    search_kind: SearchKind,
    recursion_node_count: usize,
) -> Result<mpsc::Receiver<SearchEvent>, failure::Error> {
    let nenv = GeneralEnvironment {
        now: Instant::now(),
    };

    let mut rs;
    let bm_locked = context.bm.borrow_mut();
    if let Some(rs_tmp) = RecursionState::new(target, &bm_locked, &nenv, recursion_node_count) {
        rs = rs_tmp;
    } else {
        return Err(Unavailable.into());
    }

    let self_peer_id = bm_locked.self_peer_id;
    for b in &bm_locked.buckets {
        for n in &b.nodes {
            rs.add_candidate(n.thin);
        }
    }
    drop(bm_locked);

    let (sender, receiver) = mpsc::channel(8);
    task::spawn_local(async move {
        let mut seen_torrent_peers = BTreeSet::new();
        let mut inflight: FuturesUnordered<_> = futures::stream::FuturesUnordered::new();
        let mut next_query = Instant::now();
        let timer = tokio::time::sleep_until(next_query.into());
        tokio::pin!(timer);

        sender_wrapper(&sender, SearchEvent::Started).await?;

        while !inflight.is_empty() || rs.has_work() {
            sender_wrapper(
                &sender,
                SearchEvent::StatusUpdate(SearchEventStatusUpdate {
                    search_eye: rs.search_eye(),
                    candidates: rs.nodes.len(),
                    inflight: inflight.len(),
                }),
            )
            .await?;

            let mut completed: Option<Box<TransactionCompletion>> = None;
            tokio::select! {
                c = inflight.next(), if !inflight.is_empty() => {
                    completed = Some(match c.unwrap() {
                        Ok(v) => v,
                        Err(..) => break,
                    });
                },
                _ = timer.as_mut(), if inflight.len() <= 3 => {
                    next_query += if inflight.is_empty() {
                        Duration::from_millis(970)
                    } else {
                        Duration::from_millis(1970)
                    };
                    timer.as_mut().reset(next_query.into());
                }
            }
            if let Some(c) = completed {
                let mut found_peers = Vec::new();
                let mut found_nodes = Vec::new();
                if let Ok(mr) = c.response {
                    for node in &mr.data.nodes {
                        let saddr = SocketAddr::V4(node.1);
                        let node = ThinNode { id: node.0, saddr };
                        if rs.add_candidate(node) {
                            found_nodes.push(node);
                        }
                    }
                    for value in &mr.data.values {
                        if !seen_torrent_peers.contains(value) {
                            seen_torrent_peers.insert(*value);
                            found_peers.push(*value);
                        }
                    }
                    if !found_peers.is_empty() {
                        sender_wrapper(&sender, SearchEvent::FoundPeers(found_peers)).await?;
                    }
                    if !found_nodes.is_empty() {
                        sender_wrapper(&sender, SearchEvent::FoundNodes(found_nodes)).await?;
                    }
                }
                continue;
            }

            let now = Instant::now();
            if let Some(wn) = rs.get_work() {
                let mut msg = match search_kind {
                    SearchKind::FindNode => Into::into(DhtMessageQueryFindNode {
                        id: self_peer_id,
                        target: target,
                        want: SmallVec::new(),
                    }),
                    SearchKind::GetPeers => Into::into(DhtMessageQueryGetPeers {
                        id: self_peer_id,
                        info_hash: target,
                    }),
                };

                let bm_locked = context.bm.borrow_mut();
                let future = dht_query_apply_txid(
                    bm_locked,
                    &context.bm,
                    &mut msg,
                    wn.saddr,
                    &now,
                    Some(wn.id),
                );
                inflight.push(future);
                send_to_node(&context.so, wn.saddr, &msg).await?;
            }
        }

        Result::<(), failure::Error>::Ok(())
    });

    Result::<_, failure::Error>::Ok(receiver)
}
