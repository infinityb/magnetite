use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Instant;
use std::str::FromStr;
use std::io::Write;

use tracing::{event, Level};
use hyper::{Request, Response, Server, StatusCode};
use hyper::service::service_fn;
use hyper::body::Body;
use tokio_stream::StreamExt as TokioStreamExt;

use magnetite_common::TorrentId;
use dht::TorrentIdPrefix;

use crate::{DhtContext, search};
use crate::search::SearchEvent;

async fn handle_show_tracked(
    context: DhtContext,
    _addr: SocketAddr,
    _req: Request<Body>,
) -> Result<Response<Body>, hyper::http::Error> {
    let bm_locked = context.bm.borrow();
    let buf = bm_locked.tracker.get_torrent_peers_display(&Instant::now());
    drop(bm_locked);

    let builder = Response::builder()
            .header("Content-Type", "text/plain; charset=utf-8")
            .status(StatusCode::OK);

    return builder.body(Body::from(buf));
}

async fn handle_search(
    context: DhtContext,
    _addr: SocketAddr,
    _req: Request<Body>,
    target: TorrentId,
    search_kind: search::SearchKind,
) -> Result<Response<Body>, hyper::http::Error> {
    let mut search = match search::start_search(context, target, search_kind, 32) {
        Ok(s) => tokio_stream::wrappers::ReceiverStream::new(s),
        Err(err) => {
            if let Some(uerr) = err.downcast_ref::<search::Unavailable>() {
                let builder = Response::builder()
                    .header("Content-Type", "text/plain; charset=utf-8")
                    .status(StatusCode::SERVICE_UNAVAILABLE);

                let errmsg = format!("Service Unavailable - {}", uerr);
                return builder.body(Body::from(errmsg));
            }

            let builder = Response::builder()
                .header("Content-Type", "text/plain; charset=utf-8")
                .status(StatusCode::INTERNAL_SERVER_ERROR);

            let errmsg = format!("Service Unavailable - {}", err);
            return builder.body(Body::from(errmsg));
        }
    };

    let (mut sender, body) = hyper::Body::channel();
    tokio::spawn(async move {
        let mut search_eye = TorrentIdPrefix {
            base: TorrentId::zero(),
            prefix_len: 0,
        };
        while let Some(v) = TokioStreamExt::next(&mut search).await {
            let buf = match v {
                SearchEvent::Started => {
                    sender.send_data("started\n".into()).await?;
                }
                SearchEvent::StatusUpdate(ref su) => {
                    if search_eye != su.search_eye {
                        search_eye = su.search_eye;
                        let s = format!(
                            "status:\n\teye: {}/{}\n\tcandidates: {}\n\tinflight: {}\n",
                            su.search_eye.base.hex(), su.search_eye.prefix_len,
                            su.candidates,
                            su.inflight,
                        );
                        sender.send_data(s.into()).await?;
                    }
                }
                SearchEvent::FoundNodes(ref nodes) => {
                    let mut buf = Vec::new();
                    write!(&mut buf, "found {} nodes:\n", nodes.len()).unwrap();
                    for node in nodes {
                        write!(&mut buf, "\t{} {}\n", node.id.hex(), node.saddr).unwrap();
                    }
                    let s = String::from_utf8(buf).unwrap();
                    sender.send_data(s.into()).await?;
                }
                SearchEvent::FoundPeers(ref peers) => {
                    let mut buf = Vec::new();
                    write!(&mut buf, "added {} peers:\n", peers.len()).unwrap();
                    for peer in peers {
                        write!(&mut buf, "\t{}\n", peer).unwrap();
                    }
                    let s = String::from_utf8(buf).unwrap();
                    sender.send_data(s.into()).await?;
                }
            };

        }

        sender.send_data(" ------------ End-of-stream ------------\n".into()).await?;

        Result::<_, failure::Error>::Ok(())
    });

    let builder = Response::builder()
        .header("Content-Type", "text/plain; charset=utf-8")
        .status(StatusCode::OK);

    let resp = builder.body(body)?;
    Ok(resp)
}

async fn handle(
    context: DhtContext,
    addr: SocketAddr,
    req: Request<Body>,
) -> Result<Response<Body>, hyper::http::Error> {
    let uri = req.uri().path();
    event!(Level::INFO, uri=?uri, "requested");
    if uri.starts_with("/get-peers/") {
        let uri_rest = &uri["/get-peers/".len()..];
        if let Ok(tid) = TorrentId::from_str(uri_rest) {
            event!(Level::INFO, route_handler="get-peers", target=?tid, "requested");
            return handle_search(context, addr, req, tid, search::SearchKind::GetPeers).await;
        }
    }

    if uri.starts_with("/find-nodes/") {
        let uri_rest = &uri["/find-nodes/".len()..];
        if let Ok(tid) = TorrentId::from_str(uri_rest) {
            event!(Level::INFO, route_handler="find-nodes", target=?tid, "requested");
            return handle_search(context, addr, req, tid, search::SearchKind::FindNode).await;
        }
    }
    if uri == "/show-tracked" {
        return handle_show_tracked(context, addr, req).await;
    }

    if uri == "/show-buckets" {
        let bm_locked = context.bm.borrow();
        let formatted = format!("self-id: {}\n{}",
            bm_locked.self_peer_id.hex(),
            bm_locked.format_buckets());
        drop(bm_locked);

        let builder = Response::builder()
            .header("Content-Type", "text/plain; charset=utf-8")
            .status(StatusCode::OK);

        let resp = builder.body(Body::from(formatted))?;
        return Ok(resp);
    }

    let builder = Response::builder()
        .header("Content-Type", "text/plain; charset=utf-8")
        .status(StatusCode::NOT_FOUND);

    let resp = builder.body(Body::from("Not Found"))?;
    Ok(resp)
}

pub async fn start_service(context: DhtContext) -> Result<(), failure::Error> {
    let make_service = make_service_fn(move |conn: &AddrStream| {
        let context = context.clone();
        let addr = conn.remote_addr();
        let service = service_fn(move |req| {
            handle(context.clone(), addr, req)
        });
        async move { Ok::<_, Infallible>(service) }
    });

    let addr = SocketAddr::from(([127, 0, 0, 1], 3001));
    let server = Server::bind(&addr).serve(make_service);
    server.await?;
    Ok(())
}
