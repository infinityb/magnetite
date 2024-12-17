use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Instant;

use tracing::{event, Level};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use crate::DhtContext;

pub async fn webserver_system(context: DhtContext) -> anyhow::Result<()> {
    async fn hello(_: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
        Ok(Response::new(Full::new(Bytes::from("Hello, World!"))))
    }

    let addr = SocketAddr::from(([0, 0, 0, 0], 3004));

    // We create a TcpListener and bind it to 127.0.0.1:3000
    let listener = TcpListener::bind(addr).await?;

    // We start a loop to continuously accept incoming connections
    loop {
        let (stream, _) = listener.accept().await?;

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        // let io = TokioIo::new(stream);
        let io = IOTypeNotSend::new(TokioIo::new(stream));

        let context = context.clone();
        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn_local(async move {
            let context = context.clone();
            // Finally, we bind the incoming connection to our `hello` service
            if let Err(err) = http1::Builder::new()
                // `service_fn` converts our function in a `Service`
                .serve_connection(io, service_fn(|req| {
                    handle(context.clone(), req)
                }))
                .await
            {
                event!(Level::WARN, "Error serving connection: {:?}", err);
            }
        });
    }

    Ok(())
}

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::thread;
use tokio::net::TcpStream;

struct IOTypeNotSend {
    _marker: PhantomData<*const ()>,
    stream: TokioIo<TcpStream>,
}

impl IOTypeNotSend {
    fn new(stream: TokioIo<TcpStream>) -> Self {
        Self {
            _marker: PhantomData,
            stream,
        }
    }
}

impl hyper::rt::Write for IOTypeNotSend {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

impl hyper::rt::Read for IOTypeNotSend {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

async fn handle_show_tracked(
    context: DhtContext,
    // _addr: SocketAddr,
    _req: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::http::Error> {
    let bm_locked = context.bm.borrow();
    let buf = bm_locked.tracker.get_torrent_peers_display(&Instant::now());
    drop(bm_locked);

    let builder = Response::builder()
            .header("Content-Type", "text/plain; charset=utf-8")
            .status(StatusCode::OK);

    return builder.body(buf.into());
}

// async fn handle_search(
//     context: DhtContext,
//     _addr: SocketAddr,
//     _req: Request<hyper::body::Incoming>,
//     target: TorrentId,
//     search_kind: search::SearchKind,
// ) -> Result<Response<Full<Bytes>>, hyper::http::Error> {
//     let mut search = match search::start_search(context, target, search_kind, 32) {
//         Ok(s) => tokio_stream::wrappers::ReceiverStream::new(s),
//         Err(err) => {
//             if let Some(uerr) = err.downcast_ref::<search::Unavailable>() {
//                 let builder = Response::builder()
//                     .header("Content-Type", "text/plain; charset=utf-8")
//                     .status(StatusCode::SERVICE_UNAVAILABLE);

//                 let errmsg = format!("Service Unavailable - {}", uerr);
//                 return builder.body(errmsg.into());
//             }

//             let builder = Response::builder()
//                 .header("Content-Type", "text/plain; charset=utf-8")
//                 .status(StatusCode::INTERNAL_SERVER_ERROR);

//             let errmsg = format!("Service Unavailable - {}", err);
//             return builder.body(errmsg.into());
//         }
//     };

//     let (mut sender, body) = hyper::Body::channel();
//     tokio::spawn(async move {
//         let mut search_eye = TorrentIdPrefix {
//             base: TorrentId::zero(),
//             prefix_len: 0,
//         };
//         while let Some(v) = TokioStreamExt::next(&mut search).await {
//             let buf = match v {
//                 SearchEvent::Started => {
//                     sender.send_data("started\n".into()).await?;
//                 }
//                 SearchEvent::StatusUpdate(ref su) => {
//                     if search_eye != su.search_eye {
//                         search_eye = su.search_eye;
//                         let s = format!(
//                             "status:\n\teye: {}/{}\n\tcandidates: {}\n\tinflight: {}\n",
//                             su.search_eye.base.hex(), su.search_eye.prefix_len,
//                             su.candidates,
//                             su.inflight,
//                         );
//                         sender.send_data(s.into()).await?;
//                     }
//                 }
//                 SearchEvent::FoundNodes(ref nodes) => {
//                     let mut buf = Vec::new();
//                     write!(&mut buf, "found {} nodes:\n", nodes.len()).unwrap();
//                     for node in nodes {
//                         write!(&mut buf, "\t{} {}\n", node.id.hex(), node.saddr).unwrap();
//                     }
//                     let s = String::from_utf8(buf).unwrap();
//                     sender.send_data(s.into()).await?;
//                 }
//                 SearchEvent::FoundPeers(ref peers) => {
//                     let mut buf = Vec::new();
//                     write!(&mut buf, "added {} peers:\n", peers.len()).unwrap();
//                     for peer in peers {
//                         write!(&mut buf, "\t{}\n", peer).unwrap();
//                     }
//                     let s = String::from_utf8(buf).unwrap();
//                     sender.send_data(s.into()).await?;
//                 }
//             };

//         }

//         sender.send_data(" ------------ End-of-stream ------------\n".into()).await?;

//         Result::<_, failure::Error>::Ok(())
//     });

//     let builder = Response::builder()
//         .header("Content-Type", "text/plain; charset=utf-8")
//         .status(StatusCode::OK);

//     let resp = builder.body(body)?;
//     Ok(resp)
// }

async fn handle(
    context: DhtContext,
    // addr: SocketAddr,
    req: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::http::Error> {
    let uri = req.uri().path();
    event!(Level::INFO, uri=?uri, "requested");
    // if uri.starts_with("/get-peers/") {
    //     let uri_rest = &uri["/get-peers/".len()..];
    //     if let Ok(tid) = TorrentId::from_str(uri_rest) {
    //         event!(Level::INFO, route_handler="get-peers", target=?tid, "requested");
    //         return handle_search(context, addr, req, tid, search::SearchKind::GetPeers).await;
    //     }
    // }
    // if uri.starts_with("/find-nodes/") {
    //     let uri_rest = &uri["/find-nodes/".len()..];
    //     if let Ok(tid) = TorrentId::from_str(uri_rest) {
    //         event!(Level::INFO, route_handler="find-nodes", target=?tid, "requested");
    //         return handle_search(context, addr, req, tid, search::SearchKind::FindNode).await;
    //     }
    // }
    if uri == "/show-tracked" {
        return handle_show_tracked(context, req).await;
    }

    if uri == "/show-buckets" {
        let help_text = [
            "node health status:",
            "    🔥 - healthy node",
            "    ❓ - questionable node, eligible for demotion if in bucket",
            "    🧊 - bad node",
            "other indicators:",
            "    🪣 - persisted node, can be given out in responses, checked for liveness.",
            "    🦠 - expired, will be eliminated next check",
            "",
        ].join("\n");

        let bm_locked = context.bm.borrow();
        let formatted = format!("self-id: {}\n{}\n{}",
            bm_locked.self_peer_id.hex(),
            help_text,
            bm_locked.format_buckets());
        drop(bm_locked);

        let builder = Response::builder()
            .header("Content-Type", "text/plain; charset=utf-8")
            .status(StatusCode::OK);

        let resp = builder.body(formatted.into())?;
        return Ok(resp);
    }

    let builder = Response::builder()
        .header("Content-Type", "text/plain; charset=utf-8")
        .status(StatusCode::NOT_FOUND);

    let resp = builder.body("Not Found".into())?;
    Ok(resp)
}

// pub async fn start_service(context: DhtContext) -> Result<(), failure::Error> {
//     let make_service = make_service_fn(move |conn: &AddrStream| {
//         let context = context.clone();
//         let addr = conn.remote_addr();
//         let service = service_fn(move |req| {
//             handle(context.clone(), addr, req)
//         });
//         async move { Ok::<_, Infallible>(service) }
//     });

//     let addr = SocketAddr::from(([127, 0, 0, 1], 3001));
//     let server = Server::bind(&addr).serve(make_service);
//     server.await?;
//     Ok(())
// }
