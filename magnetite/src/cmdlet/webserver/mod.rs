use std::collections::BTreeMap;
use std::convert::Infallible;
use std::ffi::OsString;
use std::fmt::{self, Write};
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use clap::{App, Arg, SubCommand};
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::Body;
use hyper::{Request, Response, Server, StatusCode};
use rand::seq::SliceRandom;
use rand::thread_rng;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tracing::{event, Level};

use crate::model::config::build_storage_engine_states;
use crate::model::{InternalError, TorrentID};
use crate::storage::PieceStorageEngineDumb;
use crate::vfs::{
    Directory, FileEntry, FileEntryData, FileType, FilesystemImpl, FilesystemImplMutable,
    NoEntityExists, Vfs,
};
use crate::CARGO_PKG_VERSION;

pub const SERVER_NAME: &str = "Magnetite Demonstration Server";
pub const SUBCOMMAND_NAME: &str = "webserver";

pub fn get_subcommand() -> App<'static, 'static> {
    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("A demonstration webserver")
        .arg(
            Arg::with_name("config")
                .long("config")
                .value_name("FILE")
                .help("config file")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("bind-address")
                .long("bind-address")
                .value_name("[ADDRESS]")
                .help("The address to bind to")
                .required(true)
                .takes_value(true),
        )
}

pub fn main(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    use crate::model::config::Config;

    let config = matches.value_of("config").unwrap();
    let mut cfg_fi = File::open(&config).unwrap();
    let mut cfg_by = Vec::new();
    cfg_fi.read_to_end(&mut cfg_by).unwrap();
    let config: Config = toml::de::from_slice(&cfg_by).unwrap();

    let bind_address = matches.value_of("bind-address").unwrap().to_string();

    let mut rt = Runtime::new()?;

    let states = build_storage_engine_states(&mut rt, &config).unwrap();

    let mut fs_impl = FilesystemImplMutable {
        storage_backend: states.storage_engine,
        content_info: states.content_info_manager,
        vfs: Vfs {
            inodes: BTreeMap::new(),
            inode_seq: 3,
        },
    };
    fs_impl.vfs.inodes.insert(
        1,
        FileEntry {
            info_hash_owner_counter: 1,
            inode: 1,
            size: 0,
            data: FileEntryData::Dir(Directory {
                parent: 1,
                child_inodes: Default::default(),
            }),
        },
    );
    for t in states.path_to_torrent.values() {
        if let Err(err) = fs_impl.add_torrent(t) {
            event!(
                Level::ERROR,
                "{}:{}: failed to add torrent: {}",
                file!(),
                line!(),
                err
            );
        }
    }
    event!(
        Level::INFO,
        "mounting with {} known inodes",
        fs_impl.vfs.inodes.len()
    );

    let fs_impl = FilesystemImpl {
        mutable: Arc::new(Mutex::new(fs_impl)),
    };

    let make_svc = make_service_fn(move |socket: &AddrStream| {
        let fs_impl = fs_impl.clone();
        let remote_addr = socket.remote_addr();

        async move {
            let fs_impl = fs_impl.clone();
            let service = service_fn(move |req: Request<Body>| {
                let fs_impl = fs_impl.clone();
                async move {
                    let v = service_request(fs_impl, remote_addr, req).await;
                    Ok::<_, Infallible>(v)
                }
            });

            Ok::<_, Infallible>(service)
        }
    });

    rt.block_on(async {
        let sa: SocketAddr = bind_address.parse().unwrap();
        let server = Server::bind(&sa).serve(make_svc);
        event!(Level::INFO, "binding to {}", bind_address);

        // Finally, spawn `server` onto an Executor...
        if let Err(e) = server.await {
            eprintln!("server error: {}", e);
        }
    });

    Ok(())
}

async fn service_request<P>(
    fsi: FilesystemImpl<P>,
    _remote_addr: std::net::SocketAddr,
    req: Request<Body>,
) -> Response<Body>
where
    P: PieceStorageEngineDumb + Unpin + Clone + Send + Sync + 'static,
{
    match service_request_helper(fsi, req).await {
        Ok(resp) => resp,
        Err(err) => {
            if err.downcast_ref::<NoEntityExists>().is_some() {
                return response_http_not_found();
            }

            if err.downcast_ref::<OutOfRange>().is_some() {
                return repsonse_http_range_not_satisfiable();
            }

            if err.downcast_ref::<ClientError>().is_some() {
                event!(Level::ERROR, "bad request: {}", err);
                return response_http_bad_request();
            }

            if err.downcast_ref::<InternalError>().is_some() {
                event!(Level::ERROR, "explicit ISE: {}", err);
                return response_http_internal_server_error();
            }

            response_http_internal_server_error()
        }
    }
}

#[cfg(any(unix, target_os = "redox"))]
fn percent_decode_str(x: &str) -> OsString {
    use std::borrow::Cow;
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    use percent_encoding::percent_decode_str;

    let bytes: Cow<[u8]> = percent_decode_str(x).into();
    OsStr::from_bytes(&bytes[..]).into()
}

#[cfg(windows)]
fn percent_decode_str(x: &str) -> OsString {
    use std::borrow::Cow;
    use std::os::windows::ffi::OsStrExt;

    use percent_encoding::percent_decode_str;

    percent_decode_str(x).decode_utf8_lossy().into()
}

async fn service_request_helper<P>(
    fsi: FilesystemImpl<P>,
    req: Request<Body>,
) -> Result<Response<Body>, failure::Error>
where
    P: PieceStorageEngineDumb + Clone + Send + Sync + 'static,
{
    use crate::storage::utils::compute_piece_index_lb;
    use crate::storage::GetPieceRequest;

    let path = req
        .uri()
        .path()
        .split('/')
        .filter(|x| !x.is_empty())
        .map(percent_decode_str);

    let fs = fsi.mutable.lock().await;
    let storage_backend = fs.storage_backend.clone();
    event!(
        Level::DEBUG,
        "HTTP access {:?}",
        path.clone().collect::<Vec<_>>()
    );

    let fe = fs.vfs.traverse_path(1, path).map(Clone::clone)?;
    drop(fs);

    let content_key;
    let torrent_global_offset_start;
    match fe.data {
        FileEntryData::Dir(ref dir) => {
            if !req.uri().path().ends_with('/') {
                let uri = format!("{}/", req.uri().path());
                return Ok(response_http_found(&uri));
            }
            return Ok(response_ok_rendering_directory(&dir));
        }
        FileEntryData::File(ref file) => {
            content_key = file.content_key;
            torrent_global_offset_start = file.torrent_global_offset;
        }
    };

    let fs = fsi.mutable.lock().await;
    let content_info = fs
        .content_info
        .get_content_info(&content_key)
        .ok_or(InternalError {
            msg: "unknown content key",
        })?;
    drop(fs);

    const BOUNDARY_LENGTH: usize = 60;
    // const BOUNDARY_CHARS: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    const BOUNDARY_CHARS: &[u8] = b"0123456789abcdef";

    let mut builder = Response::builder()
        .header(hyper::header::SERVER, SERVER_NAME)
        .header(hyper::header::ACCEPT_RANGES, "bytes")
        .header(
            hyper::header::LAST_MODIFIED,
            "Wed, 22 Apr 2020 02:08:01 GMT",
        );

    let mut spans: Vec<HttpRangeSpan> = Vec::new();

    let mut boundary = None;
    let mut status_code = StatusCode::OK;

    if let Some(range_data) = req.headers().get(hyper::header::RANGE) {
        status_code = StatusCode::PARTIAL_CONTENT;

        let mut range_count = 0;
        for range_atom in get_ranges(range_data, fe.size)? {
            range_count += 1;
            spans.push(range_atom);
        }

        http_span_check_no_overlaps(&spans[..])?;

        if range_count == 0 {
            return Err(ClientError.into());
        } else if range_count > 1 {
            let mut boundary_tmp = [0u8; BOUNDARY_LENGTH];

            let mut rng = thread_rng();
            for v in boundary_tmp.iter_mut() {
                *v = *BOUNDARY_CHARS.choose(&mut rng).unwrap();
            }

            let boundary_string = std::str::from_utf8(&boundary_tmp[..]).unwrap().to_string();
            boundary = Some(boundary_string);
        } else {
            let onespan = &spans[0];
            builder = builder.header(
                hyper::header::CONTENT_RANGE,
                format!(
                    "bytes {}-{}/{}",
                    onespan.start,
                    onespan.start + onespan.length - 1,
                    fe.size
                ),
            );
        }
    } else {
        spans.push(HttpRangeSpan {
            start: 0,
            length: fe.size,
        });
    }
    if spans.len() == 1 {
        builder = builder.header(
            hyper::header::CONTENT_LENGTH,
            format!("{}", spans[0].length),
        );
    }

    let file_mime_type = if req.uri().path().ends_with(".jpg") {
        "image/jpeg"
    } else if req.uri().path().ends_with(".png") {
        "image/png"
    } else if req.uri().path().ends_with(".gif") {
        "image/gif"
    } else if req.uri().path().ends_with(".mp4") {
        "video/mp4"
    } else if req.uri().path().ends_with(".cue") {
        "text/plain; charset=utf-8"
    } else {
        "application/octet-stream"
    };

    if let Some(ref b) = boundary {
        builder = builder.header(
            hyper::header::CONTENT_TYPE,
            format!("multipart/byteranges; boundary={}", b),
        );
    } else {
        builder = builder.header(hyper::header::CONTENT_TYPE, file_mime_type);
    }

    event!(
        Level::DEBUG,
        "fetching spans: {:?} -- {:#?}",
        spans,
        req.headers()
    );

    // let mut total_builder = BytesMut::new();
    let (mut tx, rx) = tokio::sync::mpsc::channel(2);
    tokio::spawn(async move {
        let mut is_first_boundary = true;
        let mut prefix = BytesMut::new();
        for sp in spans.iter() {
            let piece_length = content_info.piece_length;

            let global_span_start = torrent_global_offset_start + sp.start;
            let piece_span_start = global_span_start % content_info.piece_length as u64;
            let piece_index_cur = compute_piece_index_lb(global_span_start, piece_length);
            if let Some(ref b) = boundary {
                if is_first_boundary {
                    is_first_boundary = false;
                } else {
                    write!(&mut prefix, "\r\n").unwrap();
                }
                write!(&mut prefix, "--{}\r\n", b).unwrap();
                write!(
                    &mut prefix,
                    "Content-Range: bytes {}-{}/{}\r\n",
                    sp.start,
                    sp.full_closed_end(),
                    fe.size
                )
                .unwrap();
                write!(&mut prefix, "Content-Type: {}\r\n", file_mime_type).unwrap();
                write!(&mut prefix, "\r\n").unwrap();

                // total_builder.extend_from_slice(&prefix[..]);
                // prefix.clear();
                if let Err(err) = tx.send(Ok(prefix.split().freeze())).await {
                    event!(
                        Level::ERROR,
                        "failed to send data to response handler: {}",
                        err
                    );
                    return;
                }
            }

            let mut piece_span_start = piece_span_start;
            let mut to_send: u64 = sp.length;
            for p in piece_index_cur.. {
                if to_send == 0 {
                    break;
                }
                let piece_sha: TorrentID = match content_info.piece_shas.get(p as usize) {
                    Some(v) => *v,
                    None => {
                        event!(
                            Level::ERROR,
                            "failed to send error to response handler: bad piece index {}",
                            p,
                        );
                        if let Err(err) = tx.send(Err("bad piece index".to_string())).await {
                            event!(
                                Level::ERROR,
                                "failed to send error to response handler: bad piece index {}: {}",
                                p,
                                err,
                            );
                        }
                        return;
                        // return Err(InternalError { msg: "bad piece" }.into());
                    }
                };

                let req = GetPieceRequest {
                    content_key,
                    piece_sha,
                    piece_length: content_info.piece_length,
                    total_length: content_info.total_length,
                    piece_index: p,
                };

                match storage_backend.get_piece_dumb(&req).await {
                    Ok(mut by) => {
                        if piece_span_start != 0 {
                            drop(by.split_to(piece_span_start as usize));
                            piece_span_start = 0;
                        }

                        let bytes_length = by.len() as u64;
                        if to_send < bytes_length {
                            by.truncate(to_send as usize);
                        }
                        to_send -= by.len() as u64;

                        // total_builder.extend_from_slice(&by[..]);
                        if let Err(err) = tx.send(Ok(by)).await {
                            event!(
                                Level::ERROR,
                                "failed to send bytes to response handler: {}",
                                err
                            );
                            return;
                        }
                    }
                    Err(err) => {
                        if let Err(err2) = tx.send(Err(format!("{}", err))).await {
                            event!(
                                Level::ERROR,
                                "failed to send error to response handler: {}: {}",
                                err,
                                err2,
                            );
                        }
                        // return Err(InternalError { msg: "upstream failure" }.into());
                    }
                };
            }
        }

        if let Some(ref b) = boundary {
            write!(&mut prefix, "\r\n--{}--\r\n", b).unwrap();
            // total_builder.extend_from_slice(&prefix[..]);
            // prefix.clear();
            if let Err(err) = tx.send(Ok(prefix.split().freeze())).await {
                event!(
                    Level::ERROR,
                    "failed to send data to response handler: {}",
                    err
                );
                return;
            }
        }
    });

    // let body = total_builder[..].to_vec().into();
    // Ok(builder.status(status_code).body(body).unwrap())
    Ok(builder
        .status(status_code)
        .body(Body::wrap_stream(rx))
        .unwrap())
}

fn get_ranges(
    value: &hyper::header::HeaderValue,
    total_size: u64,
) -> Result<Vec<HttpRangeSpan>, failure::Error> {
    let value_str = value.to_str().map_err(|_| ClientError)?;
    if !value_str.starts_with("bytes=") {
        return Err(ClientError.into());
    }

    let mut out = Vec::new();
    for part in value_str[6..].split(", ") {
        let mut part_iter = part.splitn(2, '-');
        let start: u64 = part_iter.next().ok_or(ClientError)?.parse()?;

        if total_size <= start {
            return Err(OutOfRange.into());
        }
        let end_str = part_iter.next().ok_or(ClientError)?;

        let end = if end_str.is_empty() {
            total_size - 1
        } else {
            end_str.parse()?
        };
        if end <= start {
            return Err(OutOfRange.into());
        }

        out.push(HttpRangeSpan {
            start,
            length: end - start + 1,
        });
    }
    Ok(out)
}

fn http_span_check_no_overlaps(spans: &[HttpRangeSpan]) -> Result<(), failure::Error> {
    if spans.len() > 30 {
        // FIXME
        return Err(ClientError.into());
    }

    Ok(())
}

#[derive(Debug)]
struct HttpRangeSpan {
    start: u64,
    length: u64,
}

impl HttpRangeSpan {
    pub fn full_closed_end(&self) -> u64 {
        self.start + self.length - 1
    }
}

fn response_http_bad_request() -> Response<Body> {
    const PAGE_CONTENT: &str = "400 Bad Request";

    let builder = Response::builder()
        .header(hyper::header::SERVER, SERVER_NAME)
        .status(StatusCode::BAD_REQUEST);

    builder.body(PAGE_CONTENT.into()).unwrap()
}

fn repsonse_http_range_not_satisfiable() -> Response<Body> {
    const PAGE_CONTENT: &str = "416 Range Not Satisfiable";

    let builder = Response::builder()
        .header(hyper::header::SERVER, SERVER_NAME)
        .status(StatusCode::RANGE_NOT_SATISFIABLE);

    builder.body(PAGE_CONTENT.into()).unwrap()
}

fn response_http_not_found() -> Response<Body> {
    const PAGE_CONTENT: &str = "404 Not Found";

    let builder = Response::builder()
        .header(hyper::header::SERVER, SERVER_NAME)
        .status(StatusCode::NOT_FOUND);

    builder.body(PAGE_CONTENT.into()).unwrap()
}

fn response_http_internal_server_error() -> Response<Body> {
    const PAGE_CONTENT: &str = "503 Internal Server Error";

    let builder = Response::builder()
        .header(hyper::header::SERVER, SERVER_NAME)
        .status(StatusCode::INTERNAL_SERVER_ERROR);

    builder.body(PAGE_CONTENT.into()).unwrap()
}

fn response_http_found(new_path: &str) -> Response<Body> {
    let builder = Response::builder()
        .header(hyper::header::SERVER, SERVER_NAME)
        .header(hyper::header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(hyper::header::LOCATION, new_path)
        .status(StatusCode::FOUND);

    let content = format!("Redirecting you to <a href=\"{0}\">{0}</a>", new_path);

    builder.body(content.into()).unwrap()
}

fn response_ok_rendering_directory(dir: &Directory) -> Response<Body> {
    let mut data = BytesMut::new();

    for d in &dir.child_inodes {
        if let Some(ds) = d.file_name.to_str() {
            let file_type_str;
            let mut dir_trailer = "";
            match d.ft {
                FileType::RegularFile => {
                    file_type_str = "REG";
                }
                FileType::Directory => {
                    file_type_str = "DIR";
                    dir_trailer = "/";
                }
            }
            write!(
                &mut data,
                "[{}] <a href=\"{}{}\">{}{}</a><br>",
                file_type_str, ds, dir_trailer, ds, dir_trailer,
            )
            .unwrap();
        }
    }

    let builder = Response::builder()
        .header(hyper::header::SERVER, SERVER_NAME)
        .header(
            hyper::header::CONTENT_TYPE,
            hyper::header::HeaderValue::from_static("text/html; charset=utf-8"),
        );

    builder.body(data[..].to_vec().into()).unwrap()
}

// --

#[derive(Debug)]
pub struct OutOfRange;

impl fmt::Display for OutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OutOfRange")
    }
}

impl std::error::Error for OutOfRange {}

// --

#[derive(Debug)]
pub struct ClientError;

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ClientError")
    }
}

impl std::error::Error for ClientError {}
