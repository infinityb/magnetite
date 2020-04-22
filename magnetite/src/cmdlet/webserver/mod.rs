use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt::Write;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::sync::Arc;

use bytes::BytesMut;
use clap::{App, Arg, SubCommand};
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::Body;
use hyper::{Request, Response, Server, StatusCode};
use salsa20::stream_cipher::generic_array::GenericArray;
use salsa20::stream_cipher::{NewStreamCipher, SyncStreamCipher};
use salsa20::XSalsa20;
use sha1::{Digest, Sha1};
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tracing::{event, Level};

use crate::model::TorrentMeta;
use crate::model::{TorrentID, TorrentMetaWrapped};
use crate::storage::disk_cache_layer::CacheWrapper;
use crate::storage::{
    multi_piece_read, piece_file, state_wrapper, MultiPieceReadRequest, PieceFileStorageEngine,
    PieceStorageEngineDumb, ShaVerify, ShaVerifyMode, StateWrapper,
};
use crate::vfs::{
    Directory, DirectoryChild, FileData, FileEntry, FileEntryData, NoEntityExists, NotADirectory,
    Vfs, FileType, FilesystemImpl, FilesystemImplMutable,
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
    use crate::model::config::LegacyConfig as Config;

    let config = matches.value_of("config").unwrap();
    let mut cfg_fi = File::open(&config).unwrap();
    let mut cfg_by = Vec::new();
    cfg_fi.read_to_end(&mut cfg_by).unwrap();
    let config: Config = toml::de::from_slice(&cfg_by).unwrap();

    let bind_address = matches.value_of_os("bind-address").unwrap();

    let mut rt = Runtime::new()?;
    let mut state_builder = StateWrapper::builder();
    let mut pf_builder = PieceFileStorageEngine::builder();

    let mut torrents = Vec::new();
    for s in &config.torrents {
        let mut fi = File::open(&s.torrent_file).unwrap();
        let mut by = Vec::new();
        fi.read_to_end(&mut by).unwrap();

        let torrent = TorrentMetaWrapped::from_bytes(&by).unwrap();
        let tm = Arc::new(torrent);

        let piece_count = tm.piece_shas.len() as u32;

        // let pf = File::open(&s.source_file).unwrap();

        let mut crypto = None;
        if let Some(salsa) = get_torrent_salsa(&s.secret, &tm.info_hash) {
            crypto = Some(salsa);
        }

        // pf_builder.register_info_hash(
        //     &tm.info_hash,
        //     piece_file::Registration {
        //         piece_count: piece_count,
        //         crypto: crypto,
        //         piece_file: pf.into(),
        //     },
        // );

        state_builder.register_info_hash(
            &tm.info_hash,
            state_wrapper::Registration {
                total_length: tm.total_length,
                piece_length: tm.meta.info.piece_length,
                piece_shas: tm.piece_shas.clone().into(),
            },
        );

        println!("added info_hash: {:?}", tm.info_hash);
        torrents.push(tm);
    }

    let mut cache = CacheWrapper::build_with_capacity_bytes(3 * 1024 * 1024 * 1024);

    let zero_iv = TorrentID::zero();
    use crate::cmdlet::seed::get_torrent_salsa;
    if let Some(salsa) = get_torrent_salsa(&config.cache_secret, &zero_iv) {
        cache.set_crypto(salsa);
    }

    use std::fs::OpenOptions;

    let cache_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open("magnetite.cache")
        .unwrap();

    // let storage_engine = pf_builder.build();
    let storage_engine = rt.block_on(async {
        use crate::storage::remote_magnetite::RemoteMagnetite;

        RemoteMagnetite::connected("localhost:17862")
    });
    let storage_engine = cache.build(cache_file.into(), storage_engine);
    // let storage_engine = ShaVerify::new(storage_engine, ShaVerifyMode::Always);

    let mut fs_impl = FilesystemImplMutable {
        storage_backend: storage_engine,
        content_info: state_builder.build_content_info_manager(),
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
    for t in &torrents {
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

    async fn service_request<P>(
        fs: FilesystemImpl<P>,
        remote_addr: std::net::SocketAddr,
        req: Request<Body>,
    ) -> Response<Body>
    where
        P: PieceStorageEngineDumb + Clone + Send + Sync + 'static,
    {
        println!("{:?}", req.uri().path());

        let fs = fs.mutable.lock().await;

        let path = req.uri().path().split("/").filter(|x| x.len() != 0);
        let fe = match fs.vfs.traverse_path(1, path) {
            Ok(fe) => fe,
            Err(err) => {
                if err.downcast_ref::<NoEntityExists>().is_some() {
                    return Response::new(Body::from(format!("fake 404: {}", err)));
                }

                return Response::new(Body::from(format!("fake 503: {}", err)));
            }
        };

        match fe.data {
            FileEntryData::Dir(ref dir) => {
                if !req.uri().path().ends_with("/") {
                    let mut builder = Response::builder()
                        .header("Location", format!("{}/", req.uri().path()))
                        .header("Server", SERVER_NAME)
                        .status(StatusCode::FOUND);

                    return builder.body(Body::from(vec![])).unwrap();
                }
                let mut data = BytesMut::new();
                for d in &dir.child_inodes {
                    if let Some(ds) = d.file_name.to_str() {
                        let mut file_type_str = "???";
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

                let mut resp = Response::new(data[..].to_vec().into());
                resp.headers_mut().insert(
                    hyper::header::CONTENT_TYPE,
                    hyper::header::HeaderValue::from_static("text/html"),
                );
                resp.headers_mut().insert(
                    hyper::header::SERVER,
                    hyper::header::HeaderValue::from_static(SERVER_NAME),
                );
                resp
            }
            FileEntryData::File(ref tf) => {
                let info = fs.content_info.get_content_info(&tf.content_key).unwrap();
                let req = MultiPieceReadRequest {
                    content_key: tf.content_key,
                    piece_shas: &info.piece_shas[..],
                    piece_length: info.piece_length,
                    total_length: info.total_length,

                    file_offset: 0,
                    read_length: fe.size as usize,
                    torrent_global_offset: tf.torrent_global_offset,
                };

                let bytes = match multi_piece_read(&fs.storage_backend, &req).await {
                    Ok(by) => by,
                    Err(err) => {
                        event!(Level::ERROR, "failed to fetch piece: {}", err);
                        return Response::new(Body::from(format!("fake 503: {}", err)));
                    }
                };
                drop(fs);

                let mut resp = Response::new(bytes[..].to_vec().into());
                resp.headers_mut().insert(
                    hyper::header::CONTENT_TYPE,
                    hyper::header::HeaderValue::from_static("image/jpeg"),
                );
                resp.headers_mut().insert(
                    hyper::header::SERVER,
                    hyper::header::HeaderValue::from_static(SERVER_NAME),
                );
                resp
            }
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
            let remote_addr = remote_addr.clone();

            let service = service_fn(move |req: Request<Body>| {
                let fs_impl = fs_impl.clone();
                let remote_addr = remote_addr.clone();

                async move {
                    let v = service_request(fs_impl, remote_addr, req).await;
                    Ok::<_, Infallible>(v)
                }
            });

            Ok::<_, Infallible>(service)
        }
    });

    rt.block_on(async {
        // Then bind and serve...
        let addr = ("::".parse::<std::net::Ipv6Addr>().unwrap(), 8081).into();
        let server = Server::bind(&addr).serve(make_svc);

        // Finally, spawn `server` onto an Executor...
        if let Err(e) = server.await {
            eprintln!("server error: {}", e);
        }
    });

    Ok(())
}
