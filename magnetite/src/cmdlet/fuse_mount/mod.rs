#![cfg_attr(not(unix), allow(unused_imports))]

use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use clap::{App, Arg, SubCommand};
use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request,
};
use libc::{c_int, EINVAL};

use tokio::sync::Mutex;
use tracing::{event, Level};

use crate::model::config::build_storage_engine_states;

use crate::storage::{multi_piece_read, MultiPieceReadRequest, PieceStorageEngineDumb};
use crate::vfs::{
    Directory, FileEntry, FileEntryData, FileType as VfsFileType, FilesystemImpl,
    FilesystemImplMutable, NoEntityExists, NotADirectory, Vfs,
};

mod adapter;

use self::adapter::{fuse_result_wrapper, FuseReplyAttr, FuseReplyDirectory, FuseReplyEntry};

pub mod fuse_api {
    tonic::include_proto!("magnetite.api.fuse");
}

use fuse_api::{
    magnetite_fuse_host_server::MagnetiteFuseHost, AddTorrentRequest, AddTorrentResponse,
    RemoveTorrentRequest, RemoveTorrentResponse,
};

fn file_entry_attrs(fe: &FileEntry) -> FileAttr {
    let kind;
    let perm;
    let nlink;
    match fe.data {
        FileEntryData::Dir(..) => {
            kind = FileType::Directory;
            perm = 0o555;
            nlink = 2;
        }
        FileEntryData::File(..) => {
            kind = FileType::RegularFile;
            perm = 0o444;
            nlink = 1;
        }
    }
    FileAttr {
        ino: fe.inode,
        size: fe.size,
        blocks: 0,
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind,
        perm,
        nlink,
        uid: 501,
        gid: 20,
        rdev: 0,
        flags: 0,
    }
}

const HELLO_TXT_CONTENT: &str = "Hello World!\n";

const TTL: std::time::Duration = std::time::Duration::from_secs(120);

impl<P> Filesystem for FilesystemImpl<P>
where
    P: PieceStorageEngineDumb + Clone + Send + Sync + 'static,
{
    fn init(&mut self, _req: &fuse::Request) -> Result<(), c_int> {
        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let self_cloned: Self = self.clone();
        let name = name.to_owned();
        tokio::spawn(async move {
            let fs = self_cloned.mutable.lock().await;

            let completion = fuse_result_wrapper::<FuseReplyEntry, _>(reply, |_reply| {
                let entry = fs.vfs.traverse_path(parent, [name].iter())?;
                Ok(file_entry_attrs(&entry))
            });

            drop(fs);

            completion.complete();
        });
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let self_cloned: Self = self.clone();
        tokio::spawn(async move {
            let fs = self_cloned.mutable.lock().await;

            let completion = fuse_result_wrapper::<FuseReplyAttr, _>(reply, |_reply| {
                let entry = fs.vfs.inodes.get(&ino).ok_or(NoEntityExists)?;
                Ok(file_entry_attrs(&entry))
            });

            drop(fs);

            completion.complete();
        });
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) {
        let self_cloned: Self = self.clone();

        if offset < 0 {
            reply.error(EINVAL);
            return;
        }

        let offset = offset as u64;
        tokio::spawn(async move {
            let fs = self_cloned.mutable.lock().await;

            let mut target_file = None;
            if let Some(pentry) = fs.vfs.inodes.get(&ino) {
                if let FileEntryData::File(ref file) = pentry.data {
                    target_file = Some(file.clone());
                }
            }
            if let Some(tf) = target_file {
                let info = fs.content_info.get_content_info(&tf.content_key).unwrap();
                let req = MultiPieceReadRequest {
                    content_key: tf.content_key,
                    piece_shas: &info.piece_shas[..],
                    piece_length: info.piece_length,
                    total_length: info.total_length,

                    file_offset: offset,
                    read_length: size as usize,
                    torrent_global_offset: tf.torrent_global_offset,
                };
                let bytes = match multi_piece_read(&fs.storage_backend, &req).await {
                    Ok(by) => by,
                    Err(err) => {
                        event!(Level::ERROR, "failed to fetch piece: {}", err);
                        reply.error(libc::EIO);
                        return;
                    }
                };
                drop(fs);
                reply.data(&bytes[..]);
            } else {
                drop(fs);
                reply.error(libc::ENOENT);
            }
        });
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, reply: ReplyDirectory) {
        let self_cloned: Self = self.clone();
        tokio::spawn(async move {
            let mut fs = self_cloned.mutable.lock().await;
            let completion = fuse_result_wrapper::<FuseReplyDirectory, _>(reply, |reply| {
                let FilesystemImplMutable { ref mut vfs, .. } = &mut *fs;

                let dir = vfs
                    .inodes
                    .get(&ino)
                    .ok_or(NoEntityExists)?
                    .get_directory()
                    .ok_or(NotADirectory)?;

                if offset <= 0 && reply.add(ino, 1, FileType::Directory, ".") {
                    return Ok(());
                }

                if offset <= 1 && reply.add(dir.parent, 2, FileType::Directory, "..") {
                    return Ok(());
                }

                let idx = if 2 <= offset { offset as usize - 2 } else { 0 };

                // later, we can reserve the lower few bits of the offset for internal stuff,
                // and bitshift it to get the resumption inode.
                for (i, chino) in dir.child_inodes.iter().enumerate().skip(idx) {
                    let ft = match chino.ft {
                        VfsFileType::RegularFile => FileType::RegularFile,
                        VfsFileType::Directory => FileType::Directory,
                    };
                    if reply.add(chino.inode, (i + 3) as i64, ft, &chino.file_name) {
                        return Ok(());
                    }
                }

                Ok(())
            });

            drop(fs);

            completion.complete();
        });
    }
}

use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "fuse-mount";

pub fn get_subcommand() -> App<'static, 'static> {
    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("mount torrents")
        .arg(
            Arg::with_name("config")
                .long("config")
                .value_name("FILE")
                .help("config file")
                .required(true)
                .takes_value(true),
        )
        // .arg(
        //     Arg::with_name("control-socket")
        //         .long("control-socket")
        //         .value_name("FILE")
        //         .help("Where to bind the control socket")
        //         .required(true)
        //         .takes_value(true),
        // )
        .arg(
            Arg::with_name("mount-point")
                .long("mount-point")
                .value_name("FILE")
                .help("Where to mount")
                .required(true)
                .takes_value(true),
        )
}

#[cfg(not(unix))]
pub async fn main(matches: &clap::ArgMatches<'_>) -> Result<(), failure::Error> {
    panic!("The `fuse-mount` feature only works on unix systems!");
}

#[derive(Default)]
pub struct FuseHost {}

#[tonic::async_trait]
impl MagnetiteFuseHost for FuseHost {
    async fn add_torrent(
        &self,
        _request: tonic::Request<AddTorrentRequest>,
    ) -> Result<tonic::Response<AddTorrentResponse>, tonic::Status> {
        Ok(tonic::Response::new(fuse_api::AddTorrentResponse {
            info_hash: vec![0; 20],
        }))
    }

    async fn remove_torrent(
        &self,
        _request: tonic::Request<RemoveTorrentRequest>,
    ) -> Result<tonic::Response<RemoveTorrentResponse>, tonic::Status> {
        Ok(tonic::Response::new(fuse_api::RemoveTorrentResponse {}))
    }
}

#[cfg(unix)]
#[allow(clippy::cognitive_complexity)] // macro bug around event!()
pub async fn main(matches: &clap::ArgMatches<'_>) -> Result<(), failure::Error> {
    use crate::model::config::Config;

    let config = matches.value_of("config").unwrap();
    let mut cfg_fi = File::open(&config).unwrap();
    let mut cfg_by = Vec::new();
    cfg_fi.read_to_end(&mut cfg_by).unwrap();
    let config: Config = toml::de::from_slice(&cfg_by).unwrap();

    // let control_socket = matches.value_of_os("control-socket").unwrap();
    // let control_socket = Path::new(control_socket).to_owned();
    let mount_point = matches.value_of_os("mount-point").unwrap();
    let mount_point = Path::new(mount_point).to_owned();

    let states = build_storage_engine_states(&config).unwrap();

    let mut fs_impl = FilesystemImplMutable {
        storage_backend: states.storage_engine,
        content_info: states.content_info_manager,
        vfs: Vfs {
            inodes: Default::default(),
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
        event!(Level::INFO, "added {:?} to filesystem", t.info_hash);
    }

    event!(
        Level::INFO,
        "mounting with {} known inodes",
        fs_impl.vfs.inodes.len()
    );

    tokio::task::spawn_blocking(move || {
        let options = ["-o", "ro", "-o", "fsname=magnetite"]
            .iter()
            .map(|o| o.as_ref())
            .collect::<Vec<&OsStr>>();

        let fs_impl = FilesystemImpl {
            mutable: Arc::new(Mutex::new(fs_impl)),
        };

        fuse::mount(fs_impl, &mount_point, &options).unwrap();
    })
    .await?;

    Ok(())
}

#[cfg(unix)]
mod unix {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use tokio::io::{AsyncRead, AsyncWrite};
    use tonic::transport::server::Connected;

    #[derive(Debug)]
    pub struct UnixStream(pub tokio::net::UnixStream);

    impl Connected for UnixStream {}

    impl AsyncRead for UnixStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for UnixStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }
}
