#![cfg_attr(not(unix), allow(unused_imports))]

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::path::Path;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use clap::{App, Arg, SubCommand};
use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request,
};

use libc::{c_int, ENOENT, ENOTDIR};
use sha1::Digest;
use tokio::net::UnixListener;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

use crate::model::TorrentID;

pub mod fuse_api {
    tonic::include_proto!("magnetite.api.fuse");
}

use fuse_api::{
    magnetite_fuse_host_server::MagnetiteFuseHost, AddTorrentRequest, AddTorrentResponse,
    RemoveTorrentRequest, RemoveTorrentResponse,
};

struct Directory {
    parent: u64,
    child_inodes: Vec<DirectoryChild>,
}

struct DirectoryChild {
    file_name: OsString,
    ft: FileType,
    inode: u64,
}

struct FileData {
    // length is in FileAttr#size
    info_hash: TorrentID,
    piece_index_start: u32,
}

enum FileEntryData {
    Dir(Directory),
    File(FileData),
}

struct FileEntry {
    info_hash_owner_counter: u32,
    attrs: FileAttr,
    data: FileEntryData,
}

struct FilesystemImplMutable {
    // any directory or file that has been updated.  If an owner (torrent) is removed
    // then the info_hash_owner_counter will decrement.  If the info_hash_owner_counter
    // counter drops to 0, that file shall be deallocated.
    info_hash_damage: BTreeSet<(TorrentID, u64)>,
    inodes: BTreeMap<u64, FileEntry>,
    inode_seq: u64,
}

struct FilesystemImpl {
    mutable: Arc<Mutex<FilesystemImplMutable>>,
}

const HELLO_TXT_CONTENT: &str = "Hello World!\n";

const TTL: std::time::Duration = std::time::Duration::from_secs(20);

impl Filesystem for FilesystemImpl {
    fn init(&mut self, _req: &fuse::Request) -> Result<(), c_int> {
        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let mutable = Arc::clone(&self.mutable);
        let name = name.to_owned();
        tokio::spawn(async move {
            let fs = mutable.lock().await;
            if let Some(pentry) = fs.inodes.get(&parent) {
                match pentry.data {
                    FileEntryData::Dir(ref dir) => {
                        for d in dir.child_inodes.iter() {
                            if d.file_name == name {
                                let file_data = &fs.inodes[&d.inode];
                                reply.entry(&TTL, &file_data.attrs, 0);
                                return;
                            }
                        }
                    }
                    FileEntryData::File(..) => {
                        reply.error(ENOTDIR);
                        return;
                    }
                }
            }
            reply.error(ENOENT);
        });
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let mutable = Arc::clone(&self.mutable);
        tokio::spawn(async move {
            let fs = mutable.lock().await;
            if let Some(pentry) = fs.inodes.get(&ino) {
                reply.attr(&TTL, &pentry.attrs);
                return;
            }
            reply.error(ENOENT);
        });
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        _size: u32,
        reply: ReplyData,
    ) {
        let mutable = Arc::clone(&self.mutable);
        tokio::spawn(async move {
            let fs = mutable.lock().await;
            if let Some(pentry) = fs.inodes.get(&ino) {
                if let FileEntryData::File(ref _file) = pentry.data {
                    reply.data(&HELLO_TXT_CONTENT.as_bytes()[offset as usize..]);
                    return;
                }
            }
            reply.error(ENOENT);
        });
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let mutable = Arc::clone(&self.mutable);
        tokio::spawn(async move {
            let fs = mutable.lock().await;
            if let Some(pentry) = fs.inodes.get(&ino) {
                // self and parent are intrinsic
                if let FileEntryData::Dir(ref dir) = pentry.data {
                    let mut entries: Vec<(_, _, &OsStr)> = vec![
                        (ino, FileType::Directory, OsStr::new(".")),
                        (dir.parent, FileType::Directory, OsStr::new("..")),
                    ];
                    for chino in &dir.child_inodes {
                        entries.push((chino.inode, chino.ft, &chino.file_name));
                    }
                    for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
                        reply.add(entry.0, (i + 1) as i64, entry.1, entry.2);
                    }
                    reply.ok();

                    return;
                }
            }
            reply.error(ENOENT);
            return;
        });
    }
}

use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "mount-daemon";

pub fn get_subcommand() -> App<'static, 'static> {
    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("mount torrents")
        .arg(
            Arg::with_name("control-socket")
                .long("control-socket")
                .value_name("FILE")
                .help("Where to bind the control socket")
                .required(true)
                .takes_value(true),
        )
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
pub fn main() -> Result<(), failure::Error> {
    panic!("The `uds` example only works on unix systems!");
}

#[derive(Default)]
pub struct FuseHost {}

#[tonic::async_trait]
impl MagnetiteFuseHost for FuseHost {
    async fn add_torrent(
        &self,
        _request: tonic::Request<AddTorrentRequest>,
    ) -> Result<tonic::Response<AddTorrentResponse>, tonic::Status> {
        Ok(tonic::Response::new(fuse_api::AddTorrentResponse {}))
    }

    async fn remove_torrent(
        &self,
        _request: tonic::Request<RemoveTorrentRequest>,
    ) -> Result<tonic::Response<RemoveTorrentResponse>, tonic::Status> {
        Ok(tonic::Response::new(fuse_api::RemoveTorrentResponse {}))
    }
}

#[cfg(unix)]
pub fn main(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    let control_socket = matches.value_of_os("control-socket").unwrap();
    let control_socket = Path::new(control_socket).to_owned();
    let mount_point = matches.value_of_os("mount-point").unwrap();
    let mount_point = Path::new(mount_point).to_owned();

    let mut rt = Runtime::new()?;
    rt.block_on(async {
        let _uds = UnixListener::bind(&control_socket)?;
        tokio::task::spawn_blocking(move || {
            let options = ["-o", "ro", "-o", "fsname=hello"]
                .iter()
                .map(|o| o.as_ref())
                .collect::<Vec<&OsStr>>();

            let mut fs_impl = FilesystemImplMutable {
                info_hash_damage: BTreeSet::new(),
                inodes: BTreeMap::new(),
                inode_seq: 3,
            };
            fs_impl.inodes.insert(
                1,
                FileEntry {
                    info_hash_owner_counter: 1,
                    attrs: FileAttr {
                        ino: 1,
                        size: 0,
                        blocks: 0,
                        atime: UNIX_EPOCH,
                        mtime: UNIX_EPOCH,
                        ctime: UNIX_EPOCH,
                        crtime: UNIX_EPOCH,
                        kind: FileType::Directory,
                        perm: 0o755,
                        nlink: 2,
                        uid: 501,
                        gid: 20,
                        rdev: 0,
                        flags: 0,
                    },
                    data: FileEntryData::Dir(Directory {
                        parent: 1,
                        child_inodes: vec![DirectoryChild {
                            inode: 2,
                            ft: FileType::RegularFile,
                            file_name: "hello.txt".into(),
                        }],
                    }),
                },
            );
            fs_impl.inodes.insert(
                2,
                FileEntry {
                    info_hash_owner_counter: 1,
                    attrs: FileAttr {
                        ino: 2,
                        size: 13,
                        blocks: 1,
                        atime: UNIX_EPOCH,
                        mtime: UNIX_EPOCH,
                        ctime: UNIX_EPOCH,
                        crtime: UNIX_EPOCH,
                        kind: FileType::RegularFile,
                        perm: 0o644,
                        nlink: 1,
                        uid: 501,
                        gid: 20,
                        rdev: 0,
                        flags: 0,
                    },
                    data: FileEntryData::File(FileData {
                        info_hash: TorrentID::zero(),
                        piece_index_start: 0,
                    }),
                },
            );

            // fs_impl.add_torrent();

            let fs_impl = FilesystemImpl {
                mutable: Arc::new(Mutex::new(fs_impl)),
            };
            fuse::mount(fs_impl, &mount_point, &options).unwrap();
        })
        .await?;

        Ok(())
    })
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
