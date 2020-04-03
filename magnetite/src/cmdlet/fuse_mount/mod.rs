#![cfg_attr(not(unix), allow(unused_imports))]

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::path::Path;
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use std::collections::HashMap;
use std::io::Read;
use std::fs::File;
use std::path::PathBuf;

use bytes::Bytes;
use salsa20::stream_cipher::generic_array::GenericArray;
use salsa20::stream_cipher::NewStreamCipher;
use salsa20::XSalsa20;
use clap::{App, Arg, SubCommand};
use fuse::{
    ReplyEmpty, ReplyOpen,
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request,
};
use libc::{c_int, ENOENT, ENOTDIR, EIO};
use sha1::Digest;
use tokio::net::UnixListener;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tokio::fs::File as TokioFile;
use lru::LruCache;
use slab::Slab;
use tracing::{event, Level};

mod errors;

use crate::storage::PieceStorageEngine;
pub use self::errors::{InvalidRootInode, NotADirectory, NoEntityExists, InvalidPath, FilesystemIntegrityError};
use crate::model::{BitField, TorrentID, TorrentMetaWrapped};
use crate::storage::{PieceFileStorageEngine, PieceFileStorageEngineVerifyMode, PieceFileStorageEngineLockables};


pub mod fuse_api {
    tonic::include_proto!("magnetite.api.fuse");
}

use fuse_api::{
    magnetite_fuse_host_server::MagnetiteFuseHost, AddTorrentRequest, AddTorrentResponse,
    RemoveTorrentRequest, RemoveTorrentResponse,
};

#[derive(Debug)]
struct Directory {
    parent: u64,
    child_inodes: Vec<DirectoryChild>,
}

#[derive(Debug)]
struct DirectoryChild {
    file_name: OsString,
    ft: FileType,
    inode: u64,
}

#[derive(Debug)]
#[derive(Clone)]
struct FileData {
    // length is in FileAttr#size
    info_hash: TorrentID,
    torrent_rel_offset: u64,
    // piece_index_start: u32,
    // piece_offset_start: u32,
}

#[derive(Debug)]
enum FileEntryData {
    Dir(Directory),
    File(FileData),
}

#[derive(Debug)]
struct FileEntry {
    info_hash_owner_counter: u32,
    attrs: FileAttr,
    data: FileEntryData,
}

impl FileEntry {
    pub fn get_file(&self) -> Option<&FileData> {
        match self.data {
            FileEntryData::File(ref rd) => Some(rd),
            _ => None,
        }
    }

    pub fn get_directory(&self) -> Option<&Directory> {
        match self.data {
            FileEntryData::Dir(ref dir) => Some(dir),
            _ => None,
        }
    }
}

#[derive(Debug)]
enum OpenState {
    Directory {
        sent_parent: bool,
        sent_self: bool,
        directory_inode: u64,
        last_sent_inode: u64,
        // next_index: usize,
    }
}

#[derive(Debug)]
struct FilesystemImplMutable {
    // any directory or file that has been updated.  If an owner (torrent) is removed
    // then the info_hash_owner_counter will decrement.  If the info_hash_owner_counter
    // counter drops to 0, that file shall be deallocated.
    piece_cache: LruCache<(TorrentID, u32), Bytes>,
    storage_backends: BTreeMap<TorrentID, PieceFileStorageEngine>,
    info_hash_damage: BTreeSet<(TorrentID, u64)>,
    inodes: BTreeMap<u64, FileEntry>,
    inode_seq: u64,
    open_files: Slab<OpenState>,
}

fn sensible_directory_file_attr(ino: u64) -> FileAttr {
    FileAttr {
        ino,
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
    }
}

fn sensible_directory_file_entry(parent: u64, ino: u64) -> FileEntry {
    FileEntry {
        info_hash_owner_counter: 1,
        attrs: sensible_directory_file_attr(ino),
        data: FileEntryData::Dir(Directory {
            parent: parent,
            child_inodes: Default::default(),
        })
    }
}

fn fs_impl_mkdir<T>(
    fs: &mut FilesystemImplMutable,
    parent: u64,
    name: T,
    owner: TorrentID,
) -> Result<u64, failure::Error> where T: AsRef<OsStr> {
    let mut affected_inodes = Vec::new();

    let target = fs.inodes.get_mut(&parent).ok_or(NoEntityExists)?;
    if let FileEntryData::Dir(ref mut dir) = target.data {
        let parent_inode = target.attrs.ino;
        affected_inodes.push(parent_inode);

        for chino in &dir.child_inodes {
            if chino.file_name == name.as_ref() {
                let inode = chino.inode;
                affected_inodes.push(inode);
                
                drop(chino);
                drop(dir);
                
                for v in affected_inodes {
                    fs.info_hash_damage.insert((owner, v));
                }
                return Ok(inode);
            }
        }

        let created_inode = fs.inode_seq;
        fs.inode_seq += 1;
        
        dir.child_inodes.push(DirectoryChild {
            file_name: name.as_ref().to_owned(),
            ft: FileType::Directory,
            inode: created_inode,
        });
        affected_inodes.push(created_inode);
        drop(dir);

        fs.inodes.insert(created_inode,
            sensible_directory_file_entry(parent_inode, created_inode));

        for v in affected_inodes {
            fs.info_hash_damage.insert((owner, v));
        }

        return Ok(created_inode);
    } else {
        return Err(NotADirectory.into());
    }
}

impl FilesystemImplMutable {
    #[inline]
    fn traverse_path<I, T>(&self, parent: u64, path_parts: I) -> Result<&FileEntry, failure::Error>
        where I: Iterator<Item=T>, T: AsRef<OsStr>
    {
        use std::path::Component;
        use smallvec::SmallVec;

        let mut entry_path = SmallVec::<[&FileEntry; 16]>::new();
        let root = self.inodes.get(&parent).ok_or(InvalidRootInode)?;
        entry_path.push(root);

        for part in path_parts {
            let part: &OsStr = part.as_ref();
            for comp in Path::new(part).components() {
                match comp {
                    Component::RootDir | Component::Prefix(..) => return Err(InvalidPath.into()),
                    Component::CurDir => (),
                    Component::ParentDir if entry_path.len() > 1 => {
                        entry_path.pop();
                    }
                    Component::ParentDir => return Err(InvalidPath.into()),
                    Component::Normal(pp) => {
                        let current_entry = entry_path[entry_path.len() - 1];
                        let dir = current_entry.get_directory().ok_or(NotADirectory)?;
                        let mut found_file = false;
                        for chino in &dir.child_inodes {
                            if chino.file_name == pp {
                                found_file = true;
                                let next_node = self.inodes.get(&chino.inode)
                                    .ok_or(FilesystemIntegrityError)?;
                                entry_path.push(next_node);
                            }
                        }
                        if !found_file {
                            return Err(NoEntityExists.into());
                        }
                    }
                }
            }
        }
        Ok(entry_path[entry_path.len() - 1])
    }

    pub fn add_torrent(&mut self, tm: &TorrentMetaWrapped) -> Result<(), failure::Error> {
        use smallvec::SmallVec;

        let mut cur_offset: u64 = 0;
        for f in &tm.meta.info.files {
            let torrent_rel_offset = cur_offset;
            cur_offset += f.length;
            if f.path == Path::new("") {
                continue;
            }

            let dir_path = f.path.parent().unwrap();
            let file_name = f.path.file_name().unwrap();

            let mut assert_path = SmallVec::<[u64; 8]>::new();

            let mut parent = 1;
            parent = fs_impl_mkdir(self, parent, &tm.meta.info.name, tm.info_hash)?;
            assert_path.push(parent);
            
            for p in dir_path.components() {
                parent = fs_impl_mkdir(self, parent, p, tm.info_hash)?;
                assert_path.push(parent);
            }

            let created_inode = self.inode_seq;
            self.inode_seq += 1;

            assert_path.push(created_inode);

            self.inodes.insert(
                created_inode,
                FileEntry {
                    info_hash_owner_counter: 1,
                    attrs: FileAttr {
                        ino: created_inode,
                        size: f.length,
                        blocks: 1,
                        atime: UNIX_EPOCH,
                        mtime: UNIX_EPOCH,
                        ctime: UNIX_EPOCH,
                        crtime: UNIX_EPOCH,
                        kind: FileType::RegularFile,
                        perm: 0o444,
                        nlink: 1,
                        uid: 501,
                        gid: 20,
                        rdev: 0,
                        flags: 0,
                    },
                    data: FileEntryData::File(FileData {
                        info_hash: tm.info_hash,
                        torrent_rel_offset,
                    }),
                },
            );

            let target = self.inodes.get_mut(&parent).unwrap();
            if let FileEntryData::Dir(ref mut dir) = target.data {
                dir.child_inodes.push(DirectoryChild {
                    file_name: AsRef::<OsStr>::as_ref(file_name).to_owned(),
                    ft: FileType::RegularFile,
                    inode: created_inode,
                });
            }

            // {
            //     println!("asserting {:#?} on {:#?}", assert_path, self);
            //     let mut inode_path = Vec::new();
            //     let mut current = self.inodes.get(&1).unwrap();
            //     for assert_inode in &assert_path {
            //         inode_path.push(current);
            //         let cur_dir = current.get_directory().unwrap();

            //         let mut reachable = false;
            //         for chino in &cur_dir.child_inodes {
            //             println!("checking for {} in {:#?}", assert_inode, cur_dir);
            //             if chino.inode == *assert_inode {
            //                 reachable = true;
            //             }
            //         }

            //         if !reachable {
            //             println!("asserting {:#?} on {:#?} -- {:#?}", assert_path, self, inode_path);
            //             panic!("failed to make file reachable - this is a bug");
            //         }

            //         current = self.inodes.get(assert_inode).unwrap();
            //     }
            // }
        }
        Ok(())
    }
}

struct FilesystemImpl {
    mutable: Arc<Mutex<FilesystemImplMutable>>,
}


const HELLO_TXT_CONTENT: &str = "Hello World!\n";

const TTL: std::time::Duration = std::time::Duration::from_secs(120);

impl Filesystem for FilesystemImpl {
    fn init(&mut self, _req: &fuse::Request) -> Result<(), c_int> {
        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let mutable = Arc::clone(&self.mutable);
        let name = name.to_owned();
        tokio::spawn(async move {
            let fs = mutable.lock().await;
            match fs.traverse_path(parent, [name].iter()) {
                Ok(fe) => reply.entry(&TTL, &fe.attrs, 0),
                Err(err) => {
                    if err.downcast_ref::<NoEntityExists>().is_some() {
                        reply.error(ENOENT);
                    } else if err.downcast_ref::<InvalidPath>().is_some() {
                        reply.error(ENOENT);
                    } else if err.downcast_ref::<NotADirectory>().is_some() {
                        reply.error(ENOTDIR);
                    } else {
                        event!(Level::ERROR, "Responding with EIO due to unexpected error: {}", err);
                        reply.error(EIO);
                    }
                }
            }
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
        size: u32,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(ENOENT);
            return;
        }
        let offset = offset as u64;

        let mutable = Arc::clone(&self.mutable);
        tokio::spawn(async move {
            let mut fs = mutable.lock().await;
            let mut target_file = None;
            if let Some(pentry) = fs.inodes.get(&ino) {
                if let FileEntryData::File(ref file) = pentry.data {
                    target_file = Some(file.clone());
                }
            }
            if let Some(tf) = target_file {

                let storage_engine = fs.storage_backends.get(&tf.info_hash).unwrap().clone();
                let piece_file_offset = tf.torrent_rel_offset + offset;
                let piece_index = (piece_file_offset / storage_engine.get_piece_length()) as u32;
                let piece_offset = (piece_file_offset % storage_engine.get_piece_length()) as u32;

                let cache_key = (tf.info_hash, piece_index);
                let piece_data;
                if let Some(cached) = fs.piece_cache.get(&cache_key) {
                    piece_data = cached.clone();
                    drop(cached);
                    drop(fs);
                } else {
                    drop(fs);

                    piece_data = storage_engine.get_piece(&tf.info_hash, piece_index as u32).await.unwrap();

                    let mut fs = mutable.lock().await;
                    fs.piece_cache.put((tf.info_hash, piece_index), piece_data.clone());
                    drop(fs);
                }

                let mut interesting_data = &piece_data[piece_offset as usize..];
                if (size as usize) < interesting_data.len() {
                    interesting_data = &interesting_data[..size as usize];
                }
                reply.data(interesting_data);
            } else {
                reply.error(ENOENT);
            }            
        });
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let mutable = Arc::clone(&self.mutable);
        tokio::spawn(async move {
            let mut fs = mutable.lock().await;
            let FilesystemImplMutable {
                ref mut open_files,
                ref mut inodes,
                ..
            } = &mut *fs;

            let dir = inodes.get(&ino).unwrap().get_directory().unwrap();
            let mut replies = 0;
            let fh = fh as usize;
            if offset <= 0 {
                if reply.add(ino, 1, FileType::Directory, ".") {
                    reply.ok();
                    return;
                }
                replies += 1;
            }
            if offset <= 1 {
                if reply.add(dir.parent, 2, FileType::Directory, "..") {
                    reply.ok();
                    return;
                }
                replies += 1;
            }

            {
                let mut idx: usize = 0;
                if 2 <= offset {
                    idx = offset as usize - 2;
                }
                // later, we can reserve the lower few bits of the offset for internal stuff,
                // and bitshift it to get the resumption inode.
                for (i, chino) in dir.child_inodes.iter().enumerate().skip(idx) {
                    if reply.add(chino.inode, (i + 3) as i64, chino.ft, &chino.file_name) {
                        reply.ok();
                        return;
                    }
                    replies += 1;
                }
            }

            drop(dir);
            drop(fs);

            reply.ok();
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
pub fn main(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    struct TorrentFactory {
        torrent_file: PathBuf,
        source_file: PathBuf,
        secret: String,
    }

    let control_socket = matches.value_of_os("control-socket").unwrap();
    let control_socket = Path::new(control_socket).to_owned();
    let mount_point = matches.value_of_os("mount-point").unwrap();
    let mount_point = Path::new(mount_point).to_owned();

    //  /Users/sell/Downloads/danbooru2019-torrent/danbooru2019-2-loaded.torrent
    let mut rt = Runtime::new()?;
    let mut sources = Vec::new();
    sources.push(TorrentFactory {
        torrent_file: Path::new("/Users/sell/Downloads/danbooru2019-torrent/danbooru2019-0-loaded.torrent").to_owned(),
        source_file: Path::new("/Volumes/home/danbooru2019-0.tome").to_owned(),
        secret: "C3EsrGPe62jQx6U6Z6JTxCcWKSWpA4G2".to_string(),
    });
    sources.push(TorrentFactory {
        torrent_file: Path::new("/Users/sell/Downloads/danbooru2019-torrent/danbooru2019-1-loaded.torrent").to_owned(),
        source_file: Path::new("/Volumes/home/danbooru2019-1.tome").to_owned(),
        secret: "RhdmQRQZzbSwbqyKk4T4tzxcQ4BG6V9b".to_string(),
    });
    sources.push(TorrentFactory {
        torrent_file: Path::new("/Users/sell/Downloads/danbooru2019-torrent/danbooru2019-2-loaded.torrent").to_owned(),
        source_file: Path::new("/Volumes/home/danbooru2019-2.tome").to_owned(),
        secret: "afqR5ALeMtoyYej9UNTQi7YEM4dtdbjQ".to_string(),
    });
    sources.push(TorrentFactory {
        torrent_file: Path::new("/Users/sell/Downloads/danbooru2019-torrent/danbooru2019-3-loaded.torrent")
            .to_owned(),
        source_file: Path::new("/Volumes/home/danbooru2019-3.tome").to_owned(),
        secret: "4r86Ky8jQQt3JQncXZGg2zBQsNXbdjb9".to_string(),
    });
    sources.push(TorrentFactory {
        torrent_file: Path::new("/Users/sell/Downloads/danbooru2019-torrent/danbooru2019-4-loaded.torrent").to_owned(),
        source_file: Path::new("/Volumes/home/danbooru2019-4.tome").to_owned(),
        secret: "g9e8mSsydbxKsSqgU6oCoaqpVybfrGfN".to_string(),
    });
    sources.push(TorrentFactory {
        torrent_file: Path::new("/Users/sell/Downloads/danbooru2019-torrent/danbooru2019-5-loaded.torrent").to_owned(),
        source_file: Path::new("/Volumes/home/danbooru2019-5.tome").to_owned(),
        secret: "bKDMbbPwuQiY7grxjdaEaQSq53hkhuSK".to_string(),
    });
    sources.push(TorrentFactory {
        torrent_file: Path::new("/Users/sell/Downloads/danbooru2019-torrent/danbooru2019-6.torrent")
            .to_owned(),
        source_file: Path::new("/Volumes/home/danbooru2019-6.tome").to_owned(),
        secret: "xDY4RGFDtzk5CUM8N77RBeQ7wB9fujej".to_string(),
    });
    sources.push(TorrentFactory {
        torrent_file: Path::new("/Users/sell/Downloads/danbooru2019-torrent/danbooru2019-7.torrent").to_owned(),
        source_file: Path::new("/Volumes/home/danbooru2019-7.tome").to_owned(),
        secret: "qEvcKZiR8ynHC54SeETgrZVjoKGCUke9".to_string(),
    });
    sources.push(TorrentFactory {
        torrent_file: Path::new("/Users/sell/Downloads/danbooru2019-torrent/danbooru2019-8.torrent").to_owned(),
        source_file: Path::new("/Volumes/home/danbooru2019-8.tome").to_owned(),
        secret: "4jJZSpMwrzVSNZBZf9RqaR4UxDYy3QT7".to_string(),
    });
    sources.push(TorrentFactory {
        torrent_file: Path::new("/Users/sell/Downloads/danbooru2019-torrent/danbooru2019-9.torrent")
            .to_owned(),
        source_file: Path::new("/Volumes/home/danbooru2019-9.tome").to_owned(),
        secret: "Y2scZxSQpbxktS7fKR9YpL8uzrh2bSSZ".to_string(),
    });

    let mut fs_impl = FilesystemImplMutable {
        piece_cache: LruCache::<(TorrentID, u32), Bytes>::new(32),
        storage_backends: BTreeMap::new(),
        info_hash_damage: BTreeSet::new(),
        inodes: BTreeMap::new(),
        inode_seq: 3,
        open_files: Slab::new(),
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
                perm: 0o555,
                nlink: 2,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
            },
            data: FileEntryData::Dir(Directory {
                parent: 1,
                child_inodes: Default::default(),
            }),
        },
    );

    for s in sources.into_iter() {
        let mut fi = File::open(&s.torrent_file).unwrap();
        let mut by = Vec::new();
        fi.read_to_end(&mut by).unwrap();

        let mut torrent_meta = TorrentMetaWrapped::from_bytes(&by).unwrap();
        // torrent_meta.meta.info.files.truncate(4);
        let torrent_meta = Arc::new(torrent_meta);

        let info_hash = torrent_meta.info_hash;
        let piece_file = rt.block_on(TokioFile::open(&s.source_file)).unwrap();

        let mut nonce_data = [0; 24];
        for (o, i) in nonce_data[4..]
            .iter_mut()
            .zip(torrent_meta.info_hash.as_bytes().iter())
        {
            *o = *i;
        }
        let nonce = GenericArray::from_slice(&nonce_data[..]);
        let bf_length = torrent_meta.meta.info.pieces.chunks(20).count() as u32;
        let key = GenericArray::from_slice(s.secret.as_bytes());

        let mut builder = PieceFileStorageEngine::from_torrent_wrapped(&torrent_meta);
        builder.set_complete();
        builder.set_crypto(XSalsa20::new(&key, &nonce));
        let storage_engine = builder.build(piece_file);

        println!("loaded {} files from torrent file: {:?}", torrent_meta.meta.info.files.len(), fs_impl.add_torrent(&torrent_meta));
        fs_impl.storage_backends.insert(torrent_meta.info_hash, storage_engine);
        println!("added info_hash: {:?}", info_hash);
    }


    rt.block_on(async {
        let _uds = UnixListener::bind(&control_socket)?;
        tokio::task::spawn_blocking(move || {
            let options = ["-o", "ro", "-o", "fsname=magnetite"]
                .iter()
                .map(|o| o.as_ref())
                .collect::<Vec<&OsStr>>();

            println!("mounting with {} known inodes", fs_impl.inodes.len());
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
