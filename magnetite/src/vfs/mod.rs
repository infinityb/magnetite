use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::Path;

use std::sync::Arc;
use tokio::sync::Mutex;

use crate::model::TorrentMetaWrapped;
use crate::storage::PieceStorageEngineDumb;

mod errors;

pub use self::errors::{
    FilesystemIntegrityError, InvalidPath, InvalidRootInode, IsADirectory, NoEntityExists,
    NotADirectory,
};
use crate::model::TorrentID;
use crate::storage::state_wrapper;

#[derive(Clone, Debug)]
pub enum FileType {
    RegularFile,
    Directory,
}

#[derive(Clone, Debug)]
pub struct Directory {
    pub parent: u64,
    pub child_inodes: Vec<DirectoryChild>,
}

#[derive(Clone, Debug)]
pub struct DirectoryChild {
    pub file_name: OsString,
    pub ft: FileType,
    pub inode: u64,
}

#[derive(Debug, Clone)]
pub struct FileData {
    // length is in FileEntry#size
    pub content_key: TorrentID,
    pub torrent_global_offset: u64,
}

#[derive(Clone, Debug)]
pub enum FileEntryData {
    Dir(Directory),
    File(FileData),
}

#[derive(Clone, Debug)]
pub struct FileEntry {
    pub info_hash_owner_counter: u32,
    pub inode: u64,
    pub size: u64,
    pub data: FileEntryData,
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
pub struct Vfs {
    pub inodes: BTreeMap<u64, FileEntry>,
    pub inode_seq: u64,
}

impl Vfs {
    pub fn mkdir<T>(
        self: &mut Vfs,
        parent: u64,
        name: T,
        _owner: TorrentID,
    ) -> Result<u64, failure::Error>
    where
        T: AsRef<OsStr>,
    {
        let mut affected_inodes = Vec::new();

        let target = self.inodes.get_mut(&parent).ok_or(NoEntityExists)?;
        if let FileEntryData::Dir(ref mut dir) = target.data {
            let parent_inode = target.inode;
            affected_inodes.push(parent_inode);

            for chino in &dir.child_inodes {
                if chino.file_name == name.as_ref() {
                    let inode = chino.inode;
                    affected_inodes.push(inode);
                    // for v in affected_inodes {
                    //     fs.info_hash_damage.insert((owner, v));
                    // }
                    return Ok(inode);
                }
            }

            let created_inode = self.inode_seq;
            self.inode_seq += 1;

            dir.child_inodes.push(DirectoryChild {
                file_name: name.as_ref().to_owned(),
                ft: FileType::Directory,
                inode: created_inode,
            });
            affected_inodes.push(created_inode);

            self.inodes.insert(
                created_inode,
                sensible_directory_file_entry(parent_inode, created_inode),
            );

            // for v in affected_inodes {
            //     fs.info_hash_damage.insert((owner, v));
            // }

            Ok(created_inode)
        } else {
            Err(NotADirectory.into())
        }
    }

    pub fn traverse_path<I, T>(
        &self,
        parent: u64,
        path_parts: I,
    ) -> Result<&FileEntry, failure::Error>
    where
        I: Iterator<Item = T>,
        T: AsRef<OsStr>,
    {
        use smallvec::SmallVec;
        use std::path::Component;

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
                                let next_node = self
                                    .inodes
                                    .get(&chino.inode)
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
}

fn sensible_directory_file_entry(parent: u64, inode: u64) -> FileEntry {
    FileEntry {
        info_hash_owner_counter: 1,
        inode,
        size: 0,
        data: FileEntryData::Dir(Directory {
            parent,
            child_inodes: Default::default(),
        }),
    }
}

#[derive(Debug)]
pub struct FilesystemImplMutable<P> {
    // any directory or file that has been updated.  If an owner (torrent) is removed
    // then the info_hash_owner_counter will decrement.  If the info_hash_owner_counter
    // counter drops to 0, that file shall be deallocated.
    pub storage_backend: P,
    pub content_info: state_wrapper::ContentInfoManager,
    // info_hash_damage: BTreeSet<(TorrentID, u64)>,
    pub vfs: Vfs,
}

impl<P> FilesystemImplMutable<P>
where
    P: PieceStorageEngineDumb + Clone + Send + Sync + 'static,
{
    pub fn add_torrent(&mut self, tm: &TorrentMetaWrapped) -> Result<(), failure::Error> {
        use smallvec::SmallVec;

        let mut cur_offset: u64 = 0;
        for f in &tm.meta.info.files {
            let torrent_global_offset = cur_offset;
            cur_offset += f.length;
            if f.path == Path::new("") {
                continue;
            }

            let dir_path = f.path.parent().unwrap();
            let file_name = f.path.file_name().unwrap();

            let mut assert_path = SmallVec::<[u64; 8]>::new();

            let mut parent = 1;
            parent = self.vfs.mkdir(parent, &tm.meta.info.name, tm.info_hash)?;
            assert_path.push(parent);

            for p in dir_path.components() {
                parent = self.vfs.mkdir(parent, p, tm.info_hash)?;
                assert_path.push(parent);
            }

            let created_inode = self.vfs.inode_seq;
            self.vfs.inode_seq += 1;

            assert_path.push(created_inode);

            self.vfs.inodes.insert(
                created_inode,
                FileEntry {
                    info_hash_owner_counter: 1,
                    inode: created_inode,
                    size: f.length,
                    data: FileEntryData::File(FileData {
                        content_key: tm.info_hash,
                        torrent_global_offset,
                    }),
                },
            );

            let target = self.vfs.inodes.get_mut(&parent).unwrap();
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

#[derive(Clone)]
pub struct FilesystemImpl<P> {
    pub mutable: Arc<Mutex<FilesystemImplMutable<P>>>,
}
