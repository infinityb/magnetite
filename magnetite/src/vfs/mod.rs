use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::Path;

use fuse::FileType;

mod errors;

pub use self::errors::{
    FilesystemIntegrityError, InvalidPath, InvalidRootInode, IsADirectory, NoEntityExists,
    NotADirectory,
};
use crate::model::TorrentID;

#[derive(Debug)]
pub struct Directory {
    pub parent: u64,
    pub child_inodes: Vec<DirectoryChild>,
}

#[derive(Debug)]
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

#[derive(Debug)]
pub enum FileEntryData {
    Dir(Directory),
    File(FileData),
}

#[derive(Debug)]
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

                    drop(chino);
                    drop(dir);

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
            drop(dir);

            self.inodes.insert(
                created_inode,
                sensible_directory_file_entry(parent_inode, created_inode),
            );

            // for v in affected_inodes {
            //     fs.info_hash_damage.insert((owner, v));
            // }

            return Ok(created_inode);
        } else {
            return Err(NotADirectory.into());
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
            parent: parent,
            child_inodes: Default::default(),
        }),
    }
}
