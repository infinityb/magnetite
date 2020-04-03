use std::fmt;

// --

#[derive(Debug)]
pub struct InvalidRootInode;

impl fmt::Display for InvalidRootInode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InvalidRootInode")
    }
}

impl std::error::Error for InvalidRootInode {}

// --

#[derive(Debug)]
pub struct NotADirectory;

impl fmt::Display for NotADirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NotADirectory")
    }
}

impl std::error::Error for NotADirectory {}

// --

#[derive(Debug)]
pub struct NoEntityExists;

impl fmt::Display for NoEntityExists {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NoEntityExists")
    }
}

impl std::error::Error for NoEntityExists {}

// -- 

#[derive(Debug)]
pub struct InvalidPath;

impl fmt::Display for InvalidPath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InvalidPath")
    }
}

impl std::error::Error for InvalidPath {}

// -- 

#[derive(Debug)]
pub struct FilesystemIntegrityError;

impl fmt::Display for FilesystemIntegrityError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FilesystemIntegrityError")
    }
}

impl std::error::Error for FilesystemIntegrityError {}