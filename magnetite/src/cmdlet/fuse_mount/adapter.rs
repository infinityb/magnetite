use fuse::{FileAttr, ReplyAttr, ReplyDirectory, ReplyEntry};
use libc::{c_int, EINVAL, EIO, ENOENT, ENOTDIR};
use tracing::{event, Level};

use super::TTL;
use crate::vfs::{
    Directory, DirectoryChild, FileData, FileEntry, FileEntryData, FilesystemIntegrityError,
    InvalidPath, InvalidRootInode, NoEntityExists, NotADirectory, Vfs,
};

pub trait FuseResultWrapper {
    type Responder;
    type Result;

    fn respond(responder: Self::Responder, result: Self::Result);

    fn error(responder: Self::Responder, errno: c_int);
}

// --

pub struct FuseReplyEntry;

impl FuseResultWrapper for FuseReplyEntry {
    type Responder = ReplyEntry;
    type Result = FileAttr;

    fn respond(responder: Self::Responder, result: Self::Result) {
        responder.entry(&TTL, &result, 0);
    }

    fn error(responder: Self::Responder, errno: c_int) {
        responder.error(errno);
    }
}

// --

pub struct FuseReplyAttr;

impl FuseResultWrapper for FuseReplyAttr {
    type Responder = ReplyAttr;
    type Result = FileAttr;

    fn respond(responder: Self::Responder, result: Self::Result) {
        responder.attr(&TTL, &result);
    }

    fn error(responder: Self::Responder, errno: c_int) {
        responder.error(errno);
    }
}

// --

pub struct FuseReplyDirectory;

impl FuseResultWrapper for FuseReplyDirectory {
    type Responder = ReplyDirectory;
    type Result = ();

    fn respond(responder: Self::Responder, result: Self::Result) {
        responder.ok();
    }

    fn error(responder: Self::Responder, errno: c_int) {
        responder.error(errno);
    }
}

// --

pub fn fuse_result_wrapper<P, F>(mut reply: P::Responder, f: F) -> ()
where
    P: FuseResultWrapper,
    F: FnOnce(&mut P::Responder) -> Result<P::Result, failure::Error>,
{
    match f(&mut reply) {
        Ok(v) => P::respond(reply, v),
        Err(err) => {
            if err.downcast_ref::<NoEntityExists>().is_some() {
                P::error(reply, ENOENT);
            } else if err.downcast_ref::<InvalidPath>().is_some() {
                P::error(reply, EINVAL);
            } else if err.downcast_ref::<NotADirectory>().is_some() {
                P::error(reply, ENOTDIR);
            } else {
                event!(
                    Level::ERROR,
                    "Responding with EIO due to unexpected error: {}",
                    err
                );
                P::error(reply, EIO);
            }
        }
    }
}
