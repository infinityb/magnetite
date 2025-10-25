use fuse::{FileAttr, ReplyAttr, ReplyDirectory, ReplyEntry};
use libc::{c_int, EINVAL, EIO, ENOENT, ENOTDIR};
use tracing::{event, Level};

use super::TTL;
use crate::vfs::{InvalidPath, NoEntityExists, NotADirectory};

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

    fn respond(responder: Self::Responder, _: ()) {
        responder.ok();
    }

    fn error(responder: Self::Responder, errno: c_int) {
        responder.error(errno);
    }
}

// --

pub fn fuse_result_wrapper<P, F>(mut responder: P::Responder, f: F) -> FuseCompleter<P>
where
    P: FuseResultWrapper,
    F: FnOnce(&mut P::Responder) -> Result<P::Result, anyhow::Error>,
{
    match f(&mut responder) {
        Ok(result) => FuseCompleter {
            responder,
            inner: FuseCompleterInner::Respond(result),
        },
        Err(err) => {
            if err.downcast_ref::<NoEntityExists>().is_some() {
                FuseCompleter {
                    responder,
                    inner: FuseCompleterInner::Error(ENOENT),
                }
            } else if err.downcast_ref::<InvalidPath>().is_some() {
                FuseCompleter {
                    responder,
                    inner: FuseCompleterInner::Error(EINVAL),
                }
            } else if err.downcast_ref::<NotADirectory>().is_some() {
                FuseCompleter {
                    responder,
                    inner: FuseCompleterInner::Error(ENOTDIR),
                }
            } else {
                event!(
                    Level::ERROR,
                    "Responding with EIO due to unexpected error: {}",
                    err
                );
                FuseCompleter {
                    responder,
                    inner: FuseCompleterInner::Error(EIO),
                }
            }
        }
    }
}

// this lets us unlock things before we signal to fuse that we're done
// which may acquire a lock inside of fuse and do a write.
#[must_use]
pub struct FuseCompleter<P>
where
    P: FuseResultWrapper,
{
    responder: P::Responder,
    inner: FuseCompleterInner<P>,
}

enum FuseCompleterInner<P>
where
    P: FuseResultWrapper,
{
    Respond(P::Result),
    Error(c_int),
}

impl<P> FuseCompleter<P>
where
    P: FuseResultWrapper,
{
    pub fn complete(self) {
        match self.inner {
            FuseCompleterInner::Respond(result) => P::respond(self.responder, result),
            FuseCompleterInner::Error(errno) => P::error(self.responder, errno),
        }
    }
}
