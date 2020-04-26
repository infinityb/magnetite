use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd};

use nix::fcntl::{fcntl, FcntlArg};
use nix::unistd::{read, write};

#[cfg(target_os = "macos")]
use nix::unistd::lseek as lseek64;

#[cfg(target_os = "linux")]
use nix::unistd::lseek64;

#[derive(Debug)]
pub struct OwnedFd {
    raw_fd: RawFd,
}

impl OwnedFd {
    pub fn dup(raw: RawFd) -> Result<OwnedFd, nix::Error> {
        fcntl(raw, nix::fcntl::FcntlArg::F_DUPFD_CLOEXEC(0)).map(|raw_fd| OwnedFd { raw_fd })
    }

    pub fn set_capacity(&mut self, size: i64) -> Result<(), nix::Error> {
        nix::unistd::ftruncate(self.raw_fd, size)
    }

    pub fn into_raw_fd(self) -> RawFd {
        let fd_no = self.raw_fd;
        ::std::mem::forget(self);
        fd_no
    }

    pub fn into_file(self) -> File {
        unsafe { FromRawFd::from_raw_fd(self.into_raw_fd()) }
    }
}

impl FromRawFd for OwnedFd {
    unsafe fn from_raw_fd(fd: RawFd) -> OwnedFd {
        OwnedFd { raw_fd: fd }
    }
}

impl From<File> for OwnedFd {
    fn from(fd: File) -> OwnedFd {
        OwnedFd {
            raw_fd: fd.into_raw_fd(),
        }
    }
}

impl AsRawFd for OwnedFd {
    fn as_raw_fd(&self) -> RawFd {
        self.raw_fd
    }
}

impl Drop for OwnedFd {
    fn drop(&mut self) {
        let _ = nix::unistd::close(self.raw_fd);
    }
}
