mod bytes_cow;
#[cfg(target_os = "linux")]
mod owned_fd;

pub use self::bytes_cow::BytesCow;
#[cfg(target_os = "linux")]
pub use self::owned_fd::OwnedFd;
