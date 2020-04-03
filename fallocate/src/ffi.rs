use libc::{c_int, off_t};

#[link(name = "c")]
extern {
    // int fallocate(int fd, int mode, off_t offset, off_t len);
    pub fn fallocate(fd: c_int, mode: c_int, offset: off_t, len: off_t) -> c_int;
}