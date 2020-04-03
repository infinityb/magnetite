use libc::c_int;

// Don't extend size of file even if offset + len is greater than file size.
pub const KEEP_SIZE: c_int = 1;

// Create a hole in the file.
pub const PUNCH_HOLE: c_int = 2;

// Remove a range of a file without leaving a hole.
pub const COLLAPSE_RANGE: c_int = 8;

// Convert a range of a file to zeros
pub const ZERO_RANGE: c_int = 16;
