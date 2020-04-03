#define _GNU_SOURCE
#include <stdio.h>
#include <fcntl.h>

int main() {
	printf("use libc::{c_int};\n");
	printf("\n");
	printf("/// Don't extend size of file even if offset + len is greater than file size.\n");
	printf("pub const KEEP_SIZE: c_int = %d;\n", FALLOC_FL_KEEP_SIZE);
	printf("\n");
	printf("/// Create a hole in the file.\n");
	printf("pub const PUNCH_HOLE: c_int = %d;\n", FALLOC_FL_PUNCH_HOLE);
	printf("\n");
	printf("/// Remove a range of a file without leaving a hole.\n");
	printf("pub const COLLAPSE_RANGE: c_int = %d;\n", FALLOC_FL_COLLAPSE_RANGE);
	printf("\n");
	printf("/// Convert a range of a file to zeros\n");
	printf("pub const ZERO_RANGE: c_int = %d;\n", FALLOC_FL_ZERO_RANGE);
	return 0;
}