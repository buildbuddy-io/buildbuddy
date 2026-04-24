//go:build linux && !android

package cachetools

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// copyLocalChunkAt copies a known chunk range using copy_file_range(2).
// Explicit offsets avoid mutating either file's seek position, so this remains
// safe alongside concurrent chunk reads and writes.
func copyLocalChunkAt(dst *os.File, dstOff int64, src *os.File, srcOff int64, size int64) (int64, error) {
	var total int64
	soff, doff := srcOff, dstOff
	for total < size {
		n, err := unix.CopyFileRange(int(src.Fd()), &soff, int(dst.Fd()), &doff, int(size-total), 0)
		if n > 0 {
			total += int64(n)
		}
		if err != nil {
			return total, err
		}
		if n <= 0 {
			break
		}
	}
	if total != size {
		return total, io.ErrShortWrite
	}
	return total, nil
}
