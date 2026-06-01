//go:build !linux || android

package cachetools

import (
	"io"
	"os"
)

// copyLocalChunkAt copies a known chunk range using offset-based reader and
// writer wrappers so concurrent chunk writes remain safe without opening
// separate destination handles.
func copyLocalChunkAt(dst *os.File, dstOff int64, src *os.File, srcOff int64, size int64) (int64, error) {
	n, err := io.Copy(io.NewOffsetWriter(dst, dstOff), io.NewSectionReader(src, srcOff, size))
	if err != nil {
		return n, err
	}
	if n != size {
		return n, io.ErrShortWrite
	}
	return n, nil
}
