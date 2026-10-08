//go:build !linux

package fsync

import (
	"os"
	"time"
)

func (r *Root) chtimesFile(_ *os.File, path string, mtime time.Time) error {
	return r.root.Chtimes(path, mtime, mtime)
}
