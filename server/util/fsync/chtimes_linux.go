//go:build linux

package fsync

import (
	"os"
	"time"

	"golang.org/x/sys/unix"
)

func (r *Root) chtimesFile(f *os.File, path string, mtime time.Time) error {
	ts, err := unix.TimeToTimespec(mtime)
	if err != nil {
		return err
	}
	// Linux 5.8+ supports setting nanosecond timestamps through an existing
	// descriptor, without any additional path lookups or opens.
	err = unix.UtimesNanoAt(int(f.Fd()), "", []unix.Timespec{ts, ts}, unix.AT_EMPTY_PATH)
	if err == unix.EINVAL || err == unix.ENOENT {
		// Preserve support for older kernels that do not accept AT_EMPTY_PATH.
		return r.root.Chtimes(path, mtime, mtime)
	}
	if err != nil {
		return &os.PathError{Op: "utimensat", Path: path, Err: err}
	}
	return nil
}
