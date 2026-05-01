//go:build linux && !android

package disk

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// openAnonymousTmpFile opens a new anonymous temporary file in dir using
// O_TMPFILE. The returned file has no directory entry until linked via
// linkAnonymousTmpFile. If the last reference is closed without linking, the
// inode is reclaimed by the kernel.
//
// ok=false indicates the filesystem at dir does not support O_TMPFILE; the
// caller should fall back to creating a named temp file.
func openAnonymousTmpFile(dir string) (f *os.File, ok bool, err error) {
	fd, err := unix.Open(dir, unix.O_WRONLY|unix.O_TMPFILE|unix.O_CLOEXEC, 0644)
	if err != nil {
		// Fall back to a named temp file when the filesystem doesn't support
		// O_TMPFILE. EOPNOTSUPP is the documented signal; some filesystems
		// (and pre-3.11 kernels) return EISDIR or EINVAL instead.
		if errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.EISDIR) || errors.Is(err, syscall.EINVAL) {
			return nil, false, nil
		}
		return nil, false, &os.PathError{Op: "open", Path: dir, Err: err}
	}
	return os.NewFile(uintptr(fd), filepath.Join(dir, "<O_TMPFILE>")), true, nil
}

// linkAnonymousTmpFile gives an O_TMPFILE-opened file a directory entry at
// linkPath. linkPath must be on the same filesystem as the directory the file
// was opened in, and must not already exist.
//
// We use /proc/self/fd/N + AT_SYMLINK_FOLLOW rather than AT_EMPTY_PATH because
// the latter requires CAP_DAC_READ_SEARCH, which we don't have in firecracker.
func linkAnonymousTmpFile(f *os.File, linkPath string) error {
	src := fmt.Sprintf("/proc/self/fd/%d", f.Fd())
	if err := unix.Linkat(unix.AT_FDCWD, src, unix.AT_FDCWD, linkPath, unix.AT_SYMLINK_FOLLOW); err != nil {
		return &os.LinkError{Op: "linkat", Old: src, New: linkPath, Err: err}
	}
	return nil
}
