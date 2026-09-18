//go:build linux && !android

package filecache

import (
	"os"
	"strings"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sys/unix"
)

var (
	// Tests replace this syscall to simulate missing birth time support and
	// verify that an unsupported probe prevents subsequent statx calls.
	statx = unix.Statx
)

// getStatFunc probes rootDir and returns a metadata reader using statx with
// birth time when supported, or ordinary stat with ctime otherwise.
func getStatFunc(rootDir string) func(string) (fileMetadata, error) {
	var st unix.Statx_t
	if err := statx(unix.AT_FDCWD, rootDir, unix.AT_STATX_SYNC_AS_STAT, unix.STATX_BASIC_STATS|unix.STATX_BTIME, &st); err != nil || st.Mask&unix.STATX_BTIME == 0 {
		// Fall back to ordinary stat so the cache still works on older kernels
		// without statx (ENOSYS) and in sandboxes that block it (EPERM).
		// If statx succeeds but doesn't return birth time, ordinary stat is
		// sufficient for the ctime fallback.
		return statFile
	}
	return statxFile
}

func statxFile(path string) (fileMetadata, error) {
	var st unix.Statx_t
	if err := statx(unix.AT_FDCWD, path, unix.AT_STATX_SYNC_AS_STAT, unix.STATX_BASIC_STATS|unix.STATX_BTIME, &st); err != nil {
		return fileMetadata{}, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		return fileMetadata{}, status.InvalidArgumentError("not a regular file")
	}
	timestamp := st.Ctime
	// Individual inodes can lack birth time even when the filesystem supports
	// it, for example after upgrading an older filesystem. Ctime is already
	// present in this result, so falling back requires no additional syscall.
	if st.Mask&unix.STATX_BTIME != 0 {
		timestamp = st.Btime
	}
	return fileMetadata{
		sizeBytes:     int64(st.Blocks) * 512,
		timestampUsec: timestamp.Sec*1_000_000 + int64(timestamp.Nsec)/1_000,
	}, nil
}

// ctimeUsec returns the file's inode change time in microseconds since the
// Unix epoch.
func ctimeUsec(info os.FileInfo) (int64, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return int64(st.Ctim.Sec)*1_000_000 + int64(st.Ctim.Nsec)/1_000, true
}

// SetStatxForTest replaces the Linux metadata syscall and returns a function
// that restores it. Tests using this hook must not run in parallel.
func SetStatxForTest(fn func(int, string, int, int, *unix.Statx_t) error) func() {
	previous := statx
	statx = fn
	return func() { statx = previous }
}

func syncFilesystem(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return unix.Syncfs(int(dir.Fd()))
}

// getBootID returns an identifier that is unique to the current boot session.
func getBootID() (string, error) {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
