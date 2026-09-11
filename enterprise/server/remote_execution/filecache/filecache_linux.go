//go:build linux && !android

package filecache

import (
	"os"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// ctimeUsec returns the file's inode change time in microseconds since the
// Unix epoch.
func ctimeUsec(info os.FileInfo) (int64, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return st.Ctim.Nano() / 1000, true
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
