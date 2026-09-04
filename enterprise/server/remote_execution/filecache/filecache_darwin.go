//go:build darwin && !ios

package filecache

import (
	"fmt"
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
	return st.Ctimespec.Nano() / 1000, true
}

func syncFilesystem(path string) error {
	return syscall.Sync()
}

// getBootID returns an identifier that is unique to the current boot session.
func getBootID() (string, error) {
	uuid, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil {
		return "", fmt.Errorf("sysctl kern.bootsessionuuid: %w", err)
	}
	return strings.TrimSpace(uuid), nil
}
