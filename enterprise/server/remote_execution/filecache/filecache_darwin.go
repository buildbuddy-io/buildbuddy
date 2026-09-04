//go:build darwin && !ios

package filecache

import (
	"fmt"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

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
