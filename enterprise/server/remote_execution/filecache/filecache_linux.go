//go:build linux && !android

package filecache

import (
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

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
