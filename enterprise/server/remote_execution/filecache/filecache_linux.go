//go:build linux && !android

package filecache

import (
	"os"

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
