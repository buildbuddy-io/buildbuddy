//go:build !linux && !darwin && !windows

package filecache

import (
	"errors"
	"os"
)

// ctimeUsec returns the file's inode change time, which is unavailable on
// this platform.
func ctimeUsec(info os.FileInfo) (int64, bool) {
	return 0, false
}

func syncDir(path string) error {
	return nil
}

func syncFilesystem(path string) error {
	return nil
}

func getBootID() (string, error) {
	return "", errors.New("boot ID is not available on this platform")
}
