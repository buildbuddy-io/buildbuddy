//go:build !linux && !darwin && !windows

package filecache

import "errors"

func syncFilesystem(path string) error {
	return nil
}

func getBootID() (string, error) {
	return "", errors.New("boot ID is not available on this platform")
}
