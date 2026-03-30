//go:build darwin

package filecache

import "syscall"

func syncFilesystem(path string) error {
	syscall.Sync()
	return nil
}
