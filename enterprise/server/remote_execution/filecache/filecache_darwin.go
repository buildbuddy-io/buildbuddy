//go:build darwin && !ios

package filecache

import "syscall"

func syncFilesystem(path string) error {
	syscall.Sync()
	return nil
}
