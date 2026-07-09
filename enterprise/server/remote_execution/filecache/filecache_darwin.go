//go:build darwin && !ios

package filecache

import "syscall"

func syncFilesystem(path string) error {
	return syscall.Sync()
}
