//go:build windows

package filecache

import (
	"path/filepath"

	"golang.org/x/sys/windows"
)

func syncFilesystem(path string) error {
	vol := filepath.VolumeName(path)
	volumePath, err := windows.UTF16PtrFromString(`\\.\` + vol)
	if err != nil {
		return err
	}
	handle, err := windows.CreateFile(
		volumePath,
		windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		0,
		0,
	)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)
	return windows.FlushFileBuffers(handle)
}
