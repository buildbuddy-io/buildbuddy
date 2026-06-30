//go:build windows

package filecache

import (
	"path/filepath"

	"golang.org/x/sys/windows"
)

func syncDir(path string) error {
	pathp, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	// FILE_FLAG_BACKUP_SEMANTICS is required to open a directory handle.
	// GENERIC_WRITE avoids the read-only handle that makes FlushFileBuffers
	// fail with ERROR_ACCESS_DENIED on Windows/ReFS.
	handle, err := windows.CreateFile(
		pathp,
		windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)
	return windows.FlushFileBuffers(handle)
}

func syncLockFileCreation(path string) error {
	return syncDir(path)
}

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
