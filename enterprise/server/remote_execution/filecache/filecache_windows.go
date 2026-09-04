//go:build windows

package filecache

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

// ctimeUsec returns the file's creation time in microseconds since the Unix
// epoch. Windows has no inode change time, and the creation time is the
// closest analog for approximating when the file was placed in the cache.
func ctimeUsec(info os.FileInfo) (int64, bool) {
	attrs, ok := info.Sys().(*syscall.Win32FileAttributeData)
	if !ok {
		return 0, false
	}
	return attrs.CreationTime.Nanoseconds() / 1000, true
}

// Buffer lengths for the Windows volume path APIs are expressed in UTF-16
// code units. See https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getvolumepathnamew.
const volumePathBufferLength = windows.MAX_PATH + 1

// Windows does not expose a supported equivalent of fsync for directories, so
// flush the whole volume to make directory metadata changes durable.
func syncDir(path string) error {
	return syncFilesystem(path)
}

func syncFilesystem(path string) error {
	volumePath, err := volumeDevicePath(path)
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

// volumeDevicePath returns the volume GUID path for the volume containing
// path. Resolving the containing volume is important when path is below a
// volume mount point rather than directly below a drive letter.
func volumeDevicePath(path string) (*uint16, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	var mountPoint [volumePathBufferLength]uint16
	if err := windows.GetVolumePathName(pathPtr, &mountPoint[0], uint32(len(mountPoint))); err != nil {
		return nil, fmt.Errorf("get volume path for %q: %w", path, err)
	}
	var volumeName [volumePathBufferLength]uint16
	if err := windows.GetVolumeNameForVolumeMountPoint(&mountPoint[0], &volumeName[0], uint32(len(volumeName))); err != nil {
		return nil, fmt.Errorf("get volume name for %q: %w", path, err)
	}
	return windows.UTF16PtrFromString(strings.TrimSuffix(windows.UTF16ToString(volumeName[:]), `\`))
}

// getBootID returns an identifier that is unique to the current boot session.
// Windows does not expose a boot session UUID, so use the BootId counter,
// which the kernel increments on each boot.
func getBootID() (string, error) {
	k, err := registry.OpenKey(registry.LOCAL_MACHINE, `SYSTEM\CurrentControlSet\Control\Session Manager\Memory Management\PrefetchParameters`, registry.QUERY_VALUE)
	if err != nil {
		return "", fmt.Errorf("open PrefetchParameters registry key: %w", err)
	}
	defer k.Close()
	bootID, _, err := k.GetIntegerValue("BootId")
	if err != nil {
		return "", fmt.Errorf("read BootId registry value: %w", err)
	}
	return strconv.FormatUint(bootID, 10), nil
}
