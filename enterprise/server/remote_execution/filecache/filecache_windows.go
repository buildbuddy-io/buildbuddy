//go:build windows

package filecache

import (
	"fmt"
	"path/filepath"
	"strconv"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
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
