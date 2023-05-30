//go:build windows

package disk

import (
	"fmt"

	"golang.org/x/sys/windows"
)

func GetDirUsage(path string) (*DirUsage, error) {
	cwd, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, fmt.Errorf("failed to call UTF16PtrFromString: %v", err)
	}

	var freeBytesAvailableToCaller, totalNumberOfBytes, totalNumberOfFreeBytes uint64
	if err := windows.GetDiskFreeSpaceEx(cwd, &freeBytesAvailableToCaller, &totalNumberOfBytes, &totalNumberOfFreeBytes); err != nil {
		return nil, err
	}
	return &DirUsage{
		TotalBytes: totalNumberOfBytes,
		UsedBytes:  totalNumberOfBytes - totalNumberOfFreeBytes,
		FreeBytes:  totalNumberOfFreeBytes,
		AvailBytes: freeBytesAvailableToCaller,
	}, nil
}
