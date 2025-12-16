//go:build windows

package disk

import (
	"fmt"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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

// EstimatedFileDiskUsage returns an estimate of the disk usage required for
// the given regular file info.
func EstimatedFileDiskUsage(info os.FileInfo) (int64, error) {
	if !info.Mode().IsRegular() {
		return 0, status.InvalidArgumentError("not a regular file")
	}
	// TODO: figure out something better for Windows.
	return info.Size(), nil
}
