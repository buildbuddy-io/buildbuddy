//go:build windows

package disk

import (
	"fmt"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sys/windows"
)

// 4KiB cluster size is the default for both NTFS and ReFS(DevDrive)
// References:
//
//	https://techcommunity.microsoft.com/blog/filecab/cluster-size-recommendations-for-refs-and-ntfs/425960
//	https://learn.microsoft.com/en-us/windows-server/storage/file-server/ntfs-overview#support-for-large-volumes
const clusterSize = int64(4096)

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
	// Windows doesn't expose allocated file blocks via os.FileInfo in a
	// cross-platform way. Use a coarse estimate by rounding up to a typical
	// filesystem allocation unit size (4KiB).
	//
	// This is intentionally conservative enough for cache eviction logic which
	// should account for size-on-disk rather than logical content size.
	size := info.Size()
	if size <= 0 {
		return 0, nil
	}
	clusterCount := (size + clusterSize - 1) / clusterSize
	sizeOnDisk := clusterCount * clusterSize
	return sizeOnDisk, nil
}
