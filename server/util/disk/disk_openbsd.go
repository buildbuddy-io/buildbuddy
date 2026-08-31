//go:build openbsd

package disk

import (
	"os"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sys/unix"
)

func GetDirUsage(path string) (*DirUsage, error) {
	fs := unix.Statfs_t{}
	if err := unix.Statfs(path, &fs); err != nil {
		return nil, err
	}
	blockSize := uint64(fs.F_bsize)
	totalBlocks := uint64(fs.F_blocks)
	freeBlocks := uint64(fs.F_bfree)
	availableBlocks := uint64(fs.F_bavail)
	return &DirUsage{
		TotalBytes: totalBlocks * blockSize,
		UsedBytes:  (totalBlocks - freeBlocks) * blockSize,
		FreeBytes:  freeBlocks * blockSize,
		AvailBytes: availableBlocks * blockSize,
	}, nil
}

// EstimatedFileDiskUsage returns an estimate of the disk usage required for
// the given regular file info.
func EstimatedFileDiskUsage(info os.FileInfo) (int64, error) {
	if !info.Mode().IsRegular() {
		return 0, status.InvalidArgumentError("not a regular file")
	}
	// stat() block units are always 512 bytes.
	return info.Sys().(*syscall.Stat_t).Blocks * 512, nil
}
