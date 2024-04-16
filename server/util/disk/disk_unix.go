//go:build (linux || darwin) && !android && !ios

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
	return &DirUsage{
		TotalBytes: fs.Blocks * uint64(fs.Bsize),
		UsedBytes:  (fs.Blocks - fs.Bfree) * uint64(fs.Bsize),
		FreeBytes:  fs.Bfree * uint64(fs.Bsize),
		AvailBytes: fs.Bavail * uint64(fs.Bsize),
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
