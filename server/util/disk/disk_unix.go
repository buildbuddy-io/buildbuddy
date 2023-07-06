//go:build (linux || darwin) && !android && !ios

package disk

import "golang.org/x/sys/unix"

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
