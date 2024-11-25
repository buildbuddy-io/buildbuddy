//go:build (linux || darwin) && !android && !ios

package block_io

import (
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// Device represents a block device.
type Device struct {
	// Major and Minor device numbers.
	Maj, Min int64
}

// LookupDevice returns the block device for the filesystem mounted at
// the given path. The path must exist.
func LookupDevice(path string) (*Device, error) {
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err != nil {
		return nil, err
	}

	major := unix.Major(uint64(stat.Dev))
	minor := unix.Minor(uint64(stat.Dev))
	blkDev := &Device{
		Maj: int64(major),
		Min: int64(minor),
	}

	return blkDev, nil
}

// ParseMajMin parses major/minor block device numbers formatted like
// "MAJ:MIN"
func ParseMajMin(str string) (major, minor int, _ error) {
	majStr, minStr, ok := strings.Cut(str, ":")
	if !ok {
		return 0, 0, fmt.Errorf("expected MAJ:MIN device numbers, got %q", str)
	}
	major, err := strconv.Atoi(majStr)
	if err != nil {
		return 0, 0, fmt.Errorf("malformed major device number")
	}
	minor, err = strconv.Atoi(minStr)
	if err != nil {
		return 0, 0, fmt.Errorf("malformed minor device number")
	}
	return major, minor, nil
}

func (dev *Device) String() string {
	return fmt.Sprintf("%d:%d", dev.Maj, dev.Min)
}
