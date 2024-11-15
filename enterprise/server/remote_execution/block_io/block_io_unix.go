//go:build (linux || darwin) && !android && !ios

package block_io

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// Device represents a block device.
type Device struct {
	// Major and Minor device numbers.
	Maj, Min int64
}

// LookupBlockDevice returns the block device for the filesystem mounted at
// the given path.
func LookupDevice(path string) (*Device, error) {
	// Get the device like /dev/sda1 etc.
	dev, err := getDeviceFromPath(path)
	if err != nil {
		return nil, err
	}

	// Stat the device to get Rdev, then extract major/minor device nums.
	fileInfo, err := os.Stat(dev)
	if err != nil {
		return nil, fmt.Errorf("stat %q: %w", dev, err)
	}
	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, err
	}
	major := unix.Major(uint64(stat.Rdev))
	minor := unix.Minor(uint64(stat.Rdev))
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

// getDeviceFromPath finds the mountpoint associated with the given path
// and returns the block device mounted there.
func getDeviceFromPath(path string) (string, error) {
	file, err := os.Open("/proc/mounts")
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var mountPoint string
	var device string
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		mountDir := fields[1]
		if strings.HasPrefix(path, mountDir) && len(mountDir) > len(mountPoint) && fields[0] != "(unknown)" {
			mountPoint = mountDir
			device = fields[0]
		}
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}
	if device == "" {
		return "", fmt.Errorf("could not find mount point for directory")
	}
	return device, nil
}
