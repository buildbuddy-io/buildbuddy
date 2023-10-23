//go:build !darwin
// +build !darwin

package fastcopy

import (
	"os"
)

const (
	OS_WRITE       = 02
	OS_USER_SHIFT  = 6
	OS_GROUP_SHIFT = 3
	OS_OTH_SHIFT   = 0

	OS_USER_W  = OS_WRITE << OS_USER_SHIFT
	OS_GROUP_W = OS_WRITE << OS_GROUP_SHIFT
	OS_OTH_W   = OS_WRITE << OS_OTH_SHIFT

	OS_ALL_W = OS_USER_W | OS_GROUP_W | OS_OTH_W
)

func Writable(fileMode os.FileMode) bool {
	return fileMode.Perm()&OS_ALL_W != 0
}

func FastCopy(source, destination string) error {
	info, err := os.Stat(source)
	if err != nil {
		return err
	}

	// Check IsDir to prevent spurious early errors. The hardlink will
	// fail on Directories anyway when os.Link is attempted below.
	if !info.IsDir() && Writable(info.Mode()) {
		if err := os.Chmod(source, 0444); err != nil {
			return err
		}
	}

	err = os.Link(source, destination)
	if !os.IsExist(err) {
		return err
	}
	return nil
}
