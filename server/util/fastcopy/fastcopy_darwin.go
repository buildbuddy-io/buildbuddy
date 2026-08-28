//go:build darwin && !ios

package fastcopy

import (
	"errors"

	"golang.org/x/sys/unix"
)

func Clone(source, destination string) error {
	if err := unix.Clonefile(source, destination, unix.CLONE_NOFOLLOW); err != nil && !errors.Is(err, unix.EEXIST) {
		return err
	}
	return nil
}

func FastCopy(source, destination string) error {
	if *useMacOSHardlinks {
		if err := unix.Linkat(unix.AT_FDCWD, source, unix.AT_FDCWD, destination, 0); err != nil && !errors.Is(err, unix.EEXIST) {
			return err
		}
		return nil
	}
	return Clone(source, destination)
}
