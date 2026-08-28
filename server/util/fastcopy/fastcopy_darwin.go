//go:build darwin && !ios

package fastcopy

import (
	"errors"
	"os"

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
		if err := os.Link(source, destination); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		return nil
	}
	return Clone(source, destination)
}
