//go:build darwin && !ios

package fastcopy

import (
	"errors"

	"golang.org/x/sys/unix"
)

func Clone(source, destination string) error {
	return FastCopy(source, destination)
}

func FastCopy(source, destination string) error {
	if err := unix.Clonefile(source, destination, unix.CLONE_NOFOLLOW); err != nil && !errors.Is(err, unix.EEXIST) {
		return err
	}
	return nil
}
