//go:build darwin && !ios

package fastcopy

import (
	"errors"

	"golang.org/x/sys/unix"
)

func FastCopy(source, destination string) error {
	if err := unix.Clonefile(source, destination, unix.CLONE_NOFOLLOW); err != nil && !errors.Is(err, unix.EEXIST) {
		return err
	}
	return nil
}
