//go:build darwin && !ios

package fastcopy

import (
	"golang.org/x/sys/unix"
)

func Clone(source, destination string) error {
	return FastCopy(source, destination)
}

func FastCopy(source, destination string) error {
	return unix.Clonefile(source, destination, unix.CLONE_NOFOLLOW)
}
