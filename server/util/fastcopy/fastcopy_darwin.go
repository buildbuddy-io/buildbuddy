//go:build darwin && !ios

package fastcopy

import (
	"errors"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sys/unix"
)

func reflink(source, destination string) error {
	return status.UnimplementedErrorf("reflinking not supported on darwin")
}

func FastCopy(source, destination string) error {
	if err := unix.Clonefile(source, destination, unix.CLONE_NOFOLLOW); err != nil && !errors.Is(err, unix.EEXIST) {
		return err
	}
	return nil
}
