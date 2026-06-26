//go:build darwin && !ios

package terminal

import (
	"os"

	"golang.org/x/sys/unix"
)

func disableEcho(f *os.File) (func() error, error) {
	return disableEchoWithRequests(f, unix.TIOCGETA, unix.TIOCSETA, unix.TIOCSETAF)
}
