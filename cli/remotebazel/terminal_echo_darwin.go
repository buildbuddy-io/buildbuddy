//go:build darwin

package remotebazel

import (
	"os"

	"golang.org/x/sys/unix"
)

func disableTerminalEcho(f *os.File) (func() error, error) {
	return disableTerminalEchoWithRequests(f, unix.TIOCGETA, unix.TIOCSETA, unix.TIOCSETAF)
}
