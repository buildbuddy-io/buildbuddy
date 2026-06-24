//go:build linux

package remotebazel

import (
	"os"

	"golang.org/x/sys/unix"
)

func disableTerminalEcho(f *os.File) (func() error, error) {
	return disableTerminalEchoWithRequests(f, unix.TCGETS, unix.TCSETS, unix.TCSETSF)
}
