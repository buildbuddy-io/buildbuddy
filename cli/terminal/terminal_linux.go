//go:build linux && !android

package terminal

import (
	"os"

	"golang.org/x/sys/unix"
)

func disableEcho(f *os.File) (func() error, error) {
	return disableEchoWithRequests(f, unix.TCGETS, unix.TCSETS, unix.TCSETSF)
}
