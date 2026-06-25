//go:build !darwin && !linux

package remotebazel

import (
	"fmt"
	"os"
)

func disableTerminalEcho(f *os.File) (func() error, error) {
	return nil, fmt.Errorf("terminal echo control is unsupported on this platform")
}
