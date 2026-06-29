//go:build !darwin && !linux && !ios && !android

package terminal

import (
	"fmt"
	"os"
)

func disableEcho(f *os.File) (func() error, error) {
	return nil, fmt.Errorf("terminal echo control is unsupported on this platform")
}
