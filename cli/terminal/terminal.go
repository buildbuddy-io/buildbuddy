package terminal

import (
	"os"
)

// IsTTY returns whether the given file descriptor is connected to a terminal.
func IsTTY(f *os.File) (bool, error) {
	stat, err := f.Stat()
	if err != nil {
		return false, err
	}
	return stat.Mode()&os.ModeCharDevice == os.ModeCharDevice, nil
}
