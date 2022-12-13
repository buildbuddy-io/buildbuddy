package terminal

import (
	"os"

	"github.com/mattn/go-isatty"
)

// IsTTY returns whether the given file descriptor is connected to a terminal.
func IsTTY(f *os.File) (bool, error) {
	return isatty.IsTerminal(f.Fd()), nil
}
