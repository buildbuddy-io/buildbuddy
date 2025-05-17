package terminal

import (
	"os"
	"strconv"
	"strings"

	"github.com/mattn/go-isatty"
)

var (
	// Follow bazel's behavior of only enabling color if both stdout and stderr
	// are a tty.
	colorEnabled = IsTTY(os.Stdout) && IsTTY(os.Stderr)
)

// IsTTY returns whether the given file descriptor is connected to a terminal.
func IsTTY(f *os.File) bool {
	return isatty.IsTerminal(f.Fd())
}

// Esc returns an ANSI escape sequence for the given codes.
// If either stdout or stderr is not a tty, it returns an empty string.
//
// Example:
//
//	terminal.Esc(1, 31) + "Bold red text" + terminal.Esc() + " normal text"
func Esc(codes ...int) string {
	if !colorEnabled {
		return ""
	}
	s := make([]string, 0, len(codes))
	for _, code := range codes {
		s = append(s, strconv.Itoa(code))
	}
	return "\x1b[" + strings.Join(s, ";") + "m"
}
