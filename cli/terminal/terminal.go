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
	//
	// Note: changing this at runtime is not advised, as other parts of the
	// codebase assume this doesn't change and may cache the result.
	ansiStylesEnabled = IsTTY(os.Stdout) && IsTTY(os.Stderr)
)

// IsTTY returns whether the given file descriptor is connected to a terminal.
func IsTTY(f *os.File) bool {
	return isatty.IsTerminal(f.Fd())
}

// Esc returns an ANSI escape sequence for the given codes.
// If either stdout or stderr is not a tty, it returns an empty string.
// It is intended only for text styling, where dropping the escape sequence
// still produces usable text content.
//
// Example:
//
//	terminal.Esc(1, 31) + "Bold red text" + terminal.Esc() + " normal text"
func Esc(codes ...int) string {
	if !ansiStylesEnabled {
		return ""
	}
	strs := make([]string, 0, len(codes))
	for _, code := range codes {
		// Missing numbers are treated as 0, and some sequences may treat
		// missing values as meaningful, so coerce 0 to empty string so that
		// missing values can be represented.
		if code == 0 {
			strs = append(strs, "")
		} else {
			strs = append(strs, strconv.Itoa(code))
		}
	}
	return "\x1b[" + strings.Join(strs, ";") + "m"
}
