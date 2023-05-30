package terminal

import (
	"os"

	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/mattn/go-isatty"
)

// IsTTY returns whether the given file descriptor is connected to a terminal.
func IsTTY(f *os.File) bool {
	return isatty.IsTerminal(f.Fd())
}

func AddTerminalFlags(args []string) []string {
	_, idx := parser.GetBazelCommandAndIndex(args)
	if idx == -1 {
		return args
	}
	tArgs := []string{"--curses=yes", "--color=yes"}
	a := make([]string, 0, len(args)+len(tArgs))
	a = append(a, args[:idx+1]...)
	a = append(a, tArgs...)
	a = append(a, args[idx+1:]...)
	return a
}
