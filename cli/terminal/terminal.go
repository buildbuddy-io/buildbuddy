package terminal

import (
	"os"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
)

// ConfigureOutputMode makes sure that bazel produces fancy terminal output.
// This is needed because we tee its output to a file, which makes bazel think
// it is not writing to a terminal, causing it to disable fancy output (ANSI
// colors, dynamic progress indicators, etc.) unless explicitly enabled.
func ConfigureOutputMode(args []string) []string {
	// If bb itself is not writing to a terminal, don't force color/curses.
	o, err := os.Stdout.Stat()
	if err != nil {
		log.Printf("Could not stat stdout; fancy output will not be enabled: %s", err)
		return args
	}
	if (o.Mode() & os.ModeCharDevice) != os.ModeCharDevice {
		return args
	}
	o, err = os.Stderr.Stat()
	if err != nil {
		log.Printf("Could not stat stderr; fancy output will not be enabled: %s", err)
		return args
	}
	if (o.Mode() & os.ModeCharDevice) != os.ModeCharDevice {
		return args
	}

	if !arg.Has(args, "curses") {
		args = append(args, "--curses=yes")
	}
	if !arg.Has(args, "color") {
		args = append(args, "--color=yes")
	}
	return args
}
