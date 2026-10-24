package log

import (
	"log"
	"os"

	"github.com/buildbuddy-io/buildbuddy/cli/terminal"
)

const (
	verboseEnvVarName = "BB_VERBOSE"
)

var (
	debugPrefix   = terminal.Esc(33) + "[bb-debug]" + terminal.Esc() + " "
	WarningPrefix = terminal.Esc(33) + "Warning:" + terminal.Esc() + " "
)

var (
	verbose bool
	// Suppresses normal-level output, leaving warnings and errors. Warnings
	// stay because a quiet run still has to say when something went wrong.
	quiet bool
)

// Configure reads the verbose flag from the args in order to configure the logs.
// Removes the verbose flag from the output args.
func Configure(verboseFlagVal string) {
	if verboseFlagVal == "" {
		verboseFlagVal = os.Getenv(verboseEnvVarName)
	}
	verbose = verboseFlagVal == "1" || verboseFlagVal == "true"
	if verbose {
		// Propagate the flag value to nested invocations (via env var)
		os.Setenv(verboseEnvVarName, "1")
	}
	log.SetFlags(0)
}

func Debug(v ...any) {
	if !verbose {
		return
	}
	log.Print(append([]any{debugPrefix}, v...)...)
}

func Debugf(format string, v ...any) {
	if !verbose {
		return
	}
	log.Printf(debugPrefix+format, v...)
}

// SetQuiet suppresses normal-level output.
func SetQuiet(q bool) {
	quiet = q
}

func Print(v ...any) {
	if quiet {
		return
	}
	log.Print(v...)
}

func Printf(format string, v ...any) {
	if quiet {
		return
	}
	log.Printf(format, v...)
}

func Warn(v ...any) {
	log.Print(append([]any{WarningPrefix}, v...)...)
}

func Warnf(format string, v ...any) {
	log.Printf(WarningPrefix+format, v...)
}

func Fatalf(format string, v ...any) {
	log.Fatalf(format, v...)
}

func Fatal(v ...any) {
	log.Fatal(v...)
}
