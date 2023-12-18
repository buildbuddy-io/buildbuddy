package log

import (
	"log"
	"os"
)

const (
	verboseEnvVarName = "BB_VERBOSE"

	debugPrefix   = "\x1b[33m[bb-debug]\x1b[m "
	WarningPrefix = "\x1b[33mWarning:\x1b[m "
)

var verbose bool

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

func Debugf(format string, v ...interface{}) {
	if !verbose {
		return
	}
	log.Printf(debugPrefix+format, v...)
}

func Print(v ...any) {
	log.Print(v...)
}

func Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func Warn(v ...any) {
	log.Print(append([]any{WarningPrefix}, v...)...)
}

func Warnf(format string, v ...interface{}) {
	log.Printf(WarningPrefix+format, v...)
}

func Fatalf(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}

func Fatal(v ...any) {
	log.Fatal(v...)
}
