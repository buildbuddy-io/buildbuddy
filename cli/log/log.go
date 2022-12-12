package log

import (
	"log"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
)

const (
	debugPrefix   = "\x1b[33m[bb-debug]\x1b[m "
	WarningPrefix = "\x1b[33mWarning:\x1b[m "
)

var verbose bool

func Configure(args []string) []string {
	verboseFlagVal, args := arg.Pop(args, "verbose")
	verbose = verboseFlagVal == "1" || verboseFlagVal == "true"
	log.SetFlags(0)
	return args
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
