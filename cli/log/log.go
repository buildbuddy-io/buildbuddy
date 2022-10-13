package log

import (
	"log"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
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
	log.Print(v...)
}

func Debugf(format string, v ...interface{}) {
	if !verbose {
		return
	}
	log.Printf(format, v...)
}

func Print(v ...any) {
	log.Print(v...)
}

func Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func Fatalf(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}

func Fatal(v ...any) {
	log.Fatal(v...)
}
