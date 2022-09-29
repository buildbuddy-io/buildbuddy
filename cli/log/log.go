package log

import (
	"flag"
	"log"
)

var (
	verbose = flag.Bool("verbose", false, "If true, enable verbose buildbuddy logging.")
)

func init() {
	log.SetFlags(0)
}

func Debugf(format string, v ...interface{}) {
	if !*verbose {
		return
	}
	log.Printf(format, v...)
}

func Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func Fatalf(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}

func Fatal(format string) {
	log.Fatal(format)
}
