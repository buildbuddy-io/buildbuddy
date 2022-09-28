package logging

import (
	"flag"
	"log"
)

var (
	verbose = flag.Bool("bb_verbose", false, "If true, enable verbose buildbuddy logging.")
)

func Printf(format string, v ...interface{}) {
	if !*verbose {
		return
	}
	log.Printf(format, v...)
}
