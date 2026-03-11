package main

import (
	"fmt"
	"os"

	"github.com/buildbuddy-io/buildbuddy/cli/analyze_profile"
)

func main() {
	exitCode, err := analyze_profile.HandleAnalyzeProfile(os.Args[1:])
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		if exitCode < 0 {
			exitCode = 1
		}
	}
	os.Exit(exitCode)
}
