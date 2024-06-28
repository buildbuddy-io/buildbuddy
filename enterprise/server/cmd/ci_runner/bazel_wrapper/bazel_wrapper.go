package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/creack/pty"
)

// This is a wrapper to Bazel that appends Bazel flags.
var (
	bazelBinary = flag.String("bazel_bin", "", "The bazel binary to invoke.")
)

func main() {
	flag.Parse()
	fmt.Println("In wrapper script")

	executable := *bazelBinary
	args := os.Args[2:]

	if err := runCommand(executable, args); err != nil {
		log.Fatalf("%s", err.Error())
	}
}

func runCommand(executable string, args []string) error {
	cmd := exec.Command(executable, args...)
	cmd.Env = os.Environ()
	size := &pty.Winsize{Rows: uint16(20), Cols: uint16(114)}
	f, err := pty.StartWithSize(cmd, size)
	if err != nil {
		return err
	}
	defer f.Close()
	copyOutputDone := make(chan struct{})
	go func() {
		io.Copy(os.Stderr, f)
		copyOutputDone <- struct{}{}
	}()
	err = cmd.Wait()
	<-copyOutputDone

	return err
}
