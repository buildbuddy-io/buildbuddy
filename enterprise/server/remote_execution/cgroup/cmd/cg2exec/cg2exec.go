// cg2exec runs a program in a cgroup.
// Only cgroup2 is supported.
// The cgroup2 path is specified as the first argument.
// The program and its arguments are specified as remaining arguments.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
)

func main() {
	// Join the cgroup (first argument)
	cgroup := os.Args[1]
	if err := os.WriteFile(filepath.Join(cgroup, "cgroup.procs"), []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "cg2exec: write cgroup.procs: %s\n", err)
		os.Exit(1)
	}
	args := os.Args[2:]
	// Run the program (remaining arguments) and disappear :o
	if err := syscall.Exec(args[0], args, os.Environ()); err != nil {
		fmt.Fprintf(os.Stderr, "cg2exec: exec: %s\n", err)
		os.Exit(1)
	}
}
