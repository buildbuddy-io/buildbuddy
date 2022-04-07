// Test binary for commandutil_test
package main

import (
	"fmt"
	"os"
	"syscall"
	"time"
)

func main() {
	// Have this binary become its own process group leader
	check(syscall.Setpgid(0, os.Getpid()))
	_, err := os.Stdout.Write([]byte("stdout\n"))
	check(err)
	_, err = os.Stderr.Write([]byte("stderr\n"))
	check(err)
	time.Sleep(100 * time.Second)
}

func check(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
