//go:build !linux && !darwin

package main

import (
	"fmt"
	"runtime"
)

func up(int, string, string) error {
	return fmt.Errorf("the device helper is not supported on %s", runtime.GOOS)
}

func down(string) error {
	return fmt.Errorf("the device helper is not supported on %s", runtime.GOOS)
}
