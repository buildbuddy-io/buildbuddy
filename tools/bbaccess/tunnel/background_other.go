//go:build !unix

package tunnel

import (
	"os"
	"syscall"
)

func detachAttr() *syscall.SysProcAttr { return nil }

func lockExclusive(f *os.File) error { return nil }

func lockHeld(f *os.File) (bool, error) { return false, nil }
