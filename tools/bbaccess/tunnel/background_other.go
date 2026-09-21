//go:build !unix

package tunnel

import (
	"os"
	"syscall"
)

func detachAttr() *syscall.SysProcAttr { return nil }

func processAlive(pid int) bool {
	_, err := os.FindProcess(pid)
	return err == nil
}
