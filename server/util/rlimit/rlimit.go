package rlimit

import (
	"fmt"
	"log"
	"syscall"
)

func MaxRLimit() error {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return fmt.Errorf("Error getting existing open files limit: %s", err.Error())
	}
	if limit.Cur != limit.Max {
		log.Printf("Increasing open files limit %d => %d", limit.Cur, limit.Max)
		limit.Cur = limit.Max
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
			return fmt.Errorf("Error increasing open files limit: %s", err.Error())
		}
	}
	return nil
}
