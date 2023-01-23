package rlimit

import (
	"fmt"
	"runtime"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

func MaxRLimit() error {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return fmt.Errorf("Error getting existing open files limit: %s", err.Error())
	}
	if runtime.GOOS == "darwin" && limit.Max > 10240 {
		// The max file limit is 10240, even though
		// the max returned by Getrlimit is 1<<63-1.
		// This is OPEN_MAX in sys/syslimits.h.
		limit.Max = 10240
	}
	if limit.Cur < limit.Max {
		log.Infof("Increasing open files limit %d => %d", limit.Cur, limit.Max)
		limit.Cur = limit.Max
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
			return fmt.Errorf("Error increasing open files limit: %s", err.Error())
		}
	}
	return nil
}

func SetOpenFileDescriptorLimit(n uint64) error {
	log.Infof("Increasing open file descriptor limit to %d", n)
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{Cur: n, Max: n}); err != nil {
		return err
	}
	return nil
}
