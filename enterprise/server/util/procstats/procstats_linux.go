//go:build linux

package procstats

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/tklauser/go-sysconf"
)

var (
	clockTicksOnce sync.Once
	clockTicksHz   int64
)

// clockTicks returns sysconf(_SC_CLK_TCK), the unit of the CPU time fields
// in /proc/<pid>/stat (USER_HZ; 100 on all mainstream architectures, 1024 on
// Alpha). go-sysconf answers this in pure Go from the AT_CLKTCK auxv entry;
// it is the same call gopsutil makes.
func clockTicks() int64 {
	clockTicksOnce.Do(func() {
		clockTicksHz = 100
		if v, err := sysconf.Sysconf(sysconf.SC_CLK_TCK); err == nil && v > 0 {
			clockTicksHz = v
		}
	})
	return clockTicksHz
}

// getProcessStats reads CPU time and resident memory of a single process
// straight from procfs. This replaces gopsutil's process.NewProcess: its
// Times() parses utime/stime from /proc/<pid>/stat exactly as below, and its
// MemoryInfo() reads the resident page count from /proc/<pid>/statm; field 24
// of stat is the same kernel RSS counter, so one read serves both.
func getProcessStats(pid int) (*repb.UsageStats, error) {
	return parseProcStat(fmt.Sprintf("/proc/%d/stat", pid), clockTicks(), int64(os.Getpagesize()))
}

func parseProcStat(path string, ticksPerSecond, pageSize int64) (*repb.UsageStats, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseProcStatData(data, ticksPerSecond, pageSize)
}

// parseProcStatData parses the contents of /proc/<pid>/stat (see proc(5)).
// The second field (comm) is in parentheses and may contain spaces or ')',
// so fields are counted from after the last ')'.
func parseProcStatData(data []byte, ticksPerSecond, pageSize int64) (*repb.UsageStats, error) {
	i := bytes.LastIndexByte(data, ')')
	if i < 0 {
		return nil, fmt.Errorf("malformed /proc stat: %q", data)
	}
	// Fields after comm, 0-indexed: [0]=state (field 3), so field N is index N-3.
	fields := strings.Fields(string(data[i+1:]))
	const (
		utimeField = 14 - 3
		stimeField = 15 - 3
		rssField   = 24 - 3
	)
	if len(fields) <= rssField {
		return nil, fmt.Errorf("malformed /proc stat: %d fields after comm", len(fields))
	}
	utime, err := strconv.ParseInt(fields[utimeField], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse utime: %w", err)
	}
	stime, err := strconv.ParseInt(fields[stimeField], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse stime: %w", err)
	}
	rssPages, err := strconv.ParseInt(fields[rssField], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse rss: %w", err)
	}
	// TODO(bduffany): Explore using PSS instead of RSS to avoid overcounting
	// shared library memory usage.
	return &repb.UsageStats{
		MemoryBytes: rssPages * pageSize,
		CpuNanos:    (utime + stime) * 1_000_000_000 / ticksPerSecond,
	}, nil
}
