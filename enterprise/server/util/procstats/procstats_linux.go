//go:build linux

package procstats

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// clockTicks is USER_HZ, the unit of the CPU time fields in
// /proc/<pid>/stat. It is part of the Linux userspace ABI and is 100 on
// every architecture regardless of the kernel's internal HZ (gopsutil
// hardcodes the same value).
const clockTicks = 100

// getProcessStats reads CPU time and resident memory of a single process
// straight from procfs. This replaces gopsutil's process.NewProcess, whose
// Times() and MemoryInfo() do exactly this (parse /proc/<pid>/stat) but drag
// in the whole gopsutil dependency tree.
func getProcessStats(pid int) (*repb.UsageStats, error) {
	return parseProcStat(fmt.Sprintf("/proc/%d/stat", pid), clockTicks, int64(os.Getpagesize()))
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
		CpuNanos:    (utime + stime) * (1e9 / ticksPerSecond),
	}, nil
}
