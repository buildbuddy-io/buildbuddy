//go:build !linux

package procstats

import (
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	procutil "github.com/shirou/gopsutil/v3/process"
)

func getProcessStats(pid int) (*repb.UsageStats, error) {
	p, err := procutil.NewProcess(int32(pid))
	if err != nil {
		return nil, err
	}
	t, err := p.Times()
	if err != nil {
		return nil, err
	}
	m, err := p.MemoryInfo()
	if err != nil {
		return nil, err
	}
	// TODO(bduffany): Explore using PSS instead of RSS to avoid overcounting
	// shared library memory usage.
	return &repb.UsageStats{
		MemoryBytes: int64(m.RSS),
		CpuNanos:    int64((t.User + t.System) * 1e9),
	}, nil
}
