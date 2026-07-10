//go:build linux && !android

package oomkiller

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

// NewMemoryMonitor returns the default executor memory monitor. cgroupPath is
// the executor's starting cgroup path relative to the cgroupfs root. The
// monitor measures the cgroup's memory usage against its effective memory
// limit. If the cgroup is unknown (empty cgroupPath), does not have the memory
// controller enabled, or has no memory limit, the monitor measures system
// memory usage instead, since the executor is then bounded by system memory.
func NewMemoryMonitor(cgroupPath string) (MemoryMonitor, error) {
	if cgroupPath == "" {
		return newSystemMemoryMonitor()
	}
	dir := filepath.Join(cgroup.RootPath, cgroupPath)
	controllers, err := cgroup.EnabledControllers(dir)
	if err != nil {
		return nil, fmt.Errorf("read enabled controllers for %q: %w", dir, err)
	}
	if !controllers["memory"] {
		log.Infof("Executor cgroup does not have the memory controller enabled; the OOM killer will measure system memory usage.")
		return newSystemMemoryMonitor()
	}
	limitBytes, err := cgroup.ReadEffectiveMemoryLimit(dir)
	if err != nil {
		return nil, fmt.Errorf("read cgroup memory limit: %w", err)
	}
	if limitBytes == nil {
		log.Infof("Executor cgroup has no memory limit; the OOM killer will measure system memory usage.")
		return newSystemMemoryMonitor()
	}
	log.Infof("Executor OOM killer is measuring memory usage of cgroup %q with effective memory limit %d bytes", cgroupPath, *limitBytes)
	return &cgroupMemoryMonitor{
		dir:        dir,
		limitBytes: *limitBytes,
	}, nil
}

// cgroupMemoryMonitor measures executor memory usage from a cgroup.
type cgroupMemoryMonitor struct {
	// dir is the absolute cgroupfs directory of the monitored cgroup.
	dir string
	// limitBytes is the effective memory limit of the monitored cgroup.
	limitBytes int64
}

func (m *cgroupMemoryMonitor) Snapshot(_ context.Context) (*MemorySnapshot, error) {
	usedBytes, err := getCgroupWorkingSetMemoryBytes(m.dir)
	if err != nil {
		return nil, fmt.Errorf("read executor cgroup memory: %w", err)
	}
	return &MemorySnapshot{
		UsedBytes:      usedBytes,
		LimitBytes:     m.limitBytes,
		AvailableBytes: max(int64(0), m.limitBytes-usedBytes),
	}, nil
}

// getCgroupWorkingSetMemoryBytes returns the memory usage of the cgroup at the
// given cgroupfs directory, excluding inactive page cache, which the kernel
// reclaims under memory pressure instead of OOM killing. This matches the
// "working set" definition that the kubelet uses for eviction decisions. See
// https://kubernetes.io/docs/concepts/scheduling-eviction/node-pressure-eviction/#memory-signals
func getCgroupWorkingSetMemoryBytes(dir string) (int64, error) {
	currentBytes, err := cgroup.ReadMemoryCurrent(dir)
	if err != nil {
		return 0, err
	}
	if currentBytes < 0 {
		return 0, fmt.Errorf("cgroup memory.current is negative (%d)", currentBytes)
	}
	inactiveFileBytes, err := cgroup.ReadMemoryStatField(dir, "inactive_file")
	if err != nil {
		return 0, fmt.Errorf("read memory stats: %w", err)
	}
	// memory.current and memory.stat are read separately, so the subtraction
	// can skew slightly negative under concurrent cache growth.
	return max(int64(0), currentBytes-inactiveFileBytes), nil
}
