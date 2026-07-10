//go:build linux && !android

package oomkiller

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

// NewMemoryMonitor returns the default executor memory monitor. cgroupPath is
// the executor's starting cgroup path relative to the cgroupfs root. The
// monitor measures the cgroup's memory usage if the cgroup memory controller
// is enabled, and system memory usage otherwise. An empty cgroupPath means the
// cgroup is unknown or is the root cgroup, which also means system memory
// usage.
func NewMemoryMonitor(cgroupPath string) (MemoryMonitor, error) {
	if cgroupPath == "" {
		return systemMemoryMonitor{}, nil
	}
	dir := filepath.Join(cgroup.RootPath, cgroupPath)
	controllers, err := cgroup.EnabledControllers(dir)
	if err != nil {
		return nil, fmt.Errorf("read enabled controllers for %q: %w", dir, err)
	}
	if !controllers["memory"] {
		log.Infof("Executor cgroup does not have the memory controller enabled; the OOM killer will measure system memory usage.")
		return systemMemoryMonitor{}, nil
	}
	log.Infof("Executor OOM killer is measuring memory usage of cgroup %q", cgroupPath)
	return &cgroupMemoryMonitor{
		dir:        dir,
		limitBytes: resources.GetAllocatedRAMBytes(),
	}, nil
}

// cgroupMemoryMonitor measures executor memory usage from a cgroup.
type cgroupMemoryMonitor struct {
	// dir is the absolute cgroupfs directory of the monitored cgroup.
	dir string
	// limitBytes is the executor memory limit.
	limitBytes int64
}

func (m *cgroupMemoryMonitor) Snapshot(_ context.Context) (*MemorySnapshot, error) {
	usedBytes, err := cgroup.ReadMemoryCurrent(m.dir)
	if err != nil {
		return nil, fmt.Errorf("read executor cgroup memory: %w", err)
	}
	if usedBytes < 0 {
		return nil, fmt.Errorf("cgroup memory.current is negative (%d)", usedBytes)
	}
	return &MemorySnapshot{
		UsedBytes:      usedBytes,
		LimitBytes:     m.limitBytes,
		AvailableBytes: max(int64(0), m.limitBytes-usedBytes),
	}, nil
}
