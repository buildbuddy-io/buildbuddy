//go:build linux && !android

package oomkiller

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCgroupMemoryMonitorSnapshot(t *testing.T) {
	cgroupDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.current"), []byte("750"), 0644))
	monitor := &cgroupMemoryMonitor{dir: cgroupDir, limitBytes: 1000}

	// The monitor should report usage from the executor cgroup and calculate
	// the remaining headroom relative to the configured executor limit.
	snapshot, err := monitor.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, &MemorySnapshot{UsedBytes: 750, LimitBytes: 1000, AvailableBytes: 250}, snapshot)
}

func TestCgroupMemoryMonitorSnapshot_UsageAboveLimit(t *testing.T) {
	cgroupDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.current"), []byte("1250"), 0644))
	monitor := &cgroupMemoryMonitor{dir: cgroupDir, limitBytes: 1000}

	// Cgroup usage can exceed the executor's assignable memory. Preserve the
	// observed usage so the killer remains over threshold, but clamp the
	// reported headroom to zero.
	snapshot, err := monitor.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, &MemorySnapshot{UsedBytes: 1250, LimitBytes: 1000, AvailableBytes: 0}, snapshot)
}
