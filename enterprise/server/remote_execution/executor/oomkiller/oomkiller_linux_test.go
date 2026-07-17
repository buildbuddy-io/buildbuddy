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
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.stat"), []byte("anon 500\ninactive_file 200\n"), 0644))
	monitor := &cgroupMemoryMonitor{dir: cgroupDir, limitBytes: 1000}

	// The monitor should report usage from the executor cgroup, excluding
	// inactive page cache since the kernel reclaims it under memory pressure,
	// and calculate the remaining headroom relative to the cgroup memory
	// limit.
	snapshot, err := monitor.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, &MemorySnapshot{UsedBytes: 550, LimitBytes: 1000, AvailableBytes: 450}, snapshot)
}

func TestCgroupMemoryMonitorSnapshot_InactiveFileAboveCurrent(t *testing.T) {
	cgroupDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.current"), []byte("100"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.stat"), []byte("inactive_file 150\n"), 0644))
	monitor := &cgroupMemoryMonitor{dir: cgroupDir, limitBytes: 1000}

	// memory.current and memory.stat are read separately, so inactive page
	// cache can momentarily exceed the earlier memory.current reading. Clamp
	// the reported usage to zero instead of going negative.
	snapshot, err := monitor.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, &MemorySnapshot{UsedBytes: 0, LimitBytes: 1000, AvailableBytes: 1000}, snapshot)
}

func TestCgroupMemoryMonitorSnapshot_NegativeUsage(t *testing.T) {
	cgroupDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.current"), []byte("-5"), 0644))
	monitor := &cgroupMemoryMonitor{dir: cgroupDir, limitBytes: 1000}

	// The kernel never reports negative memory usage, so a negative value
	// means the read went wrong somehow. Report an error instead of feeding a
	// bogus measurement to the killer.
	_, err := monitor.Snapshot(t.Context())
	require.ErrorContains(t, err, "negative")
}

func TestCgroupMemoryMonitorSnapshot_UsageAboveLimit(t *testing.T) {
	cgroupDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.current"), []byte("1250"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.stat"), []byte("inactive_file 100\n"), 0644))
	monitor := &cgroupMemoryMonitor{dir: cgroupDir, limitBytes: 1000}

	// If usage somehow exceeds the memory limit, preserve the observed usage
	// so the killer remains over threshold, but clamp the reported headroom to
	// zero.
	snapshot, err := monitor.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, &MemorySnapshot{UsedBytes: 1150, LimitBytes: 1000, AvailableBytes: 0}, snapshot)
}
