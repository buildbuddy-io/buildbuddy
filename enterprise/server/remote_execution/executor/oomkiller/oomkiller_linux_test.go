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
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.stat"), []byte("anon 500\ninactive_file 120\nactive_file 80\n"), 0644))
	monitor := &cgroupMemoryMonitor{dir: cgroupDir, limitBytes: 1000}

	// The monitor should report usage from the executor cgroup, excluding both
	// the inactive and active file page cache since the kernel reclaims file
	// pages under memory pressure, and calculate the remaining headroom
	// relative to the cgroup memory limit.
	snapshot, err := monitor.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, &MemorySnapshot{UsedBytes: 550, LimitBytes: 1000, AvailableBytes: 450}, snapshot)
}

func TestCgroupMemoryMonitorSnapshot_FileCacheAboveCurrent(t *testing.T) {
	cgroupDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.current"), []byte("100"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.stat"), []byte("inactive_file 90\nactive_file 60\n"), 0644))
	monitor := &cgroupMemoryMonitor{dir: cgroupDir, limitBytes: 1000}

	// memory.current and memory.stat are read separately, so the file page
	// cache can momentarily exceed the earlier memory.current reading. Clamp
	// the reported usage to zero instead of going negative.
	snapshot, err := monitor.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, &MemorySnapshot{UsedBytes: 0, LimitBytes: 1000, AvailableBytes: 1000}, snapshot)
}

func TestCgroupMemoryMonitorSnapshot_MissingFileFields(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		memoryStat   string
		missingField string
	}{
		{name: "missing inactive_file", memoryStat: "anon 500\nactive_file 80\n", missingField: "inactive_file"},
		{name: "missing active_file", memoryStat: "anon 500\ninactive_file 120\n", missingField: "active_file"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cgroupDir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.current"), []byte("750"), 0644))
			require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.stat"), []byte(testCase.memoryStat), 0644))
			monitor := &cgroupMemoryMonitor{dir: cgroupDir, limitBytes: 1000}

			// The kernel always reports both file LRU fields, so a missing field
			// means the read went wrong somehow. Report an error instead of
			// silently treating the missing cache as used.
			_, err := monitor.Snapshot(t.Context())
			require.ErrorContains(t, err, testCase.missingField)
		})
	}
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
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.stat"), []byte("inactive_file 60\nactive_file 40\n"), 0644))
	monitor := &cgroupMemoryMonitor{dir: cgroupDir, limitBytes: 1000}

	// If usage somehow exceeds the memory limit, preserve the observed usage
	// so the killer remains over threshold, but clamp the reported headroom to
	// zero.
	snapshot, err := monitor.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, &MemorySnapshot{UsedBytes: 1150, LimitBytes: 1000, AvailableBytes: 0}, snapshot)
}
