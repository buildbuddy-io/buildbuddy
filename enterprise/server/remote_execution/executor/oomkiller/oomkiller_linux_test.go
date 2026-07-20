//go:build linux && !android

package oomkiller

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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

func TestCgroupMemoryMonitorSnapshot_MemoryPressure(t *testing.T) {
	cgroupDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.current"), []byte("750"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.stat"), []byte("inactive_file 200\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.pressure"), []byte("some avg10=12.50 avg60=5.00 avg300=1.00 total=250000\nfull avg10=2.50 avg60=1.00 avg300=0.50 total=50000\n"), 0644))
	flags.Set(t, "executor.oom_killer.memory_pressure_some_stall_threshold", 0.2)
	monitor := &cgroupMemoryMonitor{dir: cgroupDir, limitBytes: 1000}

	// The cgroup monitor should return the cumulative some- and full-stall
	// counters that the killer uses to calculate pressure between polls.
	snapshot, err := monitor.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, &repb.PSI{
		Some: &repb.PSI_Metrics{Avg10: 12.5, Avg60: 5, Avg300: 1, Total: 250000},
		Full: &repb.PSI_Metrics{Avg10: 2.5, Avg60: 1, Avg300: 0.5, Total: 50000},
	}, snapshot.MemoryPressure)
}

func TestNewRejectsUnavailableCgroupMemoryPressure(t *testing.T) {
	cgroupDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.current"), []byte("750"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.stat"), []byte("inactive_file 200\n"), 0644))
	flags.Set(t, "executor.oom_killer.memory_pressure_some_stall_threshold", 0.2)
	flags.Set(t, "executor.enable_oci", true)
	monitor := &cgroupMemoryMonitor{dir: cgroupDir, limitBytes: 1000}

	// A configured PSI requirement cannot safely treat an unavailable pressure
	// signal as zero pressure, so OOM killer construction should reject the monitor.
	oomKiller, err := New(t.Context(), monitor)
	require.ErrorContains(t, err, "check executor OOM killer memory pressure support: read executor cgroup memory pressure")
	require.Nil(t, oomKiller)
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
