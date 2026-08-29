//go:build linux

package procstats

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseProcStatData(t *testing.T) {
	// A real /proc/<pid>/stat line, with a comm containing spaces and ')'.
	line := "12345 (a (weird) name) S 1 12345 12345 0 -1 4194560 1000 0 0 0 250 50 0 0 20 0 1 0 100 4096000 300 18446744073709551615 0 0 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n"
	s, err := parseProcStatData([]byte(line), 100, 4096)
	require.NoError(t, err)
	require.Equal(t, int64(300*4096), s.MemoryBytes)
	require.Equal(t, int64((250+50)*1e7), s.CpuNanos) // 3.00s at 100 Hz

	// Alpha: USER_HZ is 1024.
	s, err = parseProcStatData([]byte(line), 1024, 8192)
	require.NoError(t, err)
	require.Equal(t, int64(300*8192), s.MemoryBytes)
	require.Equal(t, int64(300*1_000_000_000/1024), s.CpuNanos) // 292968750, not truncated per tick

	_, err = parseProcStatData([]byte("garbage"), 100, 4096)
	require.Error(t, err)
	_, err = parseProcStatData([]byte("1 (x) S 1 2"), 100, 4096)
	require.Error(t, err)
}

func TestGetProcessStats_Self(t *testing.T) {
	require.Greater(t, clockTicks(), int64(0))
	s, err := getProcessStats(os.Getpid())
	require.NoError(t, err)
	require.Greater(t, s.MemoryBytes, int64(0))
	require.GreaterOrEqual(t, s.CpuNanos, int64(0))

	// RSS from stat field 24 must agree with what gopsutil read: the second
	// field of /proc/<pid>/statm (both are the kernel's resident page count;
	// allow a little drift since the two reads are not atomic).
	statm, err := os.ReadFile("/proc/self/statm")
	require.NoError(t, err)
	var size, resident int64
	_, err = fmt.Sscan(string(statm), &size, &resident)
	require.NoError(t, err)
	require.InEpsilon(t, float64(resident*int64(os.Getpagesize())), float64(s.MemoryBytes), 0.1)
}
