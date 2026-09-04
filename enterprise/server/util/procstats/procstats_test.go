package procstats_test

import (
	"errors"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/procstats"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestTreeStat(t *testing.T) {
	ts := procstats.NewTreeStats(os.Getpid())
	require.NoError(t, ts.Update())
}

func TestMonitorProviderSamplesAfterTermination(t *testing.T) {
	terminated := make(chan struct{})
	close(terminated)
	providerCalls := 0
	listenerCalls := 0

	stats := procstats.MonitorProvider(func() (*repb.UsageStats, error) {
		providerCalls++
		return &repb.UsageStats{CpuNanos: 123, PeakMemoryBytes: 456}, nil
	}, func(stats *repb.UsageStats) {
		listenerCalls++
		require.Equal(t, int64(123), stats.GetCpuNanos())
	}, terminated)

	require.Equal(t, 1, providerCalls)
	require.Equal(t, 1, listenerCalls)
	require.Equal(t, int64(123), stats.GetCpuNanos())
	require.Equal(t, int64(456), stats.GetPeakMemoryBytes())
}

func TestMonitorProviderIgnoresSamplingError(t *testing.T) {
	terminated := make(chan struct{})
	close(terminated)

	stats := procstats.MonitorProvider(func() (*repb.UsageStats, error) {
		return nil, errors.New("unavailable")
	}, nil, terminated)

	require.NotNil(t, stats)
	require.Zero(t, stats.GetCpuNanos())
}

func TestMonitorProviderPreservesCumulativeStatsOnSamplingError(t *testing.T) {
	terminated := make(chan struct{})
	providerCalls := 0

	stats := procstats.MonitorProvider(func() (*repb.UsageStats, error) {
		providerCalls++
		if providerCalls == 1 {
			return &repb.UsageStats{CpuNanos: 123, MemoryBytes: 100, PeakMemoryBytes: 456}, nil
		}
		return &repb.UsageStats{CpuNanos: 12}, errors.New("partial sample")
	}, func(*repb.UsageStats) {
		if providerCalls == 1 {
			close(terminated)
		}
	}, terminated)

	require.Equal(t, 2, providerCalls)
	require.Equal(t, int64(123), stats.GetCpuNanos())
	require.Equal(t, int64(456), stats.GetPeakMemoryBytes())
	require.Zero(t, stats.GetMemoryBytes())
}
