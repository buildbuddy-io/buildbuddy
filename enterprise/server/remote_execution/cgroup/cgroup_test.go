package cgroup

import (
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/block_io"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

func TestSettingsMap(t *testing.T) {
	for _, test := range []struct {
		name     string
		settings *scpb.CgroupSettings
		expected map[string]string
	}{
		{
			name:     "nil",
			settings: nil,
			expected: map[string]string{},
		},
		{
			name:     "all fields unset",
			settings: nil,
			expected: map[string]string{},
		},
		{
			name: "all values set",
			settings: &scpb.CgroupSettings{
				CpuWeight:                proto.Int64(200),
				CpuQuotaLimitUsec:        proto.Int64(400e3),
				CpuQuotaPeriodUsec:       proto.Int64(100e3),
				CpuMaxBurstUsec:          proto.Int64(50e3),
				CpuUclampMin:             proto.Float32(12.34),
				CpuUclampMax:             proto.Float32(98.76),
				MemoryThrottleLimitBytes: proto.Int64(777e6),
				MemoryLimitBytes:         proto.Int64(800e6),
				MemorySoftGuaranteeBytes: proto.Int64(100e6),
				MemoryMinimumBytes:       proto.Int64(50e6),
				SwapThrottleLimitBytes:   proto.Int64(800e6),
				SwapLimitBytes:           proto.Int64(1e9),
				BlockIoLatencyTargetUsec: proto.Int64(100e3),
				BlockIoWeight:            proto.Int64(300),
				BlockIoLimit: &scpb.CgroupSettings_BlockIOLimits{
					Riops: proto.Int64(1000),
					Wiops: proto.Int64(500),
					Rbps:  proto.Int64(4096e3),
					Wbps:  proto.Int64(1024e3),
				},
			},
			expected: map[string]string{
				"cpu.weight":       "200",
				"cpu.max":          "400000 100000",
				"cpu.max.burst":    "50000",
				"cpu.uclamp.min":   "12.34",
				"cpu.uclamp.max":   "98.76",
				"memory.high":      "777000000",
				"memory.max":       "800000000",
				"memory.low":       "100000000",
				"memory.min":       "50000000",
				"memory.swap.high": "800000000",
				"memory.swap.max":  "1000000000",
				"io.latency":       "279:8 target=100000",
				"io.weight":        "279:8 300",
				"io.max":           "279:8 riops=1000 wiops=500 rbps=4096000 wbps=1024000",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			device := &block_io.Device{Maj: 279, Min: 8}
			m, err := settingsMap(test.settings, device)
			require.NoError(t, err)
			require.Equal(t, test.expected, m)
		})
	}
}

func TestParsePSI(t *testing.T) {
	r := strings.NewReader(`some avg10=0.00 avg60=1.00 avg300=4.11 total=123456
full avg10=0.01 avg60=0.50 avg300=1.23 total=23456
`)
	psi, err := readPSI(r)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(&repb.PSI{
		Some: &repb.PSI_Metrics{
			Avg10:  0.0,
			Avg60:  1.0,
			Avg300: 4.11,
			Total:  123456,
		},
		Full: &repb.PSI_Metrics{
			Avg10:  0.01,
			Avg60:  0.5,
			Avg300: 1.23,
			Total:  23456,
		},
	}, psi, protocmp.Transform()))
}

func TestParseIOStats(t *testing.T) {
	r := strings.NewReader(`259:1 rbytes=688128 wbytes=0 rios=21 wios=0 dbytes=0 dios=0
9:0 rbytes=3952640 wbytes=0 rios=48 wios=0 dbytes=0 dios=0
`)
	stats, err := readIOStat(r)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff([]*repb.CgroupIOStats{
		{Maj: 259, Min: 1, Rbytes: 688128, Wbytes: 0, Rios: 21, Wios: 0, Dbytes: 0, Dios: 0},
		{Maj: 9, Min: 0, Rbytes: 3952640, Wbytes: 0, Rios: 48, Wios: 0, Dbytes: 0, Dios: 0},
	}, stats, protocmp.Transform()))
}
