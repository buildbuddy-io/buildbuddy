package cgroup

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

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
