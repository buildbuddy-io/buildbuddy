package invocation_stat_service

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	sfpb "github.com/buildbuddy-io/buildbuddy/proto/stat_filter"
	statspb "github.com/buildbuddy-io/buildbuddy/proto/stats"
)

func TestGetDrilldownSubquery_ExitCode(t *testing.T) {
	var iss *InvocationStatService
	ctx := context.Background()
	drilldownFields := []string{"user", "host", "exit_code"}
	execMetric := sfpb.ExecutionMetricType_UPDATED_AT_USEC_EXECUTION_METRIC
	req := &statspb.GetStatDrilldownRequest{
		DrilldownMetric: &sfpb.Metric{
			Execution: &execMetric,
		},
	}
	where := "WHERE group_id = 'GR1'"
	whereArgs := []interface{}{}
	drilldown := "success = true"
	drilldownArgs := []interface{}{}
	col := "exit_code"

	query, args := iss.getDrilldownSubquery(ctx, drilldownFields, req, where, whereArgs, drilldown, drilldownArgs, col)

	require.Contains(t, query, "toString(exit_code) as gorm_exit_code")
	require.Contains(t, query, "GROUP BY toString(exit_code)")
	require.NotContains(t, query, "GROUP BY gorm_exit_code")
	require.Equal(t, 0, len(args))
}

func TestGetDrilldownSubquery_NonExitCode(t *testing.T) {
	var iss *InvocationStatService
	ctx := context.Background()
	drilldownFields := []string{"user", "host", "worker"}
	execMetric := sfpb.ExecutionMetricType_UPDATED_AT_USEC_EXECUTION_METRIC
	req := &statspb.GetStatDrilldownRequest{
		DrilldownMetric: &sfpb.Metric{
			Execution: &execMetric,
		},
	}
	where := "WHERE group_id = 'GR1'"
	whereArgs := []interface{}{}
	drilldown := "success = true"
	drilldownArgs := []interface{}{}
	col := "worker"

	query, _ := iss.getDrilldownSubquery(ctx, drilldownFields, req, where, whereArgs, drilldown, drilldownArgs, col)

	require.Contains(t, query, "worker as gorm_worker")
	require.Contains(t, query, "GROUP BY gorm_worker")
	require.NotContains(t, query, "toString(exit_code)")
}

func TestGetLogMetricBuckets(t *testing.T) {
	for _, tc := range []struct {
		name            string
		low             int64
		high            int64
		wantBuckets     []int64
		wantHadNegative bool
	}{
		{
			name:        "anchored at floor power of 10 of min",
			low:         5,
			high:        5000,
			wantBuckets: []int64{1, 10, 100, 1000, 10000},
		},
		{
			name:        "zero min prepends [0,1) bucket",
			low:         0,
			high:        500,
			wantBuckets: []int64{0, 1, 10, 100, 1000},
		},
		{
			name:            "negative min clamps to zero and flags",
			low:             -50,
			high:            900,
			wantBuckets:     []int64{0, 1, 10, 100, 1000},
			wantHadNegative: true,
		},
		{
			name:        "all zero values yields single [0,1) bucket",
			low:         0,
			high:        0,
			wantBuckets: []int64{0, 1},
		},
		{
			name:        "min and max in same decade",
			low:         12,
			high:        12,
			wantBuckets: []int64{10, 100},
		},
		{
			name:            "all negative values",
			low:             -100,
			high:            -1,
			wantBuckets:     []int64{0, 1},
			wantHadNegative: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buckets, hadNegative := getLogMetricBuckets(tc.low, tc.high)
			require.Equal(t, tc.wantBuckets, buckets)
			require.Equal(t, tc.wantHadNegative, hadNegative)
			// Boundaries must be strictly increasing so roundDown buckets correctly.
			for i := 1; i < len(buckets); i++ {
				require.Greater(t, buckets[i], buckets[i-1])
			}
		})
	}
}

func TestGetLogMetricBuckets_CapsBucketCountWithoutOverflow(t *testing.T) {
	buckets, hadNegative := getLogMetricBuckets(0, math.MaxInt64)
	require.False(t, hadNegative)
	// At most maxNumMetricBuckets buckets (maxNumMetricBuckets+1 boundaries).
	require.LessOrEqual(t, len(buckets)-1, maxNumMetricBuckets)
	for i := 1; i < len(buckets); i++ {
		require.Greater(t, buckets[i], buckets[i-1])
	}
}

func TestGetLinearMetricBuckets(t *testing.T) {
	buckets := getLinearMetricBuckets(0, 100)
	require.Len(t, buckets, maxNumMetricBuckets+1)
	require.Equal(t, int64(0), buckets[0])
	// Evenly spaced.
	step := buckets[1] - buckets[0]
	for i := 1; i < len(buckets); i++ {
		require.Equal(t, step, buckets[i]-buckets[i-1])
	}
}
