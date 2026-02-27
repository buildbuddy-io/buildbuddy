package invocation_stat_service

import (
	"context"
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
