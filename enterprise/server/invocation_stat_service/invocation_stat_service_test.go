package invocation_stat_service

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	sfpb "github.com/buildbuddy-io/buildbuddy/proto/stat_filter"
	statspb "github.com/buildbuddy-io/buildbuddy/proto/stats"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
	olaptables "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
)

func TestGetInvocationStat(t *testing.T) {
	flags.Set(t, "testenv.use_clickhouse", true)
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1"))
	te.SetAuthenticator(ta)
	err := te.GetOLAPDBHandle().GORM(ctx, "test_create_invocations").Create([]olaptables.Invocation{
		{
			UpdatedAtUsec:    1000,
			GroupID:          "GR1",
			InvocationUUID:   "b0f0e1d284d742cd8d4ab3e128744f5d",
			RepoURL:          "https://github.com/buildbuddy-io/buildbuddy",
			Success:          true,
			InvocationStatus: int64(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
		},
		{
			UpdatedAtUsec:    2000,
			GroupID:          "GR1",
			InvocationUUID:   "6f64b34bbb17470987de56ee4f3f4813",
			RepoURL:          "https://github.com/buildbuddy-io/protoc-gen-protobufjs",
			Success:          false,
			InvocationStatus: int64(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
		},
	}).Error
	require.NoError(t, err)
	iss := NewInvocationStatService(te, te.GetDBHandle(), te.GetOLAPDBHandle())
	for _, tc := range []struct {
		name      string
		userID    string
		groupID   string
		repoURL   string
		wantStats []*inpb.InvocationStat
		wantErr   error
	}{
		{
			name:    "all builds for group",
			userID:  "US1",
			groupID: "GR1",
			wantStats: []*inpb.InvocationStat{
				{
					Name:                    "GR1",
					LatestBuildTimeUsec:     2000,
					LastGreenBuildUsec:      1000,
					LastRedBuildUsec:        2000,
					TotalNumBuilds:          2,
					TotalNumSucessfulBuilds: 1,
					TotalNumFailingBuilds:   1,
				},
			},
		},
		{
			name:    "filter by normalized repo URL",
			userID:  "US1",
			groupID: "GR1",
			repoURL: " buildbuddy-io/buildbuddy ",
			wantStats: []*inpb.InvocationStat{
				{
					Name:                    "GR1",
					LatestBuildTimeUsec:     1000,
					LastGreenBuildUsec:      1000,
					LastRedBuildUsec:        0,
					TotalNumBuilds:          1,
					TotalNumSucessfulBuilds: 1,
				},
			},
			wantErr: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var requestContext *ctxpb.RequestContext
			if tc.userID != "" {
				authCtx, err := ta.WithAuthenticatedUser(ctx, tc.userID)
				require.NoError(t, err)
				ctx = authCtx
				requestContext = testauth.RequestContext(tc.userID, tc.groupID)
			}
			rsp, err := iss.GetInvocationStat(ctx, &inpb.GetInvocationStatRequest{
				RequestContext:  requestContext,
				AggregationType: inpb.AggType_GROUP_ID_AGGREGATION_TYPE,
				Query: &inpb.InvocationStatQuery{
					RepoUrl: tc.repoURL,
				},
			})
			if tc.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tc.wantErr, err)
			} else {
				require.NoError(t, err)
				require.Empty(t, cmp.Diff(tc.wantStats, rsp.GetInvocationStat(), protocmp.Transform()))
			}
		})
	}
}

func TestGetStatDrilldown(t *testing.T) {
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "app.trends_heatmap_enabled", true)
	te := testenv.GetTestEnv(t)

	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1"))
	te.SetAuthenticator(ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "US1")
	require.NoError(t, err)

	reqCtx := testauth.RequestContext("US1", "GR1")

	iss := NewInvocationStatService(te, te.GetDBHandle(), te.GetOLAPDBHandle())
	_, err = iss.GetStatDrilldown(ctx, &statspb.GetStatDrilldownRequest{
		RequestContext: reqCtx,
	})
	require.True(t, status.IsInvalidArgumentError(err))
}

func TestExecutionDrilldownExcludesMergedExecutions(t *testing.T) {
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "app.trends_heatmap_enabled", true)
	te := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1"))
	te.SetAuthenticator(ta)
	ctx, err := ta.WithAuthenticatedUser(context.Background(), "US1")
	require.NoError(t, err)
	reqCtx := testauth.RequestContext("US1", "GR1")
	windowStart := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	updatedAt := windowStart.Add(time.Hour).UnixMicro()
	executions := []olaptables.Execution{
		{ExecutionUUID: "00000000-0000-0000-0000-000000000001", InvocationUUID: "00000000000000000000000000000001", User: "legacy", PeakMemoryBytes: 10},
		{ExecutionUUID: "00000000-0000-0000-0000-000000000002", InvocationUUID: "00000000000000000000000000000002", User: "new", PeakMemoryBytes: 20, InvocationLinkType: int8(sipb.StoredInvocationLink_NEW)},
		{ExecutionUUID: "00000000-0000-0000-0000-000000000002", InvocationUUID: "00000000000000000000000000000003", User: "merged", PeakMemoryBytes: 20, InvocationLinkType: int8(sipb.StoredInvocationLink_MERGED)},
		{ExecutionUUID: "00000000-0000-0000-0000-000000000003", InvocationUUID: "00000000000000000000000000000004", User: "merged", PeakMemoryBytes: 1000, InvocationLinkType: int8(sipb.StoredInvocationLink_MERGED)},
	}
	for i := range executions {
		executions[i].GroupID = "GR1"
		executions[i].UpdatedAtUsec = updatedAt
	}
	err = te.GetOLAPDBHandle().GORM(ctx, "test_create_executions").Create(executions).Error
	require.NoError(t, err)
	iss := NewInvocationStatService(te, te.GetDBHandle(), te.GetOLAPDBHandle())
	peakMemory := sfpb.ExecutionMetricType_PEAK_MEMORY_EXECUTION_METRIC
	metric := &sfpb.Metric{Execution: &peakMemory}
	selectionMin := int64(20)
	for _, user := range []string{"", "merged"} {
		t.Run("user="+user, func(t *testing.T) {
			query := &statspb.TrendQuery{
				User:          user,
				UpdatedAfter:  timestamppb.New(windowStart),
				UpdatedBefore: timestamppb.New(windowStart.Add(2 * time.Hour)),
			}
			t.Run("heatmap", func(t *testing.T) {
				rsp, err := iss.GetStatHeatmap(ctx, &statspb.GetStatHeatmapRequest{
					RequestContext: reqCtx, Metric: metric, Query: query,
				})
				require.NoError(t, err)
				var count, total int64
				for _, col := range rsp.GetColumn() {
					for _, v := range col.GetValue() {
						count += v
					}
					for _, v := range col.GetTotal() {
						total += v
					}
				}
				if user == "merged" {
					require.Zero(t, count)
					require.Zero(t, total)
					return
				}
				require.Equal(t, int64(2), count)
				require.Equal(t, int64(30), total)
				// Merged rows must not expand the heatmap's metric range either.
				require.NotEmpty(t, rsp.GetBucketBracket())
				require.Less(t, rsp.GetBucketBracket()[len(rsp.GetBucketBracket())-1], int64(1000))
			})
			t.Run("breakdowns", func(t *testing.T) {
				rsp, err := iss.GetStatDrilldown(ctx, &statspb.GetStatDrilldownRequest{
					RequestContext: reqCtx, DrilldownMetric: metric, Query: query,
					Filter: []*sfpb.StatFilter{{Metric: metric, Min: &selectionMin}},
				})
				require.NoError(t, err)
				if user == "merged" {
					require.Zero(t, rsp.GetTotalInBase())
					require.Zero(t, rsp.GetTotalInSelection())
					return
				}
				require.Equal(t, int64(1), rsp.GetTotalInBase())
				require.Equal(t, int64(1), rsp.GetTotalInSelection())
				var users []*statspb.DrilldownEntry
				for _, chart := range rsp.GetChart() {
					if chart.GetDrilldownType() == statspb.DrilldownType_USER_DRILLDOWN_TYPE {
						users = chart.GetEntry()
					}
				}
				require.Empty(t, cmp.Diff([]*statspb.DrilldownEntry{
					{Label: "new", SelectionValue: 1},
					{Label: "legacy", BaseValue: 1},
				}, users, protocmp.Transform()))
			})
		})
	}
}

func TestGetStatHeatmap_LogScale(t *testing.T) {
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "app.trends_heatmap_enabled", true)
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1"))
	te.SetAuthenticator(ta)

	// Insert invocations whose durations span four orders of magnitude, so a
	// log-scale heatmap subdivides each decade into 1-2-4-6-8-10 steps.
	windowStart := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	updatedAt := windowStart.Add(time.Hour).UnixMicro()
	durations := []int64{5, 50, 500, 5000}
	invocations := make([]olaptables.Invocation, 0, len(durations))
	uuids := []string{
		"00000000000000000000000000000001",
		"00000000000000000000000000000002",
		"00000000000000000000000000000003",
		"00000000000000000000000000000004",
	}
	for i, d := range durations {
		invocations = append(invocations, olaptables.Invocation{
			UpdatedAtUsec:    updatedAt,
			GroupID:          "GR1",
			InvocationUUID:   uuids[i],
			DurationUsec:     d,
			Success:          true,
			InvocationStatus: int64(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
		})
	}
	err := te.GetOLAPDBHandle().GORM(ctx, "test_create_invocations").Create(invocations).Error
	require.NoError(t, err)

	authCtx, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)
	reqCtx := testauth.RequestContext("US1", "GR1")

	durationMetric := sfpb.InvocationMetricType_DURATION_USEC_INVOCATION_METRIC
	iss := NewInvocationStatService(te, te.GetDBHandle(), te.GetOLAPDBHandle())
	rsp, err := iss.GetStatHeatmap(authCtx, &statspb.GetStatHeatmapRequest{
		RequestContext: reqCtx,
		Metric:         &sfpb.Metric{Invocation: &durationMetric},
		LogScale:       true,
		Query: &statspb.TrendQuery{
			UpdatedAfter:  timestamppb.New(windowStart),
			UpdatedBefore: timestamppb.New(windowStart.Add(2 * time.Hour)),
		},
	})
	require.NoError(t, err)

	// The range spans 4 powers of 10, so each decade is subdivided into
	// 1-2-4-6-8-10 steps and we trim down to only buckets that enclose
	// the metric range of [5, 5000].
	require.Equal(t, []int64{4, 6, 8, 10, 20, 40, 60, 80, 100, 200, 400, 600, 800, 1000, 2000, 4000, 6000}, rsp.GetBucketBracket())
	require.False(t, rsp.GetMetricHadNegativeValues())

	// Every invocation should be counted exactly once across all buckets.
	var valueCount int64
	for _, col := range rsp.GetColumn() {
		for _, v := range col.GetValue() {
			valueCount += v
		}
	}
	var totalCount int64
	for _, col := range rsp.GetColumn() {
		for _, t := range col.GetTotal() {
			totalCount += t
		}
	}
	require.Equal(t, int64(len(durations)), valueCount)
	require.Equal(t, int64(5555), totalCount)
}
