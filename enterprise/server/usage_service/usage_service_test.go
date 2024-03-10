package usage_service_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/google/go-cmp/cmp"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
)

func TestGetUsage(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1", "US2", "GR2"))
	env.SetAuthenticator(ta)
	ctx1, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)
	// Current time for test: 2024-02-22, noon UTC
	now := time.Date(2024, 2, 22, 12, 0, 0, 0, time.UTC)
	clock := clockwork.NewFakeClockAt(now)
	service := usage_service.New(env, clock)
	// Insert some usage data:
	for _, row := range []*tables.Usage{
		// GR1, current usage period
		{
			UsageID:         "UG1",
			GroupID:         "GR1",
			PeriodStartUsec: time.Date(2024, 2, 3, 0, 0, 0, 0, time.UTC).UnixMicro(),
			UsageCounts:     tables.UsageCounts{Invocations: 13, CASCacheHits: 10_000},
		},
		// GR1, previous usage period
		{
			UsageID:         "UG2",
			GroupID:         "GR1",
			PeriodStartUsec: time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC).UnixMicro(),
			UsageCounts:     tables.UsageCounts{CASCacheHits: 77},
		},
		// GR2, current usage period
		{
			UsageID:         "UG3",
			GroupID:         "GR2",
			PeriodStartUsec: time.Date(2024, 2, 3, 0, 0, 0, 0, time.UTC).UnixMicro(),
			UsageCounts:     tables.UsageCounts{Invocations: 107},
		},
	} {
		err = env.GetDBHandle().NewQuery(ctx, "test").Create(row)
		require.NoError(t, err)
	}

	rsp, err := service.GetUsage(ctx1, &usagepb.GetUsageRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
		UsagePeriod:    "2024-02",
	})
	require.NoError(t, err)

	expectedResponse := &usagepb.GetUsageResponse{
		Usage: []*usagepb.Usage{
			{
				Period:       "2024-02",
				Invocations:  13,
				CasCacheHits: 10_000,
			},
		},
		AvailableUsagePeriods: []string{
			"2024-02",
			"2024-01",
			"2023-12",
			"2023-11",
			"2023-10",
			"2023-09",
		},
	}
	assert.Empty(t, cmp.Diff(expectedResponse, rsp, protocmp.Transform()))
}
