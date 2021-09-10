package usage_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
	"github.com/go-redis/redis"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	collectionPeriodDuration = 1 * time.Minute
)

var (
	// Define some usage periods and collection periods within those usage periods

	usage1Start            = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	usage1Collection1Start = usage1Start.Add(0 * collectionPeriodDuration)
	usage1Collection2Start = usage1Start.Add(1 * collectionPeriodDuration)
)

func setupEnv(t *testing.T) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)

	redisTarget := testredis.Start(t)
	rdb := redis.NewClient(redisutil.TargetToOptions(redisTarget))
	te.SetUsageRedisClient(rdb)

	auth := testauth.NewTestAuthenticator(testauth.TestUsers(
		"US1", "GR1",
		"US2", "GR2",
	))
	te.SetAuthenticator(auth)

	return te
}

func authContext(te *testenv.TestEnv, userID string) context.Context {
	return te.GetAuthenticator().(*testauth.TestAuthenticator).AuthContextFromAPIKey(context.Background(), userID)
}

func usecString(t time.Time) string {
	return fmt.Sprintf("%d", timeutil.ToUsec(t))
}

func TestUsageTracker_Increment_UpdatesRedisStateAcrossCollectionPeriods(t *testing.T) {
	clock := clockwork.NewFakeClockAt(usage1Collection1Start)
	te := setupEnv(t)
	ctx := authContext(te, "US1")
	ut, err := usage.NewTracker(te, &usage.TrackerOpts{Clock: clock})
	require.NoError(t, err)

	// Increment some counts
	ut.Increment(ctx, &tables.UsageCounts{CasCacheHits: 1})
	ut.Increment(ctx, &tables.UsageCounts{ActionCacheHits: 10})
	ut.Increment(ctx, &tables.UsageCounts{TotalDownloadSizeBytes: 100})
	ut.Increment(ctx, &tables.UsageCounts{CasCacheHits: 1_000})

	// Go to the next collection period
	clock.Advance(usage1Collection2Start.Sub(clock.Now()))

	// Increment some more counts
	ut.Increment(ctx, &tables.UsageCounts{CasCacheHits: 2})
	ut.Increment(ctx, &tables.UsageCounts{ActionCacheHits: 20})

	rdb := te.GetUsageRedisClient()
	keys, err := rdb.Keys(ctx, "usage/*").Result()
	require.NoError(t, err)
	key1 := "usage/counts/GR1/" + usecString(usage1Collection1Start)
	key2 := "usage/counts/GR1/" + usecString(usage1Collection2Start)
	require.ElementsMatch(t, []string{key1, key2}, keys, "redis keys should match expected format")

	counts1, err := rdb.HGetAll(ctx, key1).Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"CasCacheHits":           "1001",
		"ActionCacheHits":        "10",
		"TotalDownloadSizeBytes": "100",
	}, counts1, "counts should match what we observed")

	counts2, err := rdb.HGetAll(ctx, key2).Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"CasCacheHits":    "2",
		"ActionCacheHits": "20",
	}, counts2, "counts should match what we observed")
}

func TestUsageTracker_Increment_UpdatesRedisStateForDifferentGroups(t *testing.T) {
	clock := clockwork.NewFakeClockAt(usage1Collection1Start)
	te := setupEnv(t)
	ctx1 := authContext(te, "US1")
	ctx2 := authContext(te, "US2")
	ut, err := usage.NewTracker(te, &usage.TrackerOpts{Clock: clock})
	require.NoError(t, err)

	// Increment for group 1, then group 2
	ut.Increment(ctx1, &tables.UsageCounts{CasCacheHits: 1})
	ut.Increment(ctx2, &tables.UsageCounts{CasCacheHits: 10})

	rdb := te.GetUsageRedisClient()
	ctx := context.Background()
	keys, err := rdb.Keys(ctx, "usage/*").Result()
	require.NoError(t, err)
	key1 := "usage/counts/GR1/" + usecString(usage1Collection1Start)
	key2 := "usage/counts/GR2/" + usecString(usage1Collection1Start)
	require.ElementsMatch(t, []string{key1, key2}, keys, "redis keys should match expected format")

	counts1, err := rdb.HGetAll(ctx, key1).Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"CasCacheHits": "1",
	}, counts1, "counts should match what we observed")

	counts2, err := rdb.HGetAll(ctx, key2).Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"CasCacheHits": "10",
	}, counts2, "counts should match what we observed")
}

func TestUsageTracker_ObserveInvocation_UpdatesRedisState(t *testing.T) {
	clock := clockwork.NewFakeClockAt(usage1Collection1Start)
	te := setupEnv(t)
	ctx := authContext(te, "US1")
	ut, err := usage.NewTracker(te, &usage.TrackerOpts{Clock: clock})
	require.NoError(t, err)

	ut.ObserveInvocation(ctx, "abc-123")
	ut.ObserveInvocation(ctx, "def-456")
	ut.ObserveInvocation(ctx, "abc-123")

	rdb := te.GetUsageRedisClient()
	keys, err := rdb.Keys(ctx, "usage/*").Result()
	require.NoError(t, err)
	require.Len(t, keys, 1)
	ik := keys[0]
	require.Equal(t, ik, fmt.Sprintf("usage/invocations/GR1/%d", timeutil.ToUsec(usage1Collection1Start)), "redis key should match the expected format")
	invocationIDs, err := rdb.SMembers(ctx, ik).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"abc-123", "def-456"}, invocationIDs, "invocation IDs should match what we observed")
}
