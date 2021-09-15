package usage_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclock"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/go-redis/redis"
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
	te.SetCacheRedisClient(rdb)

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

func timeStr(t time.Time) string {
	return t.Format(time.RFC3339)
}

func TestUsageTracker_Increment_UpdatesRedisStateAcrossCollectionPeriods(t *testing.T) {
	clock := testclock.StartingAt(usage1Collection1Start)
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
	clock.Set(usage1Collection2Start)

	// Increment some more counts
	ut.Increment(ctx, &tables.UsageCounts{CasCacheHits: 2})
	ut.Increment(ctx, &tables.UsageCounts{ActionCacheHits: 20})

	rdb := te.GetCacheRedisClient()
	keys, err := rdb.Keys(ctx, "usage/*").Result()
	require.NoError(t, err)
	countsKey1 := "usage/counts/GR1/" + timeStr(usage1Collection1Start)
	countsKey2 := "usage/counts/GR1/" + timeStr(usage1Collection2Start)
	groupsKey1 := "usage/groups/" + timeStr(usage1Collection1Start)
	groupsKey2 := "usage/groups/" + timeStr(usage1Collection2Start)
	require.ElementsMatch(
		t, []string{countsKey1, countsKey2, groupsKey1, groupsKey2}, keys,
		"redis keys should match expected format")

	counts1, err := rdb.HGetAll(ctx, countsKey1).Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"cas_cache_hits":            "1001",
		"action_cache_hits":         "10",
		"total_download_size_bytes": "100",
	}, counts1, "counts should match what we observed")

	groupIDs1, err := rdb.SMembers(ctx, groupsKey1).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"GR1"}, groupIDs1, "groups should equal the groups with usage data")

	counts2, err := rdb.HGetAll(ctx, countsKey2).Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"cas_cache_hits":    "2",
		"action_cache_hits": "20",
	}, counts2, "counts should match what we observed")

	groupIDs2, err := rdb.SMembers(ctx, groupsKey2).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"GR1"}, groupIDs2, "groups should equal the groups with usage data")
}

func TestUsageTracker_Increment_UpdatesRedisStateForDifferentGroups(t *testing.T) {
	clock := testclock.StartingAt(usage1Collection1Start)
	te := setupEnv(t)
	ctx1 := authContext(te, "US1")
	ctx2 := authContext(te, "US2")
	ut, err := usage.NewTracker(te, &usage.TrackerOpts{Clock: clock})
	require.NoError(t, err)

	// Increment for group 1, then group 2
	ut.Increment(ctx1, &tables.UsageCounts{CasCacheHits: 1})
	ut.Increment(ctx2, &tables.UsageCounts{CasCacheHits: 10})

	rdb := te.GetCacheRedisClient()
	ctx := context.Background()
	keys, err := rdb.Keys(ctx, "usage/*").Result()
	require.NoError(t, err)
	countsKey1 := "usage/counts/GR1/" + timeStr(usage1Collection1Start)
	countsKey2 := "usage/counts/GR2/" + timeStr(usage1Collection1Start)
	groupsKey := "usage/groups/" + timeStr(usage1Collection1Start)
	require.ElementsMatch(
		t, []string{countsKey1, countsKey2, groupsKey}, keys,
		"redis keys should match expected format")

	counts1, err := rdb.HGetAll(ctx, countsKey1).Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"cas_cache_hits": "1",
	}, counts1, "counts should match what we observed")

	counts2, err := rdb.HGetAll(ctx, countsKey2).Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"cas_cache_hits": "10",
	}, counts2, "counts should match what we observed")

	groupIDs, err := rdb.SMembers(ctx, groupsKey).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"GR1", "GR2"}, groupIDs, "groups should equal the groups with usage data")
}
