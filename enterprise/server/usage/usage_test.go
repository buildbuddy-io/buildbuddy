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
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclock"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
)

const (
	collectionPeriodDuration = 1 * time.Minute
)

var (
	// Define some usage periods and collection periods within those usage periods

	usage1Start            = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	usage1Collection1Start = usage1Start.Add(0 * collectionPeriodDuration)
	usage1Collection2Start = usage1Start.Add(1 * collectionPeriodDuration)
	usage1Collection3Start = usage1Start.Add(2 * collectionPeriodDuration)
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

func usecString(t time.Time) string {
	return fmt.Sprintf("%d", timeutil.ToUsec(t))
}

func queryAllUsages(t *testing.T, te *testenv.TestEnv) []*tables.Usage {
	usages := []*tables.Usage{}
	dbh := te.GetDBHandle()
	rows, err := dbh.Raw(`
		SELECT * From Usages
		ORDER BY group_id, period_start_usec ASC;
	`).Rows()
	require.NoError(t, err)

	for rows.Next() {
		tu := &tables.Usage{}
		err := dbh.ScanRows(rows, tu)
		require.NoError(t, err)
		// Throw out Model timestamps to simplify assertions.
		tu.Model = tables.Model{}
		usages = append(usages, tu)
	}
	return usages
}

// assertNoFurtherDBAccess makes the test fail if it tries to access the DB
// after this function is called.
func assertNoFurtherDBAccess(t *testing.T, te *testenv.TestEnv) {
	fail := func(db *gorm.DB) {
		require.FailNowf(t, "unexpected query", "SQL: %s", db.Statement.SQL.String())
	}
	dbh := te.GetDBHandle()
	dbh.Callback().Create().Register("fail", fail)
	dbh.Callback().Query().Register("fail", fail)
	dbh.Callback().Update().Register("fail", fail)
	dbh.Callback().Delete().Register("fail", fail)
	dbh.Callback().Row().Register("fail", fail)
	dbh.Callback().Raw().Register("fail", fail)
}

func TestUsageTracker_Increment_MultipleCollectionPeriodsInSameUsagePeriod(t *testing.T) {
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
	countsKey1 := "usage/counts/GR1/" + usecString(usage1Collection1Start)
	countsKey2 := "usage/counts/GR1/" + usecString(usage1Collection2Start)
	groupsKey1 := "usage/groups/" + usecString(usage1Collection1Start)
	groupsKey2 := "usage/groups/" + usecString(usage1Collection2Start)
	require.ElementsMatch(
		t, []string{countsKey1, countsKey2, groupsKey1, groupsKey2}, keys,
		"redis keys should match expected format")

	counts1, err := rdb.HGetAll(ctx, countsKey1).Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"CasCacheHits":           "1001",
		"ActionCacheHits":        "10",
		"TotalDownloadSizeBytes": "100",
	}, counts1, "counts should match what we observed")

	groupIDs1, err := rdb.SMembers(ctx, groupsKey1).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"GR1"}, groupIDs1, "groups should equal the groups with usage data")

	counts2, err := rdb.HGetAll(ctx, countsKey2).Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"CasCacheHits":    "2",
		"ActionCacheHits": "20",
	}, counts2, "counts should match what we observed")

	groupIDs2, err := rdb.SMembers(ctx, groupsKey2).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"GR1"}, groupIDs2, "groups should equal the groups with usage data")

	// Set clock so that the written collection periods are finalized.
	clock.Set(clock.Now().Add(2 * collectionPeriodDuration))
	// Now flush the data to the DB.
	err = ut.FlushToDB(context.Background())

	require.NoError(t, err)
	usages := queryAllUsages(t, te)

	assert.ElementsMatch(t, []*tables.Usage{
		{
			PeriodStartUsec: timeutil.ToUsec(usage1Start),
			GroupID:         "GR1",
			// We wrote 2 collection periods, so data should be final up to the 3rd
			// collection period.
			FinalBeforeUsec: timeutil.ToUsec(usage1Collection3Start),
			UsageCounts: tables.UsageCounts{
				CasCacheHits:           1001 + 2,
				ActionCacheHits:        10 + 20,
				TotalDownloadSizeBytes: 100,
			},
		},
	}, usages, "data flushed to DB should match expected values")
}

func TestUsageTracker_Increment_MultipleGroupsInSameCollectionPeriod(t *testing.T) {
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
	countsKey1 := "usage/counts/GR1/" + usecString(usage1Collection1Start)
	countsKey2 := "usage/counts/GR2/" + usecString(usage1Collection1Start)
	groupsKey := "usage/groups/" + usecString(usage1Collection1Start)
	require.ElementsMatch(
		t, []string{countsKey1, countsKey2, groupsKey}, keys,
		"redis keys should match expected format")

	counts1, err := rdb.HGetAll(ctx, countsKey1).Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"CasCacheHits": "1",
	}, counts1, "counts should match what we observed")

	counts2, err := rdb.HGetAll(ctx, countsKey2).Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"CasCacheHits": "10",
	}, counts2, "counts should match what we observed")

	groupIDs, err := rdb.SMembers(ctx, groupsKey).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"GR1", "GR2"}, groupIDs, "groups should equal the groups with usage data")

	// Set clock so that the written collection periods are finalized.
	clock.Set(clock.Now().Add(2 * collectionPeriodDuration))
	// Now flush the data to the DB.
	err = ut.FlushToDB(context.Background())

	require.NoError(t, err)
	usages := queryAllUsages(t, te)
	// We only flushed one collection period worth of data for both groups,
	// so usage rows should be finalized up to the second collection period.
	assert.ElementsMatch(t, []*tables.Usage{
		{
			PeriodStartUsec: timeutil.ToUsec(usage1Start),
			GroupID:         "GR1",
			FinalBeforeUsec: timeutil.ToUsec(usage1Collection2Start),
			UsageCounts: tables.UsageCounts{
				CasCacheHits: 1,
			},
		},
		{
			PeriodStartUsec: timeutil.ToUsec(usage1Start),
			GroupID:         "GR2",
			FinalBeforeUsec: timeutil.ToUsec(usage1Collection2Start),
			UsageCounts: tables.UsageCounts{
				CasCacheHits: 10,
			},
		},
	}, usages, "data flushed to DB should match expected values")
}

func TestUsageTracker_Flush_DoesNotFlushUnsettledCollectionPeriods(t *testing.T) {
	clock := testclock.StartingAt(usage1Collection1Start)
	te := setupEnv(t)
	ctx := authContext(te, "US1")
	ut, err := usage.NewTracker(te, &usage.TrackerOpts{Clock: clock})
	require.NoError(t, err)

	err = ut.Increment(ctx, &tables.UsageCounts{CasCacheHits: 1})
	require.NoError(t, err)
	clock.Set(usage1Collection2Start)
	err = ut.Increment(ctx, &tables.UsageCounts{CasCacheHits: 10})
	require.NoError(t, err)
	clock.Set(usage1Collection3Start)
	err = ut.Increment(ctx, &tables.UsageCounts{CasCacheHits: 100})
	require.NoError(t, err)

	// Note: we're at the start of the 3rd collection period when flushing.
	err = ut.FlushToDB(ctx)

	require.NoError(t, err)
	usages := queryAllUsages(t, te)
	// - 1st period should be flushed since it was 2 collection periods ago.
	// - 2nd period should *not* be flushed; even though it is in the past,
	//   we allow a bit more time for server jobs to finish writing their usage
	//   data for that period.
	// - 3rd period should *not* be flushed since it's the current period.
	require.Equal(t, []*tables.Usage{
		{
			GroupID:         "GR1",
			PeriodStartUsec: timeutil.ToUsec(usage1Start),
			FinalBeforeUsec: timeutil.ToUsec(usage1Collection2Start),
			UsageCounts: tables.UsageCounts{
				CasCacheHits: 1,
			},
		},
	}, usages)
}

func TestUsageTracker_Flush_OnlyWritesToDBIfNecessary(t *testing.T) {
	clock := testclock.StartingAt(usage1Collection1Start)
	te := setupEnv(t)
	ctx := authContext(te, "US1")
	ut, err := usage.NewTracker(te, &usage.TrackerOpts{Clock: clock})
	require.NoError(t, err)

	err = ut.Increment(ctx, &tables.UsageCounts{CasCacheHits: 1})
	require.NoError(t, err)

	clock.Set(clock.Now().Add(2 * collectionPeriodDuration))
	err = ut.FlushToDB(ctx)
	require.NoError(t, err)

	usages := queryAllUsages(t, te)
	require.NotEmpty(t, usages)

	// Make the next Flush call fail if it tries to do anything with the DB,
	// since there was no usage observed.
	assertNoFurtherDBAccess(t, te)

	err = ut.FlushToDB(ctx)
	require.NoError(t, err)
}

func TestUsageTracker_Flush_ConcurrentAccessAcrossApps(t *testing.T) {
	clock := testclock.StartingAt(usage1Collection1Start)
	te := setupEnv(t)
	ctx := authContext(te, "US1")

	ut, err := usage.NewTracker(te, &usage.TrackerOpts{Clock: clock})
	require.NoError(t, err)

	err = ut.Increment(ctx, &tables.UsageCounts{CasCacheHits: 1})
	require.NoError(t, err)
	clock.Set(clock.Now().Add(2 * collectionPeriodDuration))

	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < 100; i++ {
		eg.Go(func() error {
			ut, err := usage.NewTracker(te, &usage.TrackerOpts{Clock: clock})
			require.NoError(t, err)
			return ut.FlushToDB(ctx)
		})
	}

	err = eg.Wait()
	require.NoError(t, err)
}
