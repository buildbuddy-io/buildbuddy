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
	"github.com/go-redis/redis/v8"
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
	te.SetDefaultRedisClient(rdb)

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

func queryAllUsages(t *testing.T, te *testenv.TestEnv) []*tables.Usage {
	usages := []*tables.Usage{}
	dbh := te.GetDBHandle()
	rows, err := dbh.Raw(`
		SELECT * From Usages
		ORDER BY group_id, period_start_usec, region ASC;
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

// requireNoFurtherDBAccess makes the test fail immediately if it tries to
// access the DB after this function is called.
func requireNoFurtherDBAccess(t *testing.T, te *testenv.TestEnv) {
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

type nopDistributedLock struct{}

func (*nopDistributedLock) Lock(context context.Context) error   { return nil }
func (*nopDistributedLock) Unlock(context context.Context) error { return nil }

func TestUsageTracker_Increment_MultipleCollectionPeriodsInSameUsagePeriod(t *testing.T) {
	clock := testclock.StartingAt(usage1Collection1Start)
	te := setupEnv(t)
	ctx := authContext(te, "US1")
	rbuf := redisutil.NewCommandBuffer(te.GetDefaultRedisClient())
	ut := usage.NewTracker(te, clock, usage.NewFlushLock(te), rbuf, &usage.TrackerOpts{Region: "us-west1"})

	// Increment some counts
	ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 1})
	ut.Increment(ctx, &tables.UsageCounts{ActionCacheHits: 10})
	ut.Increment(ctx, &tables.UsageCounts{TotalDownloadSizeBytes: 100})
	ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 1_000})

	// Go to the next collection period
	clock.Set(usage1Collection2Start)

	// Increment some more counts
	ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 2})
	ut.Increment(ctx, &tables.UsageCounts{ActionCacheHits: 20})

	// Flush Redis command buffer and check that Redis has the expected state.
	err := rbuf.Flush(context.Background())
	require.NoError(t, err)
	rdb := te.GetDefaultRedisClient()
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

	// Set clock so that the written collection periods are finalized.
	clock.Set(clock.Now().Add(2 * collectionPeriodDuration))
	// Now flush the data to the DB.
	err = ut.FlushToDB(context.Background())

	require.NoError(t, err)
	usages := queryAllUsages(t, te)

	assert.ElementsMatch(t, []*tables.Usage{
		{
			PeriodStartUsec: usage1Start.UnixMicro(),
			GroupID:         "GR1",
			Region:          "us-west1",
			// We wrote 2 collection periods, so data should be final up to the 3rd
			// collection period.
			FinalBeforeUsec: usage1Collection3Start.UnixMicro(),
			UsageCounts: tables.UsageCounts{
				CASCacheHits:           1001 + 2,
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
	rbuf := redisutil.NewCommandBuffer(te.GetDefaultRedisClient())
	ut := usage.NewTracker(te, clock, usage.NewFlushLock(te), rbuf, &usage.TrackerOpts{Region: "us-west1"})

	// Increment for group 1, then group 2
	ut.Increment(ctx1, &tables.UsageCounts{CASCacheHits: 1})
	ut.Increment(ctx2, &tables.UsageCounts{CASCacheHits: 10})

	err := rbuf.Flush(context.Background())
	require.NoError(t, err)
	rdb := te.GetDefaultRedisClient()
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
			PeriodStartUsec: usage1Start.UnixMicro(),
			GroupID:         "GR1",
			Region:          "us-west1",
			FinalBeforeUsec: usage1Collection2Start.UnixMicro(),
			UsageCounts: tables.UsageCounts{
				CASCacheHits: 1,
			},
		},
		{
			PeriodStartUsec: usage1Start.UnixMicro(),
			GroupID:         "GR2",
			Region:          "us-west1",
			FinalBeforeUsec: usage1Collection2Start.UnixMicro(),
			UsageCounts: tables.UsageCounts{
				CASCacheHits: 10,
			},
		},
	}, usages, "data flushed to DB should match expected values")
}

func TestUsageTracker_Flush_DoesNotFlushUnsettledCollectionPeriods(t *testing.T) {
	clock := testclock.StartingAt(usage1Collection1Start)
	te := setupEnv(t)
	ctx := authContext(te, "US1")
	rbuf := redisutil.NewCommandBuffer(te.GetDefaultRedisClient())
	ut := usage.NewTracker(te, clock, usage.NewFlushLock(te), rbuf, &usage.TrackerOpts{Region: "us-west1"})

	err := ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 1})
	require.NoError(t, err)
	clock.Set(usage1Collection2Start)
	err = ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 10})
	require.NoError(t, err)
	clock.Set(usage1Collection3Start)
	err = ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 100})
	require.NoError(t, err)

	// Note: we're at the start of the 3rd collection period when flushing.
	err = rbuf.Flush(context.Background())
	require.NoError(t, err)
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
			Region:          "us-west1",
			PeriodStartUsec: usage1Start.UnixMicro(),
			FinalBeforeUsec: usage1Collection2Start.UnixMicro(),
			UsageCounts: tables.UsageCounts{
				CASCacheHits: 1,
			},
		},
	}, usages)
}

func TestUsageTracker_Flush_OnlyWritesToDBIfNecessary(t *testing.T) {
	clock := testclock.StartingAt(usage1Collection1Start)
	te := setupEnv(t)
	ctx := authContext(te, "US1")
	rbuf := redisutil.NewCommandBuffer(te.GetDefaultRedisClient())
	ut := usage.NewTracker(te, clock, usage.NewFlushLock(te), rbuf, &usage.TrackerOpts{Region: "us-west1"})

	err := ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 1})
	require.NoError(t, err)

	err = rbuf.Flush(context.Background())
	require.NoError(t, err)
	clock.Set(clock.Now().Add(2 * collectionPeriodDuration))
	err = ut.FlushToDB(ctx)
	require.NoError(t, err)

	usages := queryAllUsages(t, te)
	require.NotEmpty(t, usages)

	// Make the next Flush call fail if it tries to do anything with the DB,
	// since there was no usage observed.
	requireNoFurtherDBAccess(t, te)

	err = ut.FlushToDB(ctx)
	require.NoError(t, err)
}

func TestUsageTracker_Flush_ConcurrentAccessAcrossApps(t *testing.T) {
	clock := testclock.StartingAt(usage1Collection1Start)
	te := setupEnv(t)
	ctx := authContext(te, "US1")

	rbuf := redisutil.NewCommandBuffer(te.GetDefaultRedisClient())
	ut := usage.NewTracker(te, clock, usage.NewFlushLock(te), rbuf, &usage.TrackerOpts{Region: "us-west1"})

	// Write 2 collection periods worth of data.
	err := ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 1})
	require.NoError(t, err)
	clock.Set(clock.Now().Add(collectionPeriodDuration))
	err = ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 1000})
	require.NoError(t, err)
	err = rbuf.Flush(context.Background())
	require.NoError(t, err)
	clock.Set(clock.Now().Add(2 * collectionPeriodDuration))

	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < 100; i++ {
		eg.Go(func() error {
			ut := usage.NewTracker(
				te, clock,
				// Disable Redis locking to test that the DB queries are properly
				// synchronized on their own. Redis is purely used as an optimization to
				// reduce DB load, and we should not overcount data if Redis fails.
				&nopDistributedLock{},
				rbuf,
				&usage.TrackerOpts{Region: "us-west1"},
			)
			require.NoError(t, err)
			return ut.FlushToDB(ctx)
		})
	}

	err = eg.Wait()
	require.NoError(t, err)

	usages := queryAllUsages(t, te)
	require.Equal(t, []*tables.Usage{
		{
			GroupID:         "GR1",
			Region:          "us-west1",
			PeriodStartUsec: usage1Start.UnixMicro(),
			FinalBeforeUsec: usage1Collection3Start.UnixMicro(),
			UsageCounts:     tables.UsageCounts{CASCacheHits: 1001},
		},
	}, usages)
}

func TestUsageTracker_Flush_CrossRegion(t *testing.T) {
	// Set up 2 envs, one for each region. DB should be the same for each, but
	// Redis instances should be different.
	te1 := setupEnv(t)
	te2 := setupEnv(t)
	te2.SetDBHandle(te1.GetDBHandle())
	ctx1 := authContext(te1, "US1")
	ctx2 := authContext(te2, "US1")
	rbuf1 := redisutil.NewCommandBuffer(te1.GetDefaultRedisClient())
	rbuf2 := redisutil.NewCommandBuffer(te2.GetDefaultRedisClient())
	clock := testclock.StartingAt(usage1Collection1Start)
	ut1 := usage.NewTracker(te1, clock, usage.NewFlushLock(te1), rbuf1, &usage.TrackerOpts{Region: "us-west1"})
	ut2 := usage.NewTracker(te2, clock, usage.NewFlushLock(te2), rbuf2, &usage.TrackerOpts{Region: "europe-north1"})

	// Record and flush usage in 2 different regions.
	err := ut1.Increment(ctx1, &tables.UsageCounts{CASCacheHits: 1})
	require.NoError(t, err)
	err = ut2.Increment(ctx2, &tables.UsageCounts{CASCacheHits: 100})
	err = rbuf1.Flush(context.Background())
	require.NoError(t, err)
	err = rbuf2.Flush(context.Background())
	require.NoError(t, err)
	clock.Set(clock.Now().Add(2 * collectionPeriodDuration))
	err = ut1.FlushToDB(context.Background())
	require.NoError(t, err)
	err = ut2.FlushToDB(context.Background())
	require.NoError(t, err)

	// Make sure the usage was tracked in each region separately.
	usages := queryAllUsages(t, te1)
	require.Equal(t, []*tables.Usage{
		{
			GroupID:         "GR1",
			Region:          "europe-north1",
			PeriodStartUsec: usage1Start.UnixMicro(),
			FinalBeforeUsec: usage1Collection2Start.UnixMicro(),
			UsageCounts:     tables.UsageCounts{CASCacheHits: 100},
		},
		{
			GroupID:         "GR1",
			Region:          "us-west1",
			PeriodStartUsec: usage1Start.UnixMicro(),
			FinalBeforeUsec: usage1Collection2Start.UnixMicro(),
			UsageCounts:     tables.UsageCounts{CASCacheHits: 1},
		},
	}, usages)
}
