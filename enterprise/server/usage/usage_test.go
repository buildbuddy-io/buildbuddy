package usage_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclock"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/go-redis/redis/v8"
	"github.com/go-sql-driver/mysql"
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
	usage1Collection4Start = usage1Start.Add(3 * collectionPeriodDuration)
)

func setupEnv(t *testing.T) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)

	redisTarget := testredis.Start(t).Target
	rdb := redis.NewClient(redisutil.TargetToOptions(redisTarget))
	te.SetDefaultRedisClient(rdb)
	rbuf := redisutil.NewCommandBuffer(te.GetDefaultRedisClient())
	rmc := redis_metrics_collector.New(rdb, rbuf)
	te.SetMetricsCollector(rmc)

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
	ctx := context.Background()
	dbh := te.GetDBHandle()
	rows, err := dbh.DB(ctx).Raw(`
		SELECT * From Usages
		ORDER BY group_id, period_start_usec, region ASC;
	`).Rows()
	require.NoError(t, err)

	for rows.Next() {
		tu := &tables.Usage{}
		err := dbh.DB(ctx).ScanRows(rows, tu)
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
	ctx := context.Background()
	dbh := te.GetDBHandle()
	dbh.DB(ctx).Callback().Create().Register("fail", fail)
	dbh.DB(ctx).Callback().Query().Register("fail", fail)
	dbh.DB(ctx).Callback().Update().Register("fail", fail)
	dbh.DB(ctx).Callback().Delete().Register("fail", fail)
	dbh.DB(ctx).Callback().Row().Register("fail", fail)
	dbh.DB(ctx).Callback().Raw().Register("fail", fail)
}

type nopDistributedLock struct{}

func (*nopDistributedLock) Lock(context context.Context) error   { return nil }
func (*nopDistributedLock) Unlock(context context.Context) error { return nil }

func TestUsageTracker_Increment_MultipleCollectionPeriodsInSameUsagePeriod(t *testing.T) {
	clock := testclock.StartingAt(usage1Collection1Start)
	te := setupEnv(t)
	flags.Set(t, "app.usage_tracking_enabled", true)
	ctx := authContext(te, "US1")
	flags.Set(t, "app.region", "us-west1")
	ut, err := usage.NewTracker(te, clock, usage.NewFlushLock(te))
	require.NoError(t, err)

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
	err = te.GetMetricsCollector().Flush(context.Background())
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
	flags.Set(t, "app.usage_tracking_enabled", true)
	ctx1 := authContext(te, "US1")
	ctx2 := authContext(te, "US2")
	flags.Set(t, "app.region", "us-west1")
	ut, err := usage.NewTracker(te, clock, usage.NewFlushLock(te))
	require.NoError(t, err)

	// Increment for group 1, then group 2
	ut.Increment(ctx1, &tables.UsageCounts{CASCacheHits: 1})
	ut.Increment(ctx2, &tables.UsageCounts{CASCacheHits: 10})

	err = te.GetMetricsCollector().Flush(context.Background())
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
	flags.Set(t, "app.usage_tracking_enabled", true)
	ctx := authContext(te, "US1")
	flags.Set(t, "app.region", "us-west1")
	ut, err := usage.NewTracker(te, clock, usage.NewFlushLock(te))
	require.NoError(t, err)

	err = ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 1})
	require.NoError(t, err)
	clock.Set(usage1Collection2Start)
	err = ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 10})
	require.NoError(t, err)
	clock.Set(usage1Collection3Start)
	err = ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 100})
	require.NoError(t, err)

	// Note: we're at the start of the 3rd collection period when flushing.
	err = te.GetMetricsCollector().Flush(context.Background())
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
	flags.Set(t, "app.usage_tracking_enabled", true)
	ctx := authContext(te, "US1")
	flags.Set(t, "app.region", "us-west1")
	ut, err := usage.NewTracker(te, clock, usage.NewFlushLock(te))
	require.NoError(t, err)

	err = ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 1})
	require.NoError(t, err)

	err = te.GetMetricsCollector().Flush(context.Background())
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
	flags.Set(t, "app.usage_tracking_enabled", true)
	ctx := authContext(te, "US1")

	flags.Set(t, "app.region", "us-west1")
	ut, err := usage.NewTracker(te, clock, usage.NewFlushLock(te))
	require.NoError(t, err)

	// Write 2 collection periods worth of data.
	err = ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 1})
	require.NoError(t, err)
	clock.Set(clock.Now().Add(collectionPeriodDuration))
	err = ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 1000})
	require.NoError(t, err)
	err = te.GetMetricsCollector().Flush(context.Background())
	require.NoError(t, err)
	clock.Set(clock.Now().Add(2 * collectionPeriodDuration))

	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < 100; i++ {
		eg.Go(func() error {
			ut, err := usage.NewTracker(
				te, clock,
				// Disable Redis locking to test that the DB queries are properly
				// synchronized on their own. Redis is purely used as an optimization to
				// reduce DB load, and we should not overcount data if Redis fails.
				&nopDistributedLock{},
			)
			require.NoError(t, err)
			err = ut.FlushToDB(ctx)
			// With MySQL, it's very likely that the queries issued for the flush
			// transaction might cause a deadlock. It's OK if a deadlock causes a
			// flush operation to fail, as long as at least one app succeeds.
			if isDeadlockError(err) {
				return nil
			}
			return err
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
	flags.Set(t, "app.usage_tracking_enabled", true)
	te2.SetDBHandle(te1.GetDBHandle())
	ctx1 := authContext(te1, "US1")
	ctx2 := authContext(te2, "US1")
	clock := testclock.StartingAt(usage1Collection1Start)
	flags.Set(t, "app.region", "us-west1")
	ut1, err := usage.NewTracker(te1, clock, usage.NewFlushLock(te1))
	require.NoError(t, err)
	flags.Set(t, "app.region", "europe-north1")
	ut2, err := usage.NewTracker(te2, clock, usage.NewFlushLock(te2))
	require.NoError(t, err)

	// Record and flush usage in 2 different regions.
	err = ut1.Increment(ctx1, &tables.UsageCounts{CASCacheHits: 1})
	require.NoError(t, err)
	err = ut2.Increment(ctx2, &tables.UsageCounts{CASCacheHits: 100})
	err = te1.GetMetricsCollector().Flush(context.Background())
	require.NoError(t, err)
	err = te2.GetMetricsCollector().Flush(context.Background())
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

func TestUsageTracker_AllFieldsAreMapped(t *testing.T) {
	counts1 := increasingCountsStartingAt(100)
	counts2 := increasingCountsStartingAt(10000)
	te := setupEnv(t)
	flags.Set(t, "app.usage_tracking_enabled", true)
	clock := testclock.StartingAt(usage1Collection1Start)
	ctx := authContext(te, "US1")
	flags.Set(t, "app.region", "us-west1")
	ut, err := usage.NewTracker(te, clock, usage.NewFlushLock(te))
	require.NoError(t, err)

	// Increment twice to test both insert and update queries.

	err = ut.Increment(ctx, counts1)
	require.NoError(t, err)

	err = te.GetMetricsCollector().Flush(context.Background())
	require.NoError(t, err)
	clock.Set(clock.Now().Add(2 * collectionPeriodDuration))
	err = ut.FlushToDB(ctx)
	require.NoError(t, err)

	err = ut.Increment(ctx, counts2)
	require.NoError(t, err)

	err = te.GetMetricsCollector().Flush(context.Background())
	require.NoError(t, err)
	clock.Set(clock.Now().Add(2 * collectionPeriodDuration))
	err = ut.FlushToDB(ctx)
	require.NoError(t, err)

	usages := queryAllUsages(t, te)
	expectedCounts := addCounts(counts1, counts2)
	require.Equal(t, []*tables.Usage{{
		PeriodStartUsec: usage1Start.UnixMicro(),
		FinalBeforeUsec: usage1Collection4Start.UnixMicro(),
		GroupID:         "GR1",
		UsageCounts:     *expectedCounts,
		Region:          "us-west1",
	}}, usages)
}

func increasingCountsStartingAt(value int64) *tables.UsageCounts {
	counts := &tables.UsageCounts{}
	countsValue := reflect.ValueOf(counts).Elem()
	for i := 0; i < countsValue.NumField(); i++ {
		countsValue.Field(i).Set(reflect.ValueOf(value))
		value++
	}
	return counts
}

func addCounts(c1, c2 *tables.UsageCounts) *tables.UsageCounts {
	counts := &tables.UsageCounts{}
	countsValue := reflect.ValueOf(counts).Elem()
	for i := 0; i < countsValue.NumField(); i++ {
		v1 := reflect.ValueOf(c1).Elem().Field(i).Int()
		v2 := reflect.ValueOf(c2).Elem().Field(i).Int()
		countsValue.Field(i).Set(reflect.ValueOf(v1 + v2))
	}
	return counts
}

func isDeadlockError(err error) bool {
	if err == nil {
		return false
	}
	if err, ok := err.(*mysql.MySQLError); ok {
		return err.Number == 1213
	}
	return false
}
