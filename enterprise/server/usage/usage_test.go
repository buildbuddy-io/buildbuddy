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
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
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
	periodDuration = 1 * time.Minute
)

var (
	// Define some usage periods

	period1Start = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	period2Start = period1Start.Add(1 * periodDuration)
	period3Start = period1Start.Add(2 * periodDuration)
	period4Start = period1Start.Add(3 * periodDuration)
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
		SELECT * From "Usages"
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

func TestUsageTracker_Increment_MultipleGroupsInSameCollectionPeriod(t *testing.T) {
	clock := testclock.StartingAt(period1Start)
	te := setupEnv(t)
	flags.Set(t, "app.usage_tracking_enabled", true)
	ctx1 := authContext(te, "US1")
	ctx2 := authContext(te, "US2")
	flags.Set(t, "app.region", "us-west1")
	ut, err := usage.NewTracker(te, clock, newFlushLock(t, te))
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
	countsKey1 := "usage/counts/GR1/" + timeStr(period1Start)
	countsKey2 := "usage/counts/GR2/" + timeStr(period1Start)
	groupsKey := "usage/groups/" + timeStr(period1Start)
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

	// Set clock so that the written periods are finalized.
	clock.Set(clock.Now().Add(2 * periodDuration))
	// Now flush the data to the DB.
	err = ut.FlushToDB(context.Background())

	require.NoError(t, err)
	usages := queryAllUsages(t, te)
	// We only flushed one period worth of data for both groups, so usage rows
	// should be finalized up to the second period.
	assert.ElementsMatch(t, []*tables.Usage{
		{
			PeriodStartUsec: period1Start.UnixMicro(),
			GroupID:         "GR1",
			Region:          "us-west1",
			UsageCounts: tables.UsageCounts{
				CASCacheHits: 1,
			},
		},
		{
			PeriodStartUsec: period1Start.UnixMicro(),
			GroupID:         "GR2",
			Region:          "us-west1",
			UsageCounts: tables.UsageCounts{
				CASCacheHits: 10,
			},
		},
	}, usages, "data flushed to DB should match expected values")
}

func TestUsageTracker_Flush_DoesNotFlushUnsettledCollectionPeriods(t *testing.T) {
	clock := testclock.StartingAt(period1Start)
	te := setupEnv(t)
	flags.Set(t, "app.usage_tracking_enabled", true)
	ctx := authContext(te, "US1")
	flags.Set(t, "app.region", "us-west1")
	ut, err := usage.NewTracker(te, clock, newFlushLock(t, te))
	require.NoError(t, err)

	err = ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 1})
	require.NoError(t, err)
	clock.Set(period2Start)
	err = ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 10})
	require.NoError(t, err)
	clock.Set(period3Start)
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
			PeriodStartUsec: period1Start.UnixMicro(),
			UsageCounts: tables.UsageCounts{
				CASCacheHits: 1,
			},
		},
	}, usages)
}

func TestUsageTracker_Flush_OnlyWritesToDBIfNecessary(t *testing.T) {
	clock := testclock.StartingAt(period1Start)
	te := setupEnv(t)
	flags.Set(t, "app.usage_tracking_enabled", true)
	ctx := authContext(te, "US1")
	flags.Set(t, "app.region", "us-west1")
	ut, err := usage.NewTracker(te, clock, newFlushLock(t, te))
	require.NoError(t, err)

	err = ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 1})
	require.NoError(t, err)

	err = te.GetMetricsCollector().Flush(context.Background())
	require.NoError(t, err)
	clock.Set(clock.Now().Add(2 * periodDuration))
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
	clock := testclock.StartingAt(period1Start)
	te := setupEnv(t)
	flags.Set(t, "app.usage_tracking_enabled", true)
	ctx := authContext(te, "US1")

	flags.Set(t, "app.region", "us-west1")
	ut, err := usage.NewTracker(te, clock, newFlushLock(t, te))
	require.NoError(t, err)

	// Write 2 collection periods worth of data.
	err = ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 1})
	require.NoError(t, err)
	clock.Set(clock.Now().Add(periodDuration))
	err = ut.Increment(ctx, &tables.UsageCounts{CASCacheHits: 1000})
	require.NoError(t, err)
	err = te.GetMetricsCollector().Flush(context.Background())
	require.NoError(t, err)
	clock.Set(clock.Now().Add(2 * periodDuration))

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
			PeriodStartUsec: period1Start.UnixMicro(),
			UsageCounts:     tables.UsageCounts{CASCacheHits: 1},
		},
		{
			GroupID:         "GR1",
			Region:          "us-west1",
			PeriodStartUsec: period2Start.UnixMicro(),
			UsageCounts:     tables.UsageCounts{CASCacheHits: 1000},
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
	clock := testclock.StartingAt(period1Start)
	flags.Set(t, "app.region", "us-west1")
	ut1, err := usage.NewTracker(te1, clock, newFlushLock(t, te1))
	require.NoError(t, err)
	flags.Set(t, "app.region", "europe-north1")
	ut2, err := usage.NewTracker(te2, clock, newFlushLock(t, te2))
	require.NoError(t, err)

	// Record and flush usage in 2 different regions.
	err = ut1.Increment(ctx1, &tables.UsageCounts{CASCacheHits: 1})
	require.NoError(t, err)
	err = ut2.Increment(ctx2, &tables.UsageCounts{CASCacheHits: 100})
	err = te1.GetMetricsCollector().Flush(context.Background())
	require.NoError(t, err)
	err = te2.GetMetricsCollector().Flush(context.Background())
	require.NoError(t, err)
	clock.Set(clock.Now().Add(2 * periodDuration))
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
			PeriodStartUsec: period1Start.UnixMicro(),
			UsageCounts:     tables.UsageCounts{CASCacheHits: 100},
		},
		{
			GroupID:         "GR1",
			Region:          "us-west1",
			PeriodStartUsec: period1Start.UnixMicro(),
			UsageCounts:     tables.UsageCounts{CASCacheHits: 1},
		},
	}, usages)
}

func TestUsageTracker_AllFieldsAreMapped(t *testing.T) {
	counts := increasingCountsStartingAt(100)
	te := setupEnv(t)
	flags.Set(t, "app.usage_tracking_enabled", true)
	clock := testclock.StartingAt(period1Start)
	ctx := authContext(te, "US1")
	flags.Set(t, "app.region", "us-west1")
	ut, err := usage.NewTracker(te, clock, newFlushLock(t, te))
	require.NoError(t, err)

	err = ut.Increment(ctx, counts)
	require.NoError(t, err)

	err = te.GetMetricsCollector().Flush(context.Background())
	require.NoError(t, err)
	clock.Set(clock.Now().Add(2 * periodDuration))
	err = ut.FlushToDB(ctx)
	require.NoError(t, err)

	usages := queryAllUsages(t, te)
	require.Equal(t, []*tables.Usage{{
		PeriodStartUsec: period1Start.UnixMicro(),
		GroupID:         "GR1",
		UsageCounts:     *counts,
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

func newFlushLock(t *testing.T, te *testenv.TestEnv) interfaces.DistributedLock {
	lock, err := usage.NewFlushLock(te)
	require.NoError(t, err)
	return lock
}
