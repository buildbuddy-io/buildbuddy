package usage

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	redsync_goredis "github.com/go-redsync/redsync/v4/redis/goredis/v8"
)

const (
	// collectionPeriodDuration determines the length of time for usage data
	// buckets in Redis. This directly affects the minimum period at which we
	// can flush data to the DB, since we only flush buckets for past time
	// periods.
	//
	// NOTE: this is not intended to be a "knob" that can be tweaked -- various
	// pieces of the implementation implicitly rely on this value, and the current
	// synchronization logic does not account for this being changed. If we do
	// decide to change this value while the usage tracker is running in production,
	// we need to be careful not to overcount usage when transitioning to the new
	// value.
	collectionPeriodDuration = 1 * time.Minute

	// collectionPeriodSettlingTime is the max length of time that we expect
	// usage data to be written to a collection period bucket in Redis after the
	// collection period has ended. This accounts for differences in clocks across
	// apps as well as Redis latency.
	collectionPeriodSettlingTime = 10 * time.Second

	// redisKeyTTL defines how long usage keys have to live before they are
	// deleted automatically by Redis.
	//
	// Keys should live for at least 2 collection periods since collection periods
	// aren't finalized until the period is past, plus some wiggle room for Redis
	// latency. We add a few more collection periods on top of that, in case
	// flushing fails due to transient errors.
	redisKeyTTL = 5 * collectionPeriodDuration

	redisUsageKeyPrefix  = "usage/"
	redisGroupsKeyPrefix = redisUsageKeyPrefix + "groups/"
	redisCountsKeyPrefix = redisUsageKeyPrefix + "counts/"

	redisUsageLockKey    = "lock.usage"
	redisUsageLockExpiry = 45 * time.Second
	// If a flush fails due to a timeout, extend the timeout by this long so that
	// we have enough time to unlock the lock.
	redisUsageLockFinalizationTimeout = 5 * time.Second

	flushInterval = collectionPeriodDuration
	flushTimeout  = redisUsageLockExpiry - 10*time.Second
)

var (
	collectionPeriodZeroValue = collectionPeriodStartingAt(time.Unix(0, 0))
)

type TrackerOpts struct {
	Clock timeutil.Clock
}

type tracker struct {
	env   environment.Env
	rdb   *redis.Client
	clock timeutil.Clock

	flushMutex *redsync.Mutex
	stopFlush  chan struct{}
}

func NewTracker(env environment.Env, opts *TrackerOpts) (*tracker, error) {
	rdb := env.GetCacheRedisClient()
	if rdb == nil {
		return nil, status.UnimplementedError("Missing redis client for usage")
	}
	clock := opts.Clock
	if clock == nil {
		clock = timeutil.NewClock()
	}

	pool := redsync_goredis.NewPool(rdb)
	rs := redsync.New(pool)
	flushMutex := rs.NewMutex(
		redisUsageLockKey,
		redsync.WithTries(1), redsync.WithExpiry(redisUsageLockExpiry),
	)

	return &tracker{
		env:        env,
		rdb:        rdb,
		clock:      clock,
		flushMutex: flushMutex,
		stopFlush:  make(chan struct{}),
	}, nil
}

func (ut *tracker) Increment(ctx context.Context, counts *tables.UsageCounts) error {
	groupID, err := perms.AuthenticatedGroupID(ctx, ut.env)
	if err != nil {
		return err
	}
	if groupID == "" {
		// Don't track anonymous usage for now.
		return nil
	}

	m, err := countsToMap(counts)
	if err != nil {
		return err
	}
	if len(m) == 0 {
		return nil
	}

	period := ut.currentCollectionPeriod()

	pipe := ut.rdb.TxPipeline()
	// Add the group ID to the set of groups with usage
	groupsKey := groupsRedisKey(period)
	pipe.SAdd(ctx, groupsKey, groupID)
	pipe.Expire(ctx, groupsKey, redisKeyTTL)
	// Increment the hash values
	countsKey := countsRedisKey(groupID, period)
	for k, c := range m {
		pipe.HIncrBy(ctx, countsKey, k, c)
	}
	pipe.Expire(ctx, countsKey, redisKeyTTL)

	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	return nil
}

// StartDBFlush starts a goroutine that periodically flushes usage data from
// Redis to the DB.
func (ut *tracker) StartDBFlush() {
	go func() {
		ctx := context.Background()
		for {
			select {
			case <-time.After(flushInterval):
				if err := ut.FlushToDB(ctx); err != nil {
					log.Errorf("Failed to flush usage data to DB: %s", err)
				}
			case <-ut.stopFlush:
				return
			}
		}
	}()
}

// StopDBFlush cancels the goroutine started by StartDBFlush.
func (ut *tracker) StopDBFlush() {
	ut.stopFlush <- struct{}{}
}

// FlushToDB flushes usage metrics from any finalized collection periods to the
// DB.
//
// Public for testing only; the server should call StartDBFlush to periodically
// flush usage.
func (ut *tracker) FlushToDB(ctx context.Context) error {
	// redsync.ErrFailed can be safely ignored; it just means that another app
	// is already trying to flush.
	if err := ut.flushToDB(ctx); err != nil && err != redsync.ErrFailed {
		return err
	}
	return nil
}

func (ut *tracker) flushToDB(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, flushTimeout)
	defer cancel()

	// Grab lock. This will immediately return `redsync.ErrFailed` if another
	// client already holds the lock.
	if err := ut.flushMutex.LockContext(ctx); err != nil {
		return err
	}
	defer func() {
		ctx, cancel := background.ExtendContextForFinalization(ctx, redisUsageLockFinalizationTimeout)
		defer cancel()
		if ok, err := ut.flushMutex.UnlockContext(ctx); !ok || err != nil {
			log.Warningf("Failed to unlock Redis lock: ok=%v, err=%s", ok, err)
		}
	}()

	dbh := ut.env.GetDBHandle()

	// Loop through collection periods starting from the oldest collection period
	// that may exist in Redis (based on key expiration time) and looping up until
	// we hit a collection period which is not yet "settled".

	for c := ut.oldestWritableCollectionPeriod(); ut.isSettled(c); c = c.Next() {
		// Read groups
		gk := groupsRedisKey(c)
		groupIDs, err := ut.rdb.SMembers(ctx, gk).Result()
		if err != nil {
			return err
		}

		for _, groupID := range groupIDs {
			// Read usage counts, flush to DB, then delete the Redis key for the counts.
			ck := countsRedisKey(groupID, c)
			h, err := ut.rdb.HGetAll(ctx, ck).Result()
			if err != nil {
				return err
			}
			counts, err := stringMapToCounts(h)
			if err != nil {
				return err
			}

			pk := &tables.Usage{
				GroupID:         groupID,
				PeriodStartUsec: timeutil.ToUsec(c.UsagePeriod().Start()),
			}
			err = dbh.Transaction(ctx, func(tx *db.DB) error {
				// Create if not exists
				err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(pk).Error
				if err != nil {
					return err
				}
				updates, err := ut.usageUpdates(c, counts)
				if err != nil {
					return err
				}
				// Update if collection period data not already been written
				return tx.Model(&tables.Usage{}).Where(
					`group_id = ?
					AND period_start_usec = ?
					AND final_before_usec <= ?`,
					pk.GroupID,
					pk.PeriodStartUsec,
					timeutil.ToUsec(c.Start()),
				).Updates(updates).Error
			})
			if err != nil {
				return err
			}
			if _, err := ut.rdb.Del(ctx, ck).Result(); err != nil {
				return err
			}
		}

		// Delete the Redis data for the groups.
		if _, err := ut.rdb.Del(ctx, gk).Result(); err != nil {
			return err
		}
	}
	return nil
}

// usageUpdates returns a GORM updates map that finalizes the usage row up to
// the end of the collection period and increments each field by the
// corresponding value in counts.
func (ut *tracker) usageUpdates(c collectionPeriod, counts *tables.UsageCounts) (map[string]interface{}, error) {
	updates := map[string]interface{}{
		"final_before_usec": timeutil.ToUsec(c.End()),
	}
	m, err := countsToMap(counts)
	if err != nil {
		return nil, err
	}
	// We need to use SQL expressions here to increment the counts without having
	// to first read them from the DB. Expressions need to explicitly reference
	// the SQL column name, which we can look up from the Go field name using the
	// schema.
	schema, err := ut.env.GetDBHandle().Schema(&tables.Usage{})
	if err != nil {
		return nil, err
	}
	for key, count := range m {
		fieldSchema := schema.LookUpField(key)
		if fieldSchema == nil {
			return nil, status.InvalidArgumentErrorf("Failed look get SQL column name for `Usages.%s`", key)
		}
		col := fieldSchema.DBName
		updates[col] = gorm.Expr(fmt.Sprintf("%s + ?", col), count)
	}
	return updates, nil
}

func (ut *tracker) currentCollectionPeriod() collectionPeriod {
	return collectionPeriodStartingAt(ut.clock.Now())
}

func (ut *tracker) oldestWritableCollectionPeriod() collectionPeriod {
	return collectionPeriodStartingAt(ut.clock.Now().Add(-redisKeyTTL))
}

func (ut *tracker) lastSettledCollectionPeriod() collectionPeriod {
	return collectionPeriodStartingAt(ut.clock.Now().Add(-(collectionPeriodDuration + collectionPeriodSettlingTime)))
}

// isSettled returns whether the given collection period will no longer have
// usage data written to it and is therefore safe to flush to the DB.
func (ut *tracker) isSettled(c collectionPeriod) bool {
	return !time.Time(c).After(time.Time(ut.lastSettledCollectionPeriod()))
}

// collectionPeriod is an interval of time starting at the beginning of a minute
// in UTC time and lasting one minute. Usage data is bucketed by collection
// period in Redis.
type collectionPeriod time.Time

func collectionPeriodStartingAt(t time.Time) collectionPeriod {
	utc := t.UTC()
	return collectionPeriod(time.Date(
		utc.Year(), utc.Month(), utc.Day(),
		utc.Hour(), utc.Minute(), 0, 0,
		utc.Location()))
}

func parseCollectionPeriod(s string) (collectionPeriod, error) {
	usec, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return collectionPeriodZeroValue, err
	}
	t := timeutil.FromUsec(usec)
	return collectionPeriodStartingAt(t), nil
}

func (c collectionPeriod) Start() time.Time {
	return time.Time(c)
}

func (c collectionPeriod) End() time.Time {
	return c.Start().Add(collectionPeriodDuration)
}

// Next returns the next collection period after this one.
func (c collectionPeriod) Next() collectionPeriod {
	return collectionPeriod(c.End())
}

// UsagePeriod returns the usage period that this collection period is contained
// within.
func (c collectionPeriod) UsagePeriod() usagePeriod {
	t := c.Start()
	return usagePeriod(time.Date(
		t.Year(), t.Month(), t.Day(),
		t.Hour(), 0, 0, 0,
		t.Location()))
}

// String returns a string uniquely identifying this collection period. It can
// later be reconstructed with parseCollectionPeriod.
func (c collectionPeriod) String() string {
	return fmt.Sprintf("%d", timeutil.ToUsec(c.Start()))
}

// usagePeriod is an interval of time starting at the beginning of each UTC hour
// and ending at the start of the following hour.
type usagePeriod time.Time

func (u usagePeriod) Start() time.Time {
	return time.Time(u)
}

func groupsRedisKey(c collectionPeriod) string {
	return fmt.Sprintf("%s%s", redisGroupsKeyPrefix, c)
}

func countsRedisKey(groupID string, c collectionPeriod) string {
	return fmt.Sprintf("%s%s/%s", redisCountsKeyPrefix, groupID, c)
}

func countsToMap(tu *tables.UsageCounts) (map[string]int64, error) {
	b, err := json.Marshal(tu)
	if err != nil {
		return nil, err
	}
	counts := map[string]int64{}
	if err := json.Unmarshal(b, &counts); err != nil {
		return nil, err
	}
	for k, v := range counts {
		if v == 0 {
			delete(counts, k)
		}
	}
	return counts, nil
}

// stringMapToCounts converts a Redis hashmap containing usage counts to
// tables.UsageCounts.
func stringMapToCounts(h map[string]string) (*tables.UsageCounts, error) {
	hInt64 := map[string]int64{}
	for k, v := range h {
		count, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("Invalid usage count in Redis hash: %q => %q", k, v)
		}
		hInt64[k] = count
	}
	b, err := json.Marshal(hInt64)
	if err != nil {
		return nil, err
	}
	tu := &tables.UsageCounts{}
	if err := json.Unmarshal(b, tu); err != nil {
		return nil, err
	}
	return tu, nil
}
