package usage

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
	"github.com/go-redis/redis/v8"

	usage_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/usage/config"
)

var region = flag.String("app.region", "", "The region in which the app is running.")

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
	// apps, Redis buffer flush delay (see redisutil.CommandBuffer), and latency
	// to Redis itself.
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

	// Time format used to store Redis keys.
	// Example: 2020-01-01T00:00:00Z
	redisTimeKeyFormat = time.RFC3339

	// Key used to get a lock on Redis usage data. The lock is acquired using the
	// Redlock protocol. See https://redis.io/topics/distlock
	//
	// This lock is purely to reduce load on the DB. Flush jobs should be
	// able to run concurrently (without needing this lock) and still write the
	// correct usage data. The atomicity of DB writes, combined with the
	// fact that we write usage data in monotonically increasing order of
	// timestamp, is really what prevents usage data from being overcounted.
	redisUsageLockKey = "lock.usage"

	// How long any given job can hold the usage lock for, before it expires
	// and other jobs may try to acquire it.
	redisUsageLockExpiry = 45 * time.Second

	// How often to wake up and attempt to flush usage data from Redis to the DB.
	flushInterval = collectionPeriodDuration
)

var (
	collectionPeriodZeroValue = collectionPeriodStartingAt(time.Unix(0, 0))
)

// NewFlushLock returns a distributed lock that can be used with NewTracker
// to help serialize access to the usage data in Redis across apps.
func NewFlushLock(env environment.Env) interfaces.DistributedLock {
	return redisutil.NewWeakLock(env.GetDefaultRedisClient(), redisUsageLockKey, redisUsageLockExpiry)
}

type tracker struct {
	env    environment.Env
	rdb    redis.UniversalClient
	clock  timeutil.Clock
	region string

	flushLock interfaces.DistributedLock
	stopFlush chan struct{}
}

func RegisterTracker(env environment.Env) error {
	if !usage_config.UsageTrackingEnabled() {
		return nil
	}
	ut, err := NewTracker(env, timeutil.NewClock(), NewFlushLock(env))
	if err != nil {
		return err
	}
	env.SetUsageTracker(ut)
	ut.StartDBFlush()
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		ut.StopDBFlush()
		return nil
	})
	return nil
}

func NewTracker(env environment.Env, clock timeutil.Clock, flushLock interfaces.DistributedLock) (*tracker, error) {
	if *region == "" {
		return nil, status.FailedPreconditionError("Usage tracking requires app.region to be configured.")
	}
	if env.GetDefaultRedisClient() == nil {
		return nil, status.FailedPreconditionError("Usage tracking is enabled, but no Redis client is configured.")
	}
	if env.GetMetricsCollector() == nil {
		return nil, status.FailedPreconditionError("Metrics Collector must be configured for usage tracker.")
	}
	return &tracker{
		env:       env,
		rdb:       env.GetDefaultRedisClient(),
		region:    *region,
		clock:     clock,
		flushLock: flushLock,
		stopFlush: make(chan struct{}),
	}, nil
}

func (ut *tracker) Increment(ctx context.Context, uc *tables.UsageCounts) error {
	groupID, err := perms.AuthenticatedGroupID(ctx, ut.env)
	if err != nil {
		if authutil.IsAnonymousUserError(err) && ut.env.GetAuthenticator().AnonymousUsageEnabled() {
			// Don't track anonymous usage for now.
			return nil
		}
		return err
	}

	counts, err := countsToMap(uc)
	if err != nil {
		return err
	}
	if len(counts) == 0 {
		return nil
	}

	t := ut.currentCollectionPeriod()

	// Add the group ID to the set of groups with usage
	groupsCollectionPeriodKey := groupsRedisKey(t)
	if err := ut.env.GetMetricsCollector().SetAddWithExpiry(ctx, groupsCollectionPeriodKey, redisKeyTTL, groupID); err != nil {
		return err
	}
	// Increment the hash values
	countsKey := countsRedisKey(groupID, t)
	if err := ut.env.GetMetricsCollector().IncrementCountsWithExpiry(ctx, countsKey, counts, redisKeyTTL); err != nil {
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
	// Grab lock. This will immediately return ResourceExhausted if
	// another client already holds the lock. In that case, we ignore the error.
	err := ut.flushLock.Lock(ctx)
	if status.IsResourceExhaustedError(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() {
		if err := ut.flushLock.Unlock(ctx); err != nil {
			log.Warningf("Failed to unlock distributed lock: %s", err)
		}
	}()
	return ut.flushToDB(ctx)
}

func (ut *tracker) flushToDB(ctx context.Context) error {
	// Don't run for longer than we have the Redis lock. This is mostly just to
	// avoid situations where multiple apps are trying to write usage data to the
	// DB at once while it is already under high load.
	ctx, cancel := context.WithTimeout(ctx, redisUsageLockExpiry)
	defer cancel()

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
			// Read usage counts from Redis
			ck := countsRedisKey(groupID, c)
			h, err := ut.rdb.HGetAll(ctx, ck).Result()
			if err != nil {
				return err
			}
			counts, err := stringMapToCounts(h)
			if err != nil {
				return err
			}
			// Update counts in the DB
			if err := ut.flushCounts(ctx, groupID, c, counts); err != nil {
				return err
			}
			// Clean up the counts from Redis
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

func (ut *tracker) flushCounts(ctx context.Context, groupID string, c collectionPeriod, counts *tables.UsageCounts) error {
	pk := &tables.Usage{
		GroupID:         groupID,
		PeriodStartUsec: c.UsagePeriod().Start().UnixMicro(),
		Region:          ut.region,
	}
	dbh := ut.env.GetDBHandle()
	return dbh.Transaction(ctx, func(tx *db.DB) error {
		// Create a row for the corresponding usage period if one doesn't already
		// exist.
		res := tx.Exec(`
			INSERT `+dbh.InsertIgnoreModifier()+` INTO Usages (
				group_id,
				period_start_usec,
				region,
				final_before_usec,
				invocations,
				cas_cache_hits,
				action_cache_hits,
				total_download_size_bytes,
				linux_execution_duration_usec,
				mac_execution_duration_usec
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			`,
			pk.GroupID,
			pk.PeriodStartUsec,
			pk.Region,
			c.End().UnixMicro(),
			counts.Invocations,
			counts.CASCacheHits,
			counts.ActionCacheHits,
			counts.TotalDownloadSizeBytes,
			counts.LinuxExecutionDurationUsec,
			counts.MacExecutionDurationUsec,
		)
		if err := res.Error; err != nil {
			return err
		}
		// If we inserted successfully, no need to update.
		if res.RowsAffected > 0 {
			return nil
		}
		// Update the usage row, but only if collection period data has not already
		// been written (for example, if the previous flush failed to delete the
		// data from Redis).
		return tx.Exec(`
			UPDATE Usages
			SET
				final_before_usec = ?,
				invocations = invocations + ?,
				cas_cache_hits = cas_cache_hits + ?,
				action_cache_hits = action_cache_hits + ?,
				total_download_size_bytes = total_download_size_bytes + ?,
				linux_execution_duration_usec = linux_execution_duration_usec + ?,
				mac_execution_duration_usec = mac_execution_duration_usec + ?
			WHERE
				group_id = ?
				AND period_start_usec = ?
				AND region = ?
				AND final_before_usec <= ?
		`,
			c.End().UnixMicro(),
			counts.Invocations,
			counts.CASCacheHits,
			counts.ActionCacheHits,
			counts.TotalDownloadSizeBytes,
			counts.LinuxExecutionDurationUsec,
			counts.MacExecutionDurationUsec,
			pk.GroupID,
			pk.PeriodStartUsec,
			pk.Region,
			c.Start().UnixMicro(),
		).Error
	})
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
	t := time.UnixMicro(usec)
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
// within. A usage period corresponds to the coarse-level time range of usage
// rows in the DB (1 hour), while a collection period corresponds to the more
// fine-grained time ranges of Redis keys (1 minute).
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
	return c.Start().Format(redisTimeKeyFormat)
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
	counts := map[string]int64{}
	if tu.ActionCacheHits > 0 {
		counts["action_cache_hits"] = tu.ActionCacheHits
	}
	if tu.CASCacheHits > 0 {
		counts["cas_cache_hits"] = tu.CASCacheHits
	}
	if tu.Invocations > 0 {
		counts["invocations"] = tu.Invocations
	}
	if tu.TotalDownloadSizeBytes > 0 {
		counts["total_download_size_bytes"] = tu.TotalDownloadSizeBytes
	}
	if tu.LinuxExecutionDurationUsec > 0 {
		counts["linux_execution_duration_usec"] = tu.LinuxExecutionDurationUsec
	}
	if tu.MacExecutionDurationUsec > 0 {
		counts["mac_execution_duration_usec"] = tu.MacExecutionDurationUsec
	}
	return counts, nil
}

// stringMapToCounts converts a Redis hashmap containing usage counts to
// tables.UsageCounts.
func stringMapToCounts(h map[string]string) (*tables.UsageCounts, error) {
	hInt64 := make(map[string]int64, len(h))
	for k, v := range h {
		count, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("Invalid usage count in Redis hash: %q => %q", k, v)
		}
		hInt64[k] = count
	}
	return &tables.UsageCounts{
		Invocations:                hInt64["invocations"],
		CASCacheHits:               hInt64["cas_cache_hits"],
		ActionCacheHits:            hInt64["action_cache_hits"],
		TotalDownloadSizeBytes:     hInt64["total_download_size_bytes"],
		LinuxExecutionDurationUsec: hInt64["linux_execution_duration_usec"],
		MacExecutionDurationUsec:   hInt64["mac_execution_duration_usec"],
	}, nil
}
