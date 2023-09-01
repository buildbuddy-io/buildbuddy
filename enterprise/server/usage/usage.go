package usage

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
	"github.com/go-redis/redis/v8"

	usage_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/usage/config"
)

var (
	region = flag.String("app.region", "", "The region in which the app is running.")
)

const (
	// periodDuration determines the length of time for usage data
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
	periodDuration = 1 * time.Minute

	// periodSettlingTime is the max length of time that we expect
	// usage data to be written to a usage period bucket in Redis after the
	// period has ended. This accounts for differences in clocks across
	// apps, Redis buffer flush delay (see redisutil.CommandBuffer), and latency
	// to Redis itself.
	periodSettlingTime = 10 * time.Second

	// redisKeyTTL defines how long usage keys have to live before they are
	// deleted automatically by Redis.
	//
	// Keys should live for at least 2 usage periods since periods
	// aren't finalized until the period is past, plus some wiggle room for Redis
	// latency. We add a few more periods on top of that, in case
	// flushing fails due to transient errors.
	redisKeyTTL = 5 * periodDuration

	// Redis storage layout for buffered usage counts (V2):
	//
	// "usage/collections/{period}" points to a set of "collection" objects
	// where each is an encoded `Collection` struct. The Collection struct is
	// effectively the usage row "key": group ID + label values.
	//
	// "usage/counts/{period}/{encode(collection)}" holds the usage counts for
	// the collection during the collection period.
	//
	// To do a flush, apps look at the N most recent collection periods which
	// are "settled" (i.e. no more data will be collected, and therefore ready
	// to be flushed). Then, they query "usage/collections/{period}" to get the
	// list of keys, then for each key, they look up the counts. The combined
	// (key, counts) are assembled into a Usage row and inserted into the DB.

	redisUsageKeyPrefix       = "usage/"
	redisCollectionsKeyPrefix = redisUsageKeyPrefix + "collections/"
	redisCountsKeyPrefix      = redisUsageKeyPrefix + "counts/"

	// Time format used to store Redis keys.
	// Example: 2020-01-01T00:00:00Z
	redisTimeKeyFormat = time.RFC3339

	// Key used to get a lock on Redis usage data. The lock is acquired using the
	// Redlock protocol. See https://redis.io/topics/distlock
	//
	// This lock is purely to reduce load on the DB. Flush jobs should be
	// able to run concurrently (without needing this lock) and still write the
	// correct usage data. The atomicity of DB writes is really what prevents
	// usage data from being overcounted.
	redisUsageLockKey = "lock.usage"

	// How long any given job can hold the usage lock for, before it expires
	// and other jobs may try to acquire it.
	redisUsageLockExpiry = 45 * time.Second

	// How often to wake up and attempt to flush usage data from Redis to the DB.
	flushInterval = periodDuration
)

var (
	periodZeroValue = periodStartingAt(time.Unix(0, 0))
)

// NewFlushLock returns a distributed lock that can be used with NewTracker
// to help serialize access to the usage data in Redis across apps.
func NewFlushLock(env environment.Env) (interfaces.DistributedLock, error) {
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
	lock, err := NewFlushLock(env)
	if err != nil {
		return err
	}
	ut, err := NewTracker(env, timeutil.NewClock(), lock)
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

func (ut *tracker) Increment(ctx context.Context, labels *tables.UsageLabels, uc *tables.UsageCounts) error {
	groupID, err := perms.AuthenticatedGroupID(ctx, ut.env)
	if err != nil {
		if authutil.IsAnonymousUserError(err) && ut.env.GetAuthenticator().AnonymousUsageEnabled(ctx) {
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

	t := ut.currentPeriod()

	collection := &Collection{
		GroupID:     groupID,
		UsageLabels: *labels,
	}
	// Increment the hash values
	encodedCollection := encodeCollection(collection)
	countsKey := countsRedisKey(t, encodedCollection)
	if err := ut.env.GetMetricsCollector().IncrementCountsWithExpiry(ctx, countsKey, counts, redisKeyTTL); err != nil {
		return status.WrapError(err, "increment counts in redis")
	}
	// Add the collection hash to the set of collections with usage
	if err := ut.env.GetMetricsCollector().SetAddWithExpiry(ctx, collectionsRedisKey(t), redisKeyTTL, encodedCollection); err != nil {
		return status.WrapError(err, "add collection hash to set in redis")
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
					alert.UnexpectedEvent("usage_data_flush_failed", "Error flushing usage data to DB: %s", err)
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

// FlushToDB flushes usage metrics from any finalized usage periods to the
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

	// Loop through usage periods starting from the oldest period
	// that may exist in Redis (based on key expiration time) and looping up until
	// we hit a period which is not yet "settled".
	for p := ut.oldestWritablePeriod(); ut.isSettled(p); p = p.Next() {
		// Read collections (JSON-serialized Collection structs)
		gk := collectionsRedisKey(p)
		encodedCollections, err := ut.rdb.SMembers(ctx, gk).Result()
		if err != nil {
			return err
		}
		if len(encodedCollections) == 0 {
			continue
		}

		for _, encodedCollection := range encodedCollections {
			ok, err := ut.supportsCollection(ctx, encodedCollection)
			if err != nil {
				return status.WrapError(err, "check DB schema supports collection")
			}
			if !ok {
				// Collection contains a new column; let a newer app flush
				// instead.
				log.Infof("Usage collection %q for period %s contains column not yet supported by this app; will let a newer app flush this period's data.", encodedCollection, p)
				return nil
			}
		}

		for _, encodedCollection := range encodedCollections {
			collection, _, err := decodeCollection(encodedCollection)
			if err != nil {
				return status.WrapError(err, "decode collection")
			}
			// Read usage counts from Redis
			ck := countsRedisKey(p, encodedCollection)
			h, err := ut.rdb.HGetAll(ctx, ck).Result()
			if err != nil {
				return err
			}
			counts, err := stringMapToCounts(h)
			if err != nil {
				return err
			}
			// Update counts in the DB
			if err := ut.flushCounts(ctx, collection.GroupID, p, &collection.UsageLabels, counts); err != nil {
				return err
			}
			// Clean up the counts from Redis. Don't delete the collection JSON
			// content though, since those aren't specific to collection periods
			// and they might still be needed. Instead, just let those expire.
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

func (ut *tracker) flushCounts(ctx context.Context, groupID string, p period, labels *tables.UsageLabels, counts *tables.UsageCounts) error {
	dbh := ut.env.GetDBHandle()
	return dbh.TransactionWithOptions(ctx, db.Opts().WithQueryName("insert_usage"), func(tx *db.DB) error {
		tu := &tables.Usage{
			GroupID:         groupID,
			PeriodStartUsec: p.Start().UnixMicro(),
			Region:          ut.region,
			UsageCounts:     *counts,
			UsageLabels:     *labels,
		}

		b, _ := json.Marshal(tu)
		json := string(b)
		log.Infof("Flushing usage row: %s", json)

		// Lock the table so that other apps cannot attempt to insert usage rows
		// with the same key. Note that this locking should not affect
		// performance since only one app should be writing to the DB at a time
		// anyway.
		unlock, err := tables.LockExclusive(tx, &tables.Usage{})
		if err != nil {
			return err
		}
		defer unlock()

		// First check whether the row already exists.
		err = tx.Raw(`
			SELECT *
			FROM "Usages"
			WHERE
				region = ?
				AND group_id = ?
				AND period_start_usec = ?
				AND origin = ?
				AND client = ?
			`+dbh.SelectForUpdateModifier(),
			tu.Region,
			tu.GroupID,
			tu.PeriodStartUsec,
			tu.Origin,
			tu.Client,
		).Take(&tables.Usage{}).Error
		if err != nil && !db.IsRecordNotFound(err) {
			return err
		}
		if err == nil {
			alert.UnexpectedEvent("usage_update_skipped", "Usage flush skipped since the row already exists. Usage row: %s", json)
			return nil
		}
		// Row doesn't exist yet; create.
		return tx.Create(tu).Error
	})
}

func (ut *tracker) currentPeriod() period {
	return periodStartingAt(ut.clock.Now())
}

func (ut *tracker) oldestWritablePeriod() period {
	return periodStartingAt(ut.clock.Now().Add(-redisKeyTTL))
}

func (ut *tracker) lastSettledPeriod() period {
	return periodStartingAt(ut.clock.Now().Add(-(periodDuration + periodSettlingTime)))
}

// isSettled returns whether the given period will no longer have
// usage data written to it and is therefore safe to flush to the DB.
func (ut *tracker) isSettled(c period) bool {
	return !time.Time(c).After(time.Time(ut.lastSettledPeriod()))
}

// Returns whether the given JSON representing a Collection struct contains
// only fields that are supported by this app; i.e. it returns false if the
// Collection was written by a newer app.
func (ut *tracker) supportsCollection(ctx context.Context, encodedCollection string) (bool, error) {
	_, vals, err := decodeCollection(encodedCollection)
	if err != nil {
		return false, nil
	}
	schema, err := db.TableSchema(ut.env.GetDBHandle().DB(ctx), &tables.Usage{})
	if err != nil {
		return false, err
	}
	for f, v := range vals {
		// If the field is empty, the app is setting the field but to an empty
		// value; we can tolerate this because the inserted usage row will
		// have an empty value by default.
		if len(v) == 0 || (len(v) == 1 && v[0] == "") {
			continue
		}
		if _, ok := schema.FieldsByDBName[f]; !ok {
			return false, nil
		}
	}
	return true, nil
}

// period is an interval of time starting at the beginning of a minute
// in UTC time and lasting one minute. Usage data is stored at this granularity
// both in Redis and the DB.
type period time.Time

func periodStartingAt(t time.Time) period {
	utc := t.UTC()
	return period(time.Date(
		utc.Year(), utc.Month(), utc.Day(),
		utc.Hour(), utc.Minute(), 0, 0,
		utc.Location()))
}

func parseCollectionPeriod(s string) (period, error) {
	usec, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return periodZeroValue, err
	}
	t := time.UnixMicro(usec)
	return periodStartingAt(t), nil
}

func (c period) Start() time.Time {
	return time.Time(c)
}

func (c period) End() time.Time {
	return c.Start().Add(periodDuration)
}

// Next returns the next period after this one.
func (c period) Next() period {
	return period(c.End())
}

// String returns a string uniquely identifying this usage period. It can
// later be reconstructed with parsePeriod.
func (c period) String() string {
	return c.Start().Format(redisTimeKeyFormat)
}

type Collection struct {
	// TODO: maybe make GroupID a field of tables.UsageLabels.
	GroupID string
	tables.UsageLabels
}

// encodeCollection encodes the collection to a human readable format.
func encodeCollection(c *Collection) string {
	// Using a handwritten encoding scheme for performance reasons (this
	// runs on every cache request).
	s := "group_id=" + c.GroupID
	if c.UsageLabels.Origin != "" {
		s += "&origin=" + url.QueryEscape(c.UsageLabels.Origin)
	}
	if c.UsageLabels.Client != "" {
		s += "&client=" + url.QueryEscape(c.UsageLabels.Client)
	}
	return s
}

// decodeCollection decodes a string encoded using encodeCollection.
// It returns the raw url.Values so that apps can detect collections encoded
// by newer apps.
func decodeCollection(s string) (*Collection, url.Values, error) {
	q, err := url.ParseQuery(s)
	if err != nil {
		return nil, nil, err
	}
	c := &Collection{
		GroupID: q.Get("group_id"),
		UsageLabels: tables.UsageLabels{
			// Note: these need to match the DB field names.
			Origin: q.Get("origin"),
			Client: q.Get("client"),
		},
	}
	return c, q, nil
}

func collectionsRedisKey(c period) string {
	return fmt.Sprintf("%s%s", redisCollectionsKeyPrefix, c)
}

func countsRedisKey(c period, encodedCollection string) string {
	return fmt.Sprintf("%s%s/%s", redisCountsKeyPrefix, c, encodedCollection)
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
	if tu.TotalUploadSizeBytes > 0 {
		counts["total_upload_size_bytes"] = tu.TotalUploadSizeBytes
	}
	if tu.TotalCachedActionExecUsec > 0 {
		counts["total_cached_action_exec_usec"] = tu.TotalCachedActionExecUsec
	}
	if tu.CPUNanos > 0 {
		counts["cpu_nanos"] = tu.CPUNanos
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
		TotalUploadSizeBytes:       hInt64["total_upload_size_bytes"],
		TotalCachedActionExecUsec:  hInt64["total_cached_action_exec_usec"],
		CPUNanos:                   hInt64["cpu_nanos"],
	}, nil
}
