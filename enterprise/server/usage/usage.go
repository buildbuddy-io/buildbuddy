package usage

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/go-redis/redis/v8"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"

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
	redisUsageLockExpiry = 50 * time.Second

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
	clock  clockwork.Clock
	region string

	flushLock interfaces.DistributedLock
	stopFlush chan struct{}
}

func RegisterTracker(env *real_environment.RealEnv) error {
	if !usage_config.UsageTrackingEnabled() {
		return nil
	}
	lock, err := NewFlushLock(env)
	if err != nil {
		return err
	}
	ut, err := NewTracker(env, env.GetClock(), lock)
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

func NewTracker(env environment.Env, clock clockwork.Clock, flushLock interfaces.DistributedLock) (*tracker, error) {
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

// emitMetrics emit metrics that are eventually exposed to consumers.
func (ut *tracker) emitMetrics(groupID string, origin string, uc *tables.UsageCounts) {
	if origin == "" {
		origin = "external"
	}
	exportedLabels := prometheus.Labels{metrics.GroupID: groupID, metrics.CacheRequestOrigin: origin}
	if uc.TotalDownloadSizeBytes > 0 {
		metrics.CacheDownloadSizeBytesExported.With(exportedLabels).Add(float64(uc.TotalDownloadSizeBytes))
	}

	if uc.TotalUploadSizeBytes > 0 {
		metrics.CacheUploadSizeBytesExported.With(exportedLabels).Add(float64(uc.TotalUploadSizeBytes))
	}

	if uc.CASCacheHits > 0 {
		hitLabels := prometheus.Labels{metrics.GroupID: groupID, metrics.CacheTypeLabel: "cas"}
		metrics.CacheNumHitsExported.With(hitLabels).Add(float64(uc.CASCacheHits))
	}
	if uc.ActionCacheHits > 0 {
		hitLabels := prometheus.Labels{metrics.GroupID: groupID, metrics.CacheTypeLabel: "action"}
		metrics.CacheNumHitsExported.With(hitLabels).Add(float64(uc.ActionCacheHits))
	}
	if uc.LinuxExecutionDurationUsec > 0 {
		execLabels := prometheus.Labels{metrics.GroupID: groupID, metrics.OS: platform.LinuxOperatingSystemName}
		metrics.RemoteExecutionDurationUsecExported.With(execLabels).Observe(float64(uc.LinuxExecutionDurationUsec))
	}
	if uc.MacExecutionDurationUsec > 0 {
		execLabels := prometheus.Labels{metrics.GroupID: groupID, metrics.OS: platform.DarwinOperatingSystemName}
		metrics.RemoteExecutionDurationUsecExported.With(execLabels).Observe(float64(uc.MacExecutionDurationUsec))
	}
}

func (ut *tracker) Increment(ctx context.Context, labels *tables.UsageLabels, uc *tables.UsageCounts) error {
	u, err := ut.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		if authutil.IsAnonymousUserError(err) && ut.env.GetAuthenticator().AnonymousUsageEnabled(ctx) {
			// Don't track anonymous usage for now.
			return nil
		}
		return err
	}
	groupID := u.GetGroupID()

	counts, err := countsToMap(uc)
	if err != nil {
		return err
	}
	if len(counts) == 0 {
		return nil
	}

	t := ut.currentPeriod()

	collection := &usageutil.Collection{
		GroupID: groupID,
		Origin:  labels.Origin,
		Client:  labels.Client,
		Server:  labels.Server,
	}
	// Increment the hash values
	encodedCollection := usageutil.EncodeCollection(collection)
	countsKey := countsRedisKey(t, encodedCollection)
	if err := ut.env.GetMetricsCollector().IncrementCountsWithExpiry(ctx, countsKey, counts, redisKeyTTL); err != nil {
		return status.WrapError(err, "increment counts in redis")
	}
	// Add the collection hash to the set of collections with usage
	if err := ut.env.GetMetricsCollector().SetAddWithExpiry(ctx, collectionsRedisKey(t), redisKeyTTL, encodedCollection); err != nil {
		return status.WrapError(err, "add collection hash to set in redis")
	}

	ut.emitMetrics(groupID, labels.Origin, uc)
	return nil
}

// StartDBFlush starts a goroutine that periodically flushes usage data from
// Redis to the DB.
func (ut *tracker) StartDBFlush() {
	go func() {
		ctx := context.Background()
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
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
	deadline := time.Now().Add(redisUsageLockExpiry)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	// Reserve some of the context duration for cleanup. If a DB flush succeeds,
	// then we want to make sure we have a little more time to clean up the
	// Redis data corresponding to the flushed row while we still have the Redis
	// lock held.
	redisCleanupCtx := ctx
	ctx, cancel = context.WithDeadline(ctx, deadline.Add(-5*time.Second))
	defer cancel()

	// Loop through usage periods starting from the oldest period
	// that may exist in Redis (based on key expiration time) and looping up until
	// we hit a period which is not yet "settled".
	oldestPeriod := ut.oldestWritablePeriod()
	for p := oldestPeriod; ut.isSettled(p); p = p.Next() {
		// Read collections (JSON-serialized Collection structs)
		collectionsKey := collectionsRedisKey(p)
		encodedCollections, err := ut.rdb.SMembers(ctx, collectionsKey).Result()
		if err != nil {
			return err
		}
		if len(encodedCollections) == 0 {
			continue
		}

		if p.Equal(oldestPeriod) {
			alert.UnexpectedEvent("usage_flush_not_keeping_up", "Flushing usage data that is close to redis TTL - some usage data may be lost")
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
			collection, _, err := usageutil.DecodeCollection(encodedCollection)
			if err != nil {
				return status.WrapError(err, "decode collection")
			}
			// Read usage counts from Redis
			countsKey := countsRedisKey(p, encodedCollection)
			h, err := ut.rdb.HGetAll(ctx, countsKey).Result()
			if err != nil {
				return err
			}
			if len(h) == 0 {
				alert.UnexpectedEvent("usage_unexpected_empty_hash_in_redis", "Usage counts in Redis are unexpectedly empty for key %q", countsKey)
				continue
			}
			counts, err := stringMapToCounts(h)
			if err != nil {
				return err
			}
			// Update counts in the DB
			if err := ut.flushCounts(ctx, collection.GroupID, p, collection.UsageLabels(), counts); err != nil {
				return err
			}
			// Remove the collection data from Redis now that it has been
			// flushed to the DB.
			pipe := ut.rdb.TxPipeline()
			pipe.SRem(redisCleanupCtx, collectionsKey, encodedCollection)
			pipe.Del(redisCleanupCtx, countsKey)
			if _, err := pipe.Exec(redisCleanupCtx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ut *tracker) flushCounts(ctx context.Context, groupID string, p period, labels *tables.UsageLabels, counts *tables.UsageCounts) error {
	dbh := ut.env.GetDBHandle()
	return dbh.Transaction(ctx, func(tx interfaces.DB) error {
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
		unlock, err := tables.LockExclusive(tx.GORM(ctx, "usage_lock_table"), &tables.Usage{})
		if err != nil {
			return err
		}
		defer unlock()

		// First check whether the row already exists.
		err = tx.NewQuery(ctx, "usage_check_exists").Raw(`
			SELECT *
			FROM "Usages"
			WHERE
				region = ?
				AND group_id = ?
				AND period_start_usec = ?
				AND origin = ?
				AND client = ?
				AND server = ?
			`+dbh.SelectForUpdateModifier(),
			tu.Region,
			tu.GroupID,
			tu.PeriodStartUsec,
			tu.Origin,
			tu.Client,
			tu.Server,
		).Take(&tables.Usage{})
		if err != nil && !db.IsRecordNotFound(err) {
			return err
		}
		if err == nil {
			alert.UnexpectedEvent("usage_update_skipped", "Usage flush skipped since the row already exists. Usage row: %s", json)
			return nil
		}
		// Row doesn't exist yet; create.
		tu.UsageID, err = tables.PrimaryKeyForTable(tu.TableName())
		if err != nil {
			return err
		}
		return tx.NewQuery(ctx, "usage_insert_record").Create(tu)
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
	_, vals, err := usageutil.DecodeCollection(encodedCollection)
	if err != nil {
		return false, nil
	}
	schema, err := db.TableSchema(ut.env.GetDBHandle().GORM(ctx, "usage_service_get_schema"), &tables.Usage{})
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

func (c period) Equal(o period) bool {
	return time.Time(c).Equal(time.Time(o))
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
	if tu.SelfHostedLinuxExecutionDurationUsec > 0 {
		counts["self_hosted_linux_execution_duration_usec"] = tu.SelfHostedLinuxExecutionDurationUsec
	}
	if tu.SelfHostedMacExecutionDurationUsec > 0 {
		counts["self_hosted_mac_execution_duration_usec"] = tu.SelfHostedMacExecutionDurationUsec
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
	if tu.MemoryGBUsec > 0 {
		counts["memory_gb_usec"] = tu.MemoryGBUsec
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
		Invocations:                          hInt64["invocations"],
		CASCacheHits:                         hInt64["cas_cache_hits"],
		ActionCacheHits:                      hInt64["action_cache_hits"],
		TotalDownloadSizeBytes:               hInt64["total_download_size_bytes"],
		LinuxExecutionDurationUsec:           hInt64["linux_execution_duration_usec"],
		MacExecutionDurationUsec:             hInt64["mac_execution_duration_usec"],
		SelfHostedLinuxExecutionDurationUsec: hInt64["self_hosted_linux_execution_duration_usec"],
		SelfHostedMacExecutionDurationUsec:   hInt64["self_hosted_mac_execution_duration_usec"],
		TotalUploadSizeBytes:                 hInt64["total_upload_size_bytes"],
		TotalCachedActionExecUsec:            hInt64["total_cached_action_exec_usec"],
		CPUNanos:                             hInt64["cpu_nanos"],
		MemoryGBUsec:                         hInt64["memory_gb_usec"],
	}, nil
}
