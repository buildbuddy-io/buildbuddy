package usage

import (
	"context"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
	"github.com/go-redis/redis"
)

const (
	// collectionPeriodDuration determines the granularity at which we flush data
	// to the DB.
	//
	// NOTE: if this value is changed, then the implementation of
	// collectionPeriodStartUsec needs to change as well. Also note that if this
	// value is increased, we may lose some usage data for a short period of time,
	// since keys at smaller granularity may not be properly flushed to the DB.
	collectionPeriodDuration = 1 * time.Minute

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

	redisTimeKeyFormat = time.RFC3339
)

type TrackerOpts struct {
	Clock timeutil.Clock
}

type tracker struct {
	env   environment.Env
	rdb   *redis.Client
	clock timeutil.Clock
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
	return &tracker{
		env:   env,
		rdb:   rdb,
		clock: clock,
	}, nil
}

func (ut *tracker) Increment(ctx context.Context, uc *tables.UsageCounts) error {
	groupID, err := authutil.AuthenticatedGroupID(ctx, ut.env)
	if err != nil {
		if authutil.IsAnonymousUserError(err) && ut.env.GetConfigurator().GetAnonymousUsageEnabled() {
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

	t := ut.clock.Now()

	pipe := ut.rdb.TxPipeline()
	// Add the group ID to the set of groups with usage
	groupsCollectionPeriodKey := groupsRedisKey(t)
	pipe.SAdd(ctx, groupsCollectionPeriodKey, groupID)
	pipe.Expire(ctx, groupsCollectionPeriodKey, redisKeyTTL)
	// Increment the hash values
	groupIDCollectionPeriodKey := countsRedisKey(groupID, t)
	for countField, count := range counts {
		pipe.HIncrBy(ctx, groupIDCollectionPeriodKey, countField, count)
	}
	pipe.Expire(ctx, groupIDCollectionPeriodKey, redisKeyTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	return nil
}

func collectionPeriodStart(t time.Time) time.Time {
	utc := t.UTC()
	// Start of minute
	return time.Date(
		utc.Year(), utc.Month(), utc.Day(),
		utc.Hour(), utc.Minute(), 0, 0,
		utc.Location())
}

func groupsRedisKey(t time.Time) string {
	return fmt.Sprintf("%s%s", redisGroupsKeyPrefix, collectionPeriodStart(t).Format(redisTimeKeyFormat))
}

func countsRedisKey(groupID string, t time.Time) string {
	return fmt.Sprintf("%s%s/%s", redisCountsKeyPrefix, groupID, collectionPeriodStart(t).Format(redisTimeKeyFormat))
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
	return counts, nil
}
