package usage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
	"github.com/go-redis/redis"
	"github.com/jonboulle/clockwork"
)

const (
	redisUsageKeyPrefix       = "usage/"
	redisCountsKeyPrefix      = redisUsageKeyPrefix + "counts/"
	redisInvocationsKeyPrefix = redisUsageKeyPrefix + "invocations/"
)

type TrackerOpts struct {
	Clock clockwork.Clock
}

type tracker struct {
	env   environment.Env
	rdb   *redis.Client
	clock clockwork.Clock
}

func NewTracker(env environment.Env) (*tracker, error) {
	return NewTrackerWithOpts(env, &TrackerOpts{})
}

func NewTrackerWithOpts(env environment.Env, opts *TrackerOpts) (*tracker, error) {
	rdb := env.GetUsageRedisClient()
	if rdb == nil {
		return nil, status.UnimplementedError("Missing redis client for usage")
	}
	clock := opts.Clock
	if clock == nil {
		clock = clockwork.NewRealClock()
	}
	return &tracker{
		env:   env,
		rdb:   rdb,
		clock: clock,
	}, nil
}

func (ut *tracker) Increment(ctx context.Context, counts *tables.UsageCounts) error {
	groupID, err := authenticatedGroupID(ctx, ut.env)
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

	uk := countsRedisKey(groupID, ut.clock.Now())
	pipe := ut.rdb.TxPipeline()
	for k, c := range m {
		pipe.HIncrBy(ctx, uk, k, c)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	return nil
}

func (ut *tracker) ObserveInvocation(ctx context.Context, invocationID string) error {
	groupID, err := authenticatedGroupID(ctx, ut.env)
	if err != nil {
		return err
	}
	if groupID == "" {
		// Don't track anonymous usage for now.
		return nil
	}

	ik := invocationsRedisKey(groupID, ut.clock.Now())
	if _, err := ut.rdb.SAdd(ctx, ik, invocationID).Result(); err != nil {
		return err
	}

	return nil
}

func collectionPeriodStartUsec(t time.Time) int64 {
	utc := t.UTC()
	// Start of minute
	start := time.Date(
		utc.Year(), utc.Month(), utc.Day(),
		utc.Hour(), utc.Minute(), 0, 0,
		utc.Location())
	return timeutil.ToUsec(start)
}

func countsRedisKey(groupID string, t time.Time) string {
	return fmt.Sprintf("%s%s/%d", redisCountsKeyPrefix, groupID, collectionPeriodStartUsec(t))
}

func invocationsRedisKey(groupID string, t time.Time) string {
	return fmt.Sprintf("%s%s/%d", redisInvocationsKeyPrefix, groupID, collectionPeriodStartUsec(t))
}

func authenticatedGroupID(ctx context.Context, env environment.Env) (string, error) {
	u, err := perms.AuthenticatedUser(ctx, env)
	if err != nil {
		if status.IsUnauthenticatedError(err) || status.IsPermissionDeniedError(err) || status.IsUnimplementedError(err) {
			// Anonymous user.
			return "", nil
		}
		return "", err
	}
	groupID := u.GetGroupID()
	if groupID == "" {
		return "", status.FailedPreconditionError("Authenticated user does not have an associated group ID")
	}
	return groupID, nil
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
