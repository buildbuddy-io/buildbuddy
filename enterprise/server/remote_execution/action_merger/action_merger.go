package action_merger

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/metadata"
)

const (
	// TTL for action-merging data about queued executions. This should be
	// approximately equal to the longest execution queue times.
	queuedExecutionTTL = 10 * time.Minute

	// The default TTL for action-merging data about claimed execution. This
	// is expressed in the number of "lease periods," which is defined in
	// `remote_execution.lease_duration` and should be thought of in terms of
	// how many missed execution-leases are needed to stop merging against a
	// given execution.
	DefaultClaimedExecutionLeasePeriods = 4

	// Redis Hash keys for storing information about action-merging.
	//
	// The execution ID of the canonical (first-submitted) execution
	executionIDKey = "execution-id"
	// The total number of running hedged executions for this action
	hedgedExecutionCountKey = "hedged-execution-count"
	// The time (in microseconds) at which the canonical execution was
	// submitted to the execution server.
	firstExecutionSubmitTimeKey = "first-execution-submit-time"
	// The time (in microseconds) at which the most recent execution (canonical
	// or hedged) was submitted to the execution server.
	lastExecutionSubmitTimeKey = "last-execution-submit-time"
	// The total number of submitted executions (canonical, hedged, and merged)
	// for this action.
	actionCountKey = "action-count"

	// The redis keys storing action merging data are versioned to support
	// making backwards-incompatible changes to the storage representation.
	// Increment this version to cycle to new keys (and discard all old
	// action-merging data) during the next rollout.
	keyVersion = 4

	// gRPC header that can be set to true to disable action merging.
	disableActionMergingHeaderName = "x-buildbuddy-disable-action-merging"
)

var (
	enableActionMerging = flag.Bool("remote_execution.enable_action_merging", true, "If enabled, identical actions being executed concurrently are merged into a single execution.")
	hedgedActionCount   = flag.Int("remote_execution.action_merging_hedge_count", 0, "When action merging is enabled, this flag controls how many additional, 'hedged' attempts an action is run in the background. Note that even hedged actions are run at most once per execution request.")
	hedgeAfterDelay     = flag.Duration("remote_execution.action_merging_hedge_delay", 0*time.Second, "When action merging hedging is enabled, up to --remote_execution.action_merging_hedge_count hedged actions are run with this delay of linear backoff.")
)

// Returns the redis key pointing to the hash storing action merging state. The
// value stored here is a hash containing the canonical (first-submitted)
// execution ID and the count of executions run for this action (for hedging).
func redisKeyForPendingExecutionID(ctx context.Context, adResource *digest.CASResourceName) (string, error) {
	userPrefix, err := prefix.UserPrefixFromContext(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		// The {}-wrapped part of the key determines which shard the key is
		// stored on. Here, we shard by action digest hash.
		"pendingExecution/%d/%s/%s/{%s}",
		keyVersion,
		userPrefix,
		strings.ToLower(adResource.GetDigestFunction().String()),
		adResource.GetDigest().GetHash(),
	), nil
}

func redisKeyForPendingExecutionDigest(executionID string) string {
	// TODO: pass in action resource name separately to avoid a hard dependency
	// on execution IDs containing action digests.
	s, err := executionIDWithDigestSharding(executionID)
	if err != nil {
		alert.UnexpectedEvent("action_merger_unexpected_execution_id", "%s", err)
		return fmt.Sprintf("pendingExecutionDigest/%d/%s", keyVersion, executionID)
	}
	return fmt.Sprintf("pendingExecutionDigest/%d/%s", keyVersion, s)
}

// Returns the execution ID with the hash part of the action digest wrapped with
// {}, so that when the ID is included in a redis key, the sharding is based
// only on the action digest hash.
func executionIDWithDigestSharding(executionID string) (string, error) {
	_, err := digest.ParseUploadResourceName(executionID)
	if err != nil {
		return "", status.InvalidArgumentErrorf("execution ID %q is not a valid upload resource name", executionID)
	}
	parts := strings.Split(executionID, "/")
	if len(parts) < 2 {
		return "", status.InvalidArgumentErrorf("execution ID %q is not a valid upload resource name", executionID)
	}
	parts[len(parts)-2] = "{" + parts[len(parts)-2] + "}"
	return strings.Join(parts, "/"), nil
}

func isDisabledByHeader(ctx context.Context) bool {
	disabledHeaders := metadata.ValueFromIncomingContext(ctx, disableActionMergingHeaderName)
	if len(disabledHeaders) == 0 {
		return false
	}
	return disabledHeaders[len(disabledHeaders)-1] == "true"
}

// This function records a claimed execution in Redis.
func RecordClaimedExecution(ctx context.Context, rdb redis.UniversalClient, executionID string, ttl time.Duration) error {
	if !*enableActionMerging {
		return nil
	}

	// Use the execution ID to lookup the action digest and insert the forward
	// entry (from action digest to execution ID).
	reverseKey := redisKeyForPendingExecutionDigest(executionID)
	forwardKey, err := rdb.Get(ctx, reverseKey).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}

	pipe := rdb.TxPipeline()
	pipe.HSet(ctx, forwardKey, executionIDKey, executionID)
	pipe.Expire(ctx, forwardKey, ttl)
	pipe.Set(ctx, reverseKey, forwardKey, ttl)
	_, err = pipe.Exec(ctx)
	return err
}

// This function records a hedged execution in Redis.
func RecordHedgedExecution(ctx context.Context, rdb redis.UniversalClient, adResource *digest.CASResourceName, groupIdForMetrics string) error {
	key, err := redisKeyForPendingExecutionID(ctx, adResource)
	if err != nil {
		return err
	}

	pipe := rdb.TxPipeline()
	pipe.HIncrBy(ctx, key, hedgedExecutionCountKey, 1)
	pipe.HSet(ctx, key, lastExecutionSubmitTimeKey, strconv.FormatInt(time.Now().UnixMicro(), 36))
	_, err = pipe.Exec(ctx)
	return err
}

// CheckMerged returns true if the specified action has been merged against.
func CheckMerged(ctx context.Context, rdb redis.UniversalClient, adResource *digest.CASResourceName) (bool, error) {
	key, err := redisKeyForPendingExecutionID(ctx, adResource)
	if err != nil {
		return false, err
	}
	rawCount, err := rdb.HGet(ctx, key, actionCountKey).Result()
	if err == redis.Nil {
		return false, nil
	}
	count, err := strconv.Atoi(rawCount)
	if err != nil {
		return false, err
	}
	return count > 1, nil
}

// This function records a merged execution in Redis.
func RecordMergedExecution(ctx context.Context, rdb redis.UniversalClient, adResource *digest.CASResourceName, groupIdForMetrics string) error {
	key, err := redisKeyForPendingExecutionID(ctx, adResource)
	if err != nil {
		return err
	}

	if err := rdb.HIncrBy(ctx, key, actionCountKey, 1).Err(); err != nil {
		return err
	}

	hash, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		log.Debugf("Error reading action-merging state from Redis: %s", err)
		return nil
	}
	recordCountMetric(hash, groupIdForMetrics)
	recordSubmitTimeOffsetMetric(hash, groupIdForMetrics)
	return nil
}

func recordCountMetric(hash map[string]string, groupIdForMetrics string) {
	rawCount, ok := hash[actionCountKey]
	if !ok {
		return
	}
	count, err := strconv.Atoi(rawCount)
	if err != nil {
		return
	}

	metrics.RemoteExecutionMergedActionsPerExecution.
		With(prometheus.Labels{metrics.GroupID: groupIdForMetrics}).
		Observe(float64(count))
}

func recordSubmitTimeOffsetMetric(hash map[string]string, groupIdForMetrics string) {
	rawSubmitTimeMicros, ok := hash[firstExecutionSubmitTimeKey]
	if !ok {
		return
	}
	submitTimeMicros, err := strconv.ParseInt(rawSubmitTimeMicros, 36, 64)
	if err != nil || submitTimeMicros <= 0 {
		return
	}
	submitTime := time.UnixMicro(submitTimeMicros)
	if submitTime.After(time.Now()) {
		return
	}

	metrics.RemoteExecutionMergedActionSubmitTimeOffsetUsec.
		With(prometheus.Labels{metrics.GroupID: groupIdForMetrics}).
		Observe(float64(time.Since(submitTime).Microseconds()))
}

type DispatchAction int

const (
	// New signals that this is the first attempt at execution for the given
	// action digest.
	New DispatchAction = iota
	// Merge signals that this is not the first execution for the given action
	// digest and the existing execution should be reused.
	Merge DispatchAction = iota
	// Hedge signals that this is not the first execution for the given action
	// digest, but an additional execution should be run in the background.
	Hedge DispatchAction = iota
)

// GetOrCreateExecutionID implements action merging by atomically checking if
// there is an existing execution for the given action digest and registering
// a new execution if not.
//
// The first return value is *always* a valid execution ID to be used for the
// execution. Errors are logged, but not returned to the caller as action
// merging is best-effort.
//
// The second return value indicates whether that execution ID is new (New), an
// existing execution that this execution request should be merged into (Merge),
// or an existing execution that should be hedged with a new one (Hedge).
//
// Action merging is an optimization that detects when an execution is
// requested for an action that is in-flight, but not yet in the action cache.
// This optimization is particularly helpful for preventing duplicate work for
// long-running actions. It is implemented as a pair of entries in redis, one
// from the action digest to the execution ID (the forward mapping), and one
// from the execution ID to the action digest (the reverse mapping). These
// entries are initially written to Redis atomically when an execution is
// enqueued, and then rewritten (to extend the TTL) while the action is running.
// Because the queueing mechanism isn't 100% reliable, it is possible for the
// merging data to exist in Redis pointing to a dead execution. For this reason,
// the TTLs are set somewhat conservatively so this problem self-heals
// reasonably quickly.
func GetOrCreateExecutionID(ctx context.Context, rdb redis.UniversalClient, schedulerService interfaces.SchedulerService, adResource *digest.CASResourceName, doNotCache bool) (string, DispatchAction) {
	newExecutionID := adResource.NewUploadString()
	if !*enableActionMerging || doNotCache || isDisabledByHeader(ctx) {
		return newExecutionID, New
	}
	forwardKey, err := redisKeyForPendingExecutionID(ctx, adResource)
	if err != nil {
		log.CtxDebugf(ctx, "Failed to compute redis key for execution %v: %v", adResource, err)
		return newExecutionID, New
	}
	var executionID string
	err = rdb.Watch(ctx, func(tx *redis.Tx) error {
		executionID, err = tx.HGet(ctx, forwardKey, executionIDKey).Result()
		if err == nil {
			// This key already exists and points to an existing execution.
			return nil
		}
		if !errors.Is(redis.Nil, err) {
			// An unexpected error occurred.
			return err
		}

		// We checked that the key doesn't exist under WATCH, so we can
		// safely create it with a transactional pipeline. If another
		// execution is created in the meantime, the pipeline will fail.
		reverseKey := redisKeyForPendingExecutionDigest(newExecutionID)
		nowString := strconv.FormatInt(time.Now().UnixMicro(), 36)
		pipe := tx.TxPipeline()
		pipe.HSetNX(ctx, forwardKey, executionIDKey, newExecutionID)
		pipe.HSetNX(ctx, forwardKey, firstExecutionSubmitTimeKey, nowString)
		pipe.HSet(ctx, forwardKey, lastExecutionSubmitTimeKey, nowString)
		pipe.HIncrBy(ctx, forwardKey, hedgedExecutionCountKey, 0)
		pipe.HIncrBy(ctx, forwardKey, actionCountKey, 1)
		pipe.Expire(ctx, forwardKey, queuedExecutionTTL)
		pipe.Set(ctx, reverseKey, forwardKey, queuedExecutionTTL)
		_, err = pipe.Exec(ctx)
		executionID = newExecutionID
		return err
	}, forwardKey)
	if err != nil && !errors.Is(err, redis.TxFailedErr) {
		// Unexpected redis error.
		log.CtxWarningf(ctx, "Unexpected redis error while registering execution for %s: %v", forwardKey, err)
		return newExecutionID, New
	}
	if errors.Is(err, redis.TxFailedErr) {
		// The pipeline may have failed because another execution was created
		// between the initial WATCH and the pipeline execution.
		executionID, err = rdb.HGet(ctx, forwardKey, executionIDKey).Result()
		if err != nil {
			log.CtxWarningf(ctx, "Transaction to register execution for %s failed with %v, but reading the key again failed with %v", forwardKey, redis.TxFailedErr, err)
			return newExecutionID, New
		}
	}

	if executionID == "" {
		log.CtxWarningf(ctx, "Failed to register or read execution ID for %s", forwardKey)
		return newExecutionID, New
	}
	if executionID == newExecutionID {
		// We won the race and created the first execution record.
		return newExecutionID, New
	}

	// At this point, there is an existing execution in some state. We need to
	// validate it before deciding whether to merge into it.
	hash, err := rdb.HGetAll(ctx, forwardKey).Result()
	if err != nil {
		log.CtxDebugf(ctx, "Error reading action-merging state from Redis: %s", err)
		return newExecutionID, New
	}

	// Validate that the reverse mapping exists as well. The reverse mapping is
	// used to delete the pending task information when the task is done.
	// Bail out if it doesn't exist.
	err = rdb.Get(ctx, redisKeyForPendingExecutionDigest(executionID)).Err()
	if errors.Is(err, redis.Nil) {
		log.CtxWarningf(ctx, "Pending execution %q does not exist in the scheduler", newExecutionID)
		return newExecutionID, New
	}
	if err != nil {
		log.CtxWarningf(ctx, "Unexpected redis error while reading reverse key for existing execution ID %q", newExecutionID)
		return newExecutionID, New
	}

	// The action merging state is recorded prior to scheduling the task. Verify
	// that the task is created in the scheduler before merging to avoid a
	// scenario where we reuse an execution ID that fails to schedule.
	err = retry.DoVoid(ctx, &retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		MaxBackoff:     3 * time.Second,
		Multiplier:     2,
		Name:           "Checking scheduler for pending execution",
	}, func(ctx context.Context) error {
		existsInScheduler, err := schedulerService.ExistsTask(ctx, executionID)
		if err != nil {
			log.CtxWarningf(ctx, "Error checking if pending execution %q exists in the scheduler: %s", newExecutionID, err)
			return retry.NonRetryableError(err)
		}
		if !existsInScheduler {
			return fmt.Errorf("pending execution %q does not exist in the scheduler yet", newExecutionID)
		}
		return nil
	})
	if err != nil {
		log.CtxWarningf(ctx, "Failed to check if pending execution %q exists in the scheduler: %s", newExecutionID, err)
		return newExecutionID, New
	}

	if shouldHedge(hash) {
		return executionID, Hedge
	} else {
		return executionID, Merge
	}
}

// Returns true if a hedged execution should be run given the provided
// action-merging hash from Redis.
func shouldHedge(hash map[string]string) bool {
	rawCount, ok := hash[hedgedExecutionCountKey]
	if !ok {
		return false
	}
	count, err := strconv.Atoi(rawCount)
	if err != nil {
		return false
	}
	if count >= *hedgedActionCount {
		return false
	}

	rawLastSubmitTimeMicros, ok := hash[lastExecutionSubmitTimeKey]
	if !ok {
		return false
	}
	lastSubmitTimeMicros, err := strconv.ParseInt(rawLastSubmitTimeMicros, 36, 64)
	if err != nil || lastSubmitTimeMicros <= 0 {
		return false
	}
	lastSubmitTime := time.UnixMicro(lastSubmitTimeMicros)
	return lastSubmitTime.Add(*hedgeAfterDelay).Before(time.Now())
}

// Deletes the pending execution with the provided execution ID.
func DeletePendingExecution(ctx context.Context, rdb redis.UniversalClient, executionID string) error {
	if !*enableActionMerging {
		return nil
	}

	pendingExecutionDigestKey := redisKeyForPendingExecutionDigest(executionID)
	pendingExecutionKey, err := rdb.Get(ctx, pendingExecutionDigestKey).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	if err := rdb.Del(ctx, pendingExecutionKey).Err(); err != nil {
		log.CtxWarningf(ctx, "could not delete pending execution key %q: %s", pendingExecutionKey, err)
	}
	if err := rdb.Del(ctx, pendingExecutionDigestKey).Err(); err != nil {
		log.CtxWarningf(ctx, "could not delete pending execution digest key %q: %s", pendingExecutionDigestKey, err)
	}
	return nil
}
