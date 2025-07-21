package action_merger

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
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
	keyVersion = 3
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
	downloadString := adResource.DownloadString()
	return fmt.Sprintf("pendingExecution/%d/%s%s", keyVersion, userPrefix, downloadString), nil
}

func redisKeyForPendingExecutionDigest(executionID string) string {
	return fmt.Sprintf("pendingExecutionDigest/%d/%s", keyVersion, executionID)
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
	// NEW signals that this is the first attempt at execution for the given
	// action digest.
	NEW DispatchAction = iota
	// MERGE signals that this is not the first execution for the given action
	// digest and the existing execution should be reused.
	MERGE DispatchAction = iota
	// HEDGE signals that this is not the first execution for the given action
	// digest, but an additional execution should be run in the background.
	HEDGE DispatchAction = iota
)

// GetOrCreateExecutionID implements action merging by atomically checking if
// there is an existing execution for the given action digest and registering
// a new execution if not.
//
// The first return value is *always* a valid execution ID to be used for the
// execution. Errors are logged, but not returned to the caller as action
// merging is best-effort.
//
// The second return value indicates whether that execution ID is new (NEW), an
// existing execution that this execution request should be merged into (MERGE),
// or an existing execution that should be hedged with a new one (HEDGE).
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
	if !*enableActionMerging || doNotCache {
		return newExecutionID, NEW
	}
	forwardKey, err := redisKeyForPendingExecutionID(ctx, adResource)
	if err != nil {
		log.CtxDebugf(ctx, "Failed to compute redis key for execution %v: %v", adResource, err)
		return newExecutionID, NEW
	}
	var executionID string
	err = rdb.Watch(ctx, func(tx *redis.Tx) error {
		executionID, err = tx.HGet(ctx, forwardKey, executionIDKey).Result()
		if err == nil {
			// This key already exists and points to an existing execution.
			return nil
		}
		if err != redis.Nil {
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
	if err != nil && err != redis.TxFailedErr {
		// Unexpected redis error.
		log.CtxWarningf(ctx, "Unexpected redis error while registering execution for %s: %v", forwardKey, err)
		return newExecutionID, NEW
	}
	if err == redis.TxFailedErr {
		// The pipeline may have failed because another execution was created
		// between the initial WATCH and the pipeline execution.
		executionID, err = rdb.HGet(ctx, forwardKey, executionIDKey).Result()
		if err != nil {
			log.CtxWarningf(ctx, "Transaction to register execution for %s failed with %v, but reading the key again failed with %v", forwardKey, redis.TxFailedErr, err)
			return newExecutionID, NEW
		}
	}

	if executionID == "" {
		log.CtxWarningf(ctx, "Failed to register or read execution ID for %s", forwardKey)
		return newExecutionID, NEW
	}
	if executionID == newExecutionID {
		// We won the race and created the first execution record.
		return newExecutionID, NEW
	}

	// At this point, there is an existing execution in some state. We need to
	// validate it before deciding whether to merge into it.
	hash, err := rdb.HGetAll(ctx, forwardKey).Result()
	if err != nil {
		log.CtxDebugf(ctx, "Error reading action-merging state from Redis: %s", err)
		return newExecutionID, NEW
	}

	// Validate that the reverse mapping exists as well. The reverse mapping is
	// used to delete the pending task information when the task is done.
	// Bail out if it doesn't exist.
	err = rdb.Get(ctx, redisKeyForPendingExecutionDigest(executionID)).Err()
	if err == redis.Nil {
		log.CtxWarningf(ctx, "Pending execution %q does not exist in the scheduler", newExecutionID)
		return newExecutionID, NEW
	}
	if err != nil {
		log.CtxWarningf(ctx, "Unexpected redis error while reading reverse key for existing execution ID %q", newExecutionID)
		return newExecutionID, NEW
	}

	if shouldHedge(hash) {
		return executionID, HEDGE
	} else {
		return executionID, MERGE
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
