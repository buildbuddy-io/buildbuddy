package redis_execution_collector

import (
	"context"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/go-redis/redis/v8"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
)

const (
	// Redis key prefix for mapping an invocation ID to the executions waiting
	// for the invocation to complete before they can be flushed to ClickHouse.
	redisExecutionKeyPrefix = "exec"

	// Redis key prefix for mapping an invocation ID to finalized invocation
	// metadata.
	redisInvocationKeyPrefix = "invocation"

	// Redis key prefix for mapping execution ID to invocation-execution links.
	redisInvocationLinkKeyPrefix = "invocationLink"

	// Redis key prefix for mapping invocation ID to invocation-execution links.
	redisReverseInvocationLinkKeyPrefix = "reverseInvocationLink"

	// Redis key prefix for mapping execution ID to a list of execution updates,
	// which are just partial StoredExecution protos. This list will contain the
	// initial execution metadata as well as changes to the "stage" field as the
	// execution progresses. It also contains the final execution state once the
	// execution is complete, but only for a short duration, since the execution
	// is normally moved to the ClickHouse flush queue shortly after completion.
	redisExecutionUpdatesKeyPrefix = "executionUpdates"

	invocationExpiration            = 24 * time.Hour
	invocationLinkExpiration        = 24 * time.Hour
	reverseInvocationLinkExpiration = 24 * time.Hour
	executionExpiration             = 24 * time.Hour
	executionUpdatesExpiration      = 24 * time.Hour
)

var (
	// Whether to write invocation => execution links.
	writeReverseInvocationLinks bool
)

type collector struct {
	rdb redis.UniversalClient
}

func Register(env *real_environment.RealEnv) error {
	rdb := env.GetDefaultRedisClient()
	if rdb == nil {
		return nil
	}
	env.SetExecutionCollector(New(rdb))
	return nil
}

func New(rdb redis.UniversalClient) *collector {
	return &collector{
		rdb: rdb,
	}
}

func getExecutionKey(iid string) string {
	return strings.Join([]string{redisExecutionKeyPrefix, iid}, "/")
}

func getInvocationKey(iid string) string {
	return strings.Join([]string{redisInvocationKeyPrefix, iid}, "/")
}

func getInvocationLinkKey(executionID string) string {
	return strings.Join([]string{redisInvocationLinkKeyPrefix, executionID}, "/")
}

func getReverseInvocationLinkKey(invocationID string) string {
	return strings.Join([]string{redisReverseInvocationLinkKeyPrefix, invocationID}, "/")
}

func getExecutionUpdatesKey(executionID string) string {
	return strings.Join([]string{redisExecutionUpdatesKeyPrefix, executionID}, "/")
}

func (c *collector) AddInvocation(ctx context.Context, inv *sipb.StoredInvocation) error {
	b, err := proto.Marshal(inv)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, getInvocationKey(inv.GetInvocationId()), string(b), invocationExpiration).Err()
}

func (c *collector) GetInvocation(ctx context.Context, iid string) (*sipb.StoredInvocation, error) {
	key := getInvocationKey(iid)
	serializedStoredInv, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	res := &sipb.StoredInvocation{}
	if err := proto.Unmarshal([]byte(serializedStoredInv), res); err != nil {
		return nil, err
	}
	return res, nil
}

func (c *collector) AddInvocationLink(ctx context.Context, link *sipb.StoredInvocationLink, storeReverseLink bool) error {
	b, err := proto.Marshal(link)
	if err != nil {
		return err
	}
	key := getInvocationLinkKey(link.GetExecutionId())
	pipe := c.rdb.TxPipeline()
	s := string(b)
	pipe.SAdd(ctx, key, s)
	pipe.Expire(ctx, key, invocationLinkExpiration)
	if storeReverseLink {
		key := getReverseInvocationLinkKey(link.GetInvocationId())
		pipe.SAdd(ctx, key, s)
		pipe.Expire(ctx, key, reverseInvocationLinkExpiration)
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (c *collector) GetInvocationLinks(ctx context.Context, executionID string) ([]*sipb.StoredInvocationLink, error) {
	serializedResults, err := c.rdb.SMembers(ctx, getInvocationLinkKey(executionID)).Result()
	if err != nil {
		return nil, err
	}
	return unmarshalStoredInvocationLinks(serializedResults)
}

func (c *collector) getReverseInvocationLinks(ctx context.Context, invocationID string) ([]*sipb.StoredInvocationLink, error) {
	serializedResults, err := c.rdb.SMembers(ctx, getReverseInvocationLinkKey(invocationID)).Result()
	if err != nil {
		return nil, err
	}
	return unmarshalStoredInvocationLinks(serializedResults)
}

func (c *collector) GetInProgressExecutions(ctx context.Context, invocationID string) ([]*repb.StoredExecution, error) {
	links, err := c.getReverseInvocationLinks(ctx, invocationID)
	if err != nil {
		return nil, err
	}
	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.StringSliceCmd, len(links))
	for i, link := range links {
		cmds[i] = pipe.LRange(ctx, getExecutionUpdatesKey(link.GetExecutionId()), 0, -1)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	// For each execution, combine the events into a single StoredExecution
	// proto.
	var executions []*repb.StoredExecution
	for _, cmd := range cmds {
		serializedResults, err := cmd.Result()
		if len(serializedResults) == 0 || err == redis.Nil {
			continue
		}
		if err != nil {
			return nil, err
		}
		execution, err := mergeExecutionUpdates(serializedResults)
		if err != nil {
			return nil, err
		}
		executions = append(executions, execution)
	}
	return executions, nil
}

// UpdateInProgressExecution updates the given in-progress execution in Redis.
// The completed execution state is also written using this method, but should
// be followed by a call to AppendExecution which queues the execution to be
// flushed to ClickHouse.
func (c *collector) UpdateInProgressExecution(ctx context.Context, execution *repb.StoredExecution) error {
	b, err := proto.Marshal(execution)
	if err != nil {
		return err
	}
	// To avoid having to read the current execution, just push the update to a
	// list; we can merge updates when reading them back. The updates are mostly
	// orthogonal except for in-progress updates (which are usually only written
	// once), so this approach should be comparable to a read-modify-write
	// approach in terms of data stored.
	pipe := c.rdb.TxPipeline()
	pipe.RPush(ctx, getExecutionUpdatesKey(execution.GetExecutionId()), string(b))
	pipe.Expire(ctx, getExecutionUpdatesKey(execution.GetExecutionId()), executionUpdatesExpiration)
	_, err = pipe.Exec(ctx)
	return err
}

// GetInProgressExecution reads the current state of an in-progress execution.
func (c *collector) GetInProgressExecution(ctx context.Context, executionID string) (*repb.StoredExecution, error) {
	serializedResults, err := c.rdb.LRange(ctx, getExecutionUpdatesKey(executionID), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	if len(serializedResults) == 0 {
		return nil, nil
	}
	return mergeExecutionUpdates(serializedResults)
}

// DeleteInProgressExecution deletes the given in-progress execution.
func (c *collector) DeleteInProgressExecution(ctx context.Context, executionID string) error {
	// TODO: maybe also proactively delete the reverse invocation links for this
	// execution, since reverse links are only used to point back to the
	// in-progress executions for an invocation. Those links will get cleaned up
	// when the invocation is complete, but cleaning them up here would reduce
	// the amount of unnecessary data fetched in the invocation page for
	// executions that have already been completed. For now, this is probably
	// not a big enough issue to justify the additional reads from redis and
	// additional complexity.
	pipe := c.rdb.Pipeline()
	pipe.Del(ctx, getExecutionUpdatesKey(executionID)).Err()
	_, err := pipe.Exec(ctx)
	return err
}

func (c *collector) AppendExecution(ctx context.Context, iid string, execution *repb.StoredExecution) error {
	b, err := proto.Marshal(execution)
	if err != nil {
		return err
	}
	key := getExecutionKey(iid)
	pipe := c.rdb.TxPipeline()
	pipe.RPush(ctx, key, string(b))
	pipe.Expire(ctx, key, executionExpiration)
	_, err = pipe.Exec(ctx)
	return err
}

func (c *collector) GetExecutions(ctx context.Context, iid string, start, stop int64) ([]*repb.StoredExecution, error) {
	serializedResults, err := c.rdb.LRange(ctx, getExecutionKey(iid), start, stop).Result()
	if err != nil {
		return nil, err
	}
	res := make([]*repb.StoredExecution, 0)
	for _, serializedResult := range serializedResults {
		execution := &repb.StoredExecution{}
		if err := proto.Unmarshal([]byte(serializedResult), execution); err != nil {
			return nil, err
		}
		res = append(res, execution)
	}
	return res, nil
}

func (c *collector) DeleteExecutions(ctx context.Context, iid string) error {
	return c.rdb.Del(ctx, getExecutionKey(iid)).Err()
}

func (c *collector) DeleteInvocationLinks(ctx context.Context, executionID string) error {
	return c.rdb.Del(ctx, getInvocationLinkKey(executionID)).Err()
}

func (c *collector) DeleteReverseInvocationLinks(ctx context.Context, invocationID string) error {
	return c.rdb.Del(ctx, getReverseInvocationLinkKey(invocationID)).Err()
}

func unmarshalStoredInvocationLinks(serializedResults []string) ([]*sipb.StoredInvocationLink, error) {
	res := make([]*sipb.StoredInvocationLink, 0)
	for _, serializedResult := range serializedResults {
		link := &sipb.StoredInvocationLink{}
		if err := proto.Unmarshal([]byte(serializedResult), link); err != nil {
			return nil, err
		}
		res = append(res, link)
	}
	return res, nil
}

func mergeExecutionUpdates(serializedResults []string) (*repb.StoredExecution, error) {
	out := &repb.StoredExecution{}
	for _, serializedResult := range serializedResults {
		event := &repb.StoredExecution{}
		if err := proto.Unmarshal([]byte(serializedResult), event); err != nil {
			return nil, err
		}
		// Copy fields from the latest event to the output proto. proto.Merge()
		// is a convenient way to do this. Note that this will concatenate
		// repeated fields, which is fine for now since repeated fields are not
		// currently populated as execution events.
		proto.Merge(out, event)

		// The scheduler attempts to prevent concurrent execution updates via
		// task leasing, but this is not currently 100% reliable. So we ignore
		// updates that happen after the initial attempt.
		if event.GetStage() == int64(repb.ExecutionStage_COMPLETED) {
			break
		}
	}
	return out, nil
}
