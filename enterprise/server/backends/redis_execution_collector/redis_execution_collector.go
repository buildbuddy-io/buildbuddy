package redis_execution_collector

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/go-redis/redis/v8"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
)

const (
	redisExecutionKeyPrefix                = "exec"
	redisInvocationKeyPrefix               = "invocation"
	redisInvocationLinkKeyPrefix           = "invocationLink"
	redisExecutionRequestMetadataKeyPrefix = "execReqMetadata"

	invocationExpiration     = 24 * time.Hour
	invocationLinkExpiration = 24 * time.Hour
	executionExpiration      = 24 * time.Hour
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

func getExecutionRequestMetadataKey(executionID string) string {
	return strings.Join([]string{redisExecutionRequestMetadataKeyPrefix, executionID}, "/")
}

func getInvocationKey(iid string) string {
	return strings.Join([]string{redisInvocationKeyPrefix, iid}, "/")
}

func getInvocationLinkKey(executionID string) string {
	return strings.Join([]string{redisInvocationLinkKeyPrefix, executionID}, "/")
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

func (c *collector) AddInvocationLink(ctx context.Context, link *sipb.StoredInvocationLink) error {
	b, err := proto.Marshal(link)
	if err != nil {
		return err
	}
	key := getInvocationLinkKey(link.GetExecutionId())
	pipe := c.rdb.TxPipeline()
	pipe.SAdd(ctx, key, string(b))
	pipe.Expire(ctx, key, invocationLinkExpiration)
	_, err = pipe.Exec(ctx)
	return err
}

func (c *collector) GetInvocationLinks(ctx context.Context, executionID string) ([]*sipb.StoredInvocationLink, error) {
	serializedResults, err := c.rdb.SMembers(ctx, getInvocationLinkKey(executionID)).Result()
	if err != nil {
		return nil, err
	}
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

func (c *collector) AddExecutionRequestMetadata(ctx context.Context, executionID string, metadata *repb.RequestMetadata) error {
	b, err := proto.Marshal(metadata)
	if err != nil {
		return err
	}
	key := getExecutionRequestMetadataKey(executionID)
	pipe := c.rdb.TxPipeline()
	if result := pipe.RPush(ctx, key, b); result.Err() != nil {
		return result.Err()
	}
	if result := pipe.Expire(ctx, key, executionExpiration); result.Err() != nil {
		return result.Err()
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	return nil
}

func (c *collector) GetExecutionRequestMetadata(ctx context.Context, executionID string) (*repb.RequestMetadata, error) {
	key := getExecutionRequestMetadataKey(executionID)
	res, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	m := &repb.RequestMetadata{}
	if err := proto.Unmarshal([]byte(res), m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *collector) GetExecutionRequestMetadatas(ctx context.Context, executionIDs []string) (map[string]*repb.RequestMetadata, error) {
	keys := make([]string, 0, len(executionIDs))
	for _, id := range executionIDs {
		keys = append(keys, getExecutionRequestMetadataKey(id))
	}
	allResp, err := c.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	execIDToMetadata := make(map[string]*repb.RequestMetadata, len(keys))
	for i, resp := range allResp {
		respBody, ok := resp.([]byte)
		if !ok {
			log.Warningf("unknown data type from Redis")
			continue
		}
		m := &repb.RequestMetadata{}
		if err := proto.Unmarshal(respBody, m); err != nil {
			log.Warningf("failed to unmarshal request metadata from Redis: %v", err)
			continue
		}
		// MGet preserves the order of the request keys
		execIDToMetadata[executionIDs[i]] = m
	}
	return execIDToMetadata, nil
}

func (c *collector) DeleteExecutionRequestMetadata(ctx context.Context, executionID string) error {
	return c.rdb.Del(ctx, getExecutionRequestMetadataKey(executionID)).Err()
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
