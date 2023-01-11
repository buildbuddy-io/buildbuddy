package redis_execution_collector

import (
	"context"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
)

const (
	redisExecutionKeyPrefix      = "exec"
	redisInvocationKeyPrefix     = "invocation"
	redisInvocationLinkKeyPrefix = "invocationLink"

	invocationExpiration = 24 * time.Hour
)

type collector struct {
	rdb redis.UniversalClient
}

func Register(env environment.Env) error {
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
	return c.rdb.SAdd(ctx, getInvocationLinkKey(link.GetExecutionId()), string(b)).Err()
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
	return c.rdb.RPush(ctx, getExecutionKey(iid), string(b)).Err()
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
