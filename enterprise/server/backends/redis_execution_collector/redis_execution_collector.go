package redis_execution_collector

import (
	"context"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/proto"

	iepb "github.com/buildbuddy-io/buildbuddy/proto/internal_execution"
)

const (
	redisExecutionKeyPrefix = "exec"
)

type collector struct {
	rdb  redis.UniversalClient
	rbuf *redisutil.CommandBuffer
}

func Register(env environment.Env) error {
	rdb := env.GetDefaultRedisClient()
	if rdb == nil {
		return nil
	}
	rbuf := redisutil.NewCommandBuffer(rdb)
	rbuf.StartPeriodicFlush(context.Background())
	env.GetHealthChecker().RegisterShutdownFunction(rbuf.StopPeriodicFlush)
	env.SetExecutionCollector(New(rdb, rbuf))
	return nil
}

func New(rdb redis.UniversalClient, rbuf *redisutil.CommandBuffer) *collector {
	return &collector{
		rdb:  rdb,
		rbuf: rbuf,
	}
}

func getKey(iid string) string {
	return strings.Join([]string{redisExecutionKeyPrefix, iid}, ":")
}

func (c *collector) Append(ctx context.Context, iid string, execution *iepb.Execution) error {
	b, err := proto.Marshal(execution)
	if err != nil {
		return err
	}
	return c.rbuf.RPush(ctx, getKey(iid), string(b))
}

func (c *collector) ListRange(ctx context.Context, iid string, start, stop int64) ([]*iepb.Execution, error) {
	serializedResults, err := c.rdb.LRange(ctx, getKey(iid), start, stop).Result()
	if err != nil {
		return nil, err
	}
	res := make([]*iepb.Execution, 0)
	for _, serializedResult := range serializedResults {
		execution := &iepb.Execution{}
		if err := proto.Unmarshal([]byte(serializedResult), execution); err != nil {
			return nil, err
		}
		res = append(res, execution)
	}
	return res, nil
}

func (c *collector) Delete(ctx context.Context, iid string) error {
	return c.rdb.Del(ctx, iid).Err()
}
