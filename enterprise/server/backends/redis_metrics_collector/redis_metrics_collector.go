package redis_metrics_collector

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/go-redis/redis"
)

type collector struct {
	rdb  *redis.Client
	rbuf *redisutil.CommandBuffer
}

func New(rdb *redis.Client, rbuf *redisutil.CommandBuffer) *collector {
	return &collector{
		rdb:  rdb,
		rbuf: rbuf,
	}
}

func (c *collector) IncrementCount(ctx context.Context, counterName string, n int64) error {
	// Buffer writes to avoid high load on Redis.
	c.rbuf.IncrBy(counterName, n)
	return nil
}

func (c *collector) ReadCount(ctx context.Context, counterName string) (int64, error) {
	return c.rdb.IncrBy(ctx, counterName, 0).Result()
}
