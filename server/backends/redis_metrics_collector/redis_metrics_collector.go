package redis_metrics_collector

import (
	"context"

	"github.com/go-redis/redis"
)

type collector struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *collector {
	return &collector{rdb}
}

func (c *collector) IncrementCount(ctx context.Context, counterName string, n int64) error {
	return c.rdb.IncrBy(ctx, counterName, n).Result()
}

func (c *collector) ReadCount(ctx context.Context, counterName string) (int64, error) {
	return c.rdb.IncrBy(ctx, counterName, 0).Result()
}
