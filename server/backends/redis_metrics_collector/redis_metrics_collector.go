package redis_metrics_collector

import (
	"context"

	"github.com/go-redis/redis"

)

type redisMetricsCollector struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *redisMetricsCollector {
	return &redisMetricsCollector{rdb}
}

func (c *redisMetricsCollector) IncrementCount(ctx context.Context, counterName string, n int64) (int64, error) {
	return c.rdb.IncrBy(ctx, counterName, n).Result()
}

func (c *redisMetricsCollector) ReadCount(ctx context.Context, counterName string) (int64, error) {
	return c.rdb.IncrBy(ctx, counterName, 0).Result()
}
