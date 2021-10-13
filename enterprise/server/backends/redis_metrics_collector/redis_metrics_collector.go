package redis_metrics_collector

import (
	"context"
	"strconv"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/go-redis/redis/v8"
)

const (
	// How long until counts expire from Redis.
	countExpiration = 3 * 24 * time.Hour
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

func (c *collector) IncrementCount(ctx context.Context, key, field string, n int64) error {
	// Buffer writes to avoid high load on Redis.
	if err := c.rbuf.HIncrBy(ctx, key, field, n); err != nil {
		return err
	}
	// Set an expiration so keys don't stick around forever if we fail to clean
	// up.
	if err := c.rbuf.Expire(ctx, key, countExpiration); err != nil {
		return err
	}
	return nil
}

func (c *collector) ReadCounts(ctx context.Context, key string) (map[string]int64, error) {
	h, err := c.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return make(map[string]int64), nil
		}
		return nil, err
	}

	counts := make(map[string]int64, len(h))
	for field, strCount := range h {
		count, err := strconv.ParseInt(strCount, 10, 64)
		if err != nil {
			return nil, err
		}
		counts[field] = count
	}
	return counts, nil
}

func (c *collector) Delete(ctx context.Context, key string) error {
	return c.rdb.Del(ctx, key).Err()
}
