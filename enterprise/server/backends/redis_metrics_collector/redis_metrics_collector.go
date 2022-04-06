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
	rdb  redis.UniversalClient
	rbuf *redisutil.CommandBuffer
}

func New(rdb redis.UniversalClient, rbuf *redisutil.CommandBuffer) *collector {
	return &collector{
		rdb:  rdb,
		rbuf: rbuf,
	}
}

func (c *collector) IncrementCountsWithExpiry(ctx context.Context, key string, counts map[string]int64, expiry time.Duration) error {
	// Buffer writes to avoid high load on Redis.

	for field, n := range counts {
		if err := c.rbuf.HIncrBy(ctx, key, field, n); err != nil {
			return err
		}
	}
	// Set an expiration so keys don't stick around forever if we fail to clean
	// up.
	return c.rbuf.Expire(ctx, key, expiry)
}

func (c *collector) IncrementCounts(ctx context.Context, key string, counts map[string]int64) error {
	return c.IncrementCountsWithExpiry(ctx, key, counts, countExpiration)
}

func (c *collector) IncrementCountWithExpiry(ctx context.Context, key, field string, n int64, expiry time.Duration) error {
	// Buffer writes to avoid high load on Redis.
	if err := c.rbuf.HIncrBy(ctx, key, field, n); err != nil {
		return err
	}
	// Set an expiration so keys don't stick around forever if we fail to clean
	// up.
	return c.rbuf.Expire(ctx, key, expiry)
}

func (c *collector) IncrementCount(ctx context.Context, key, field string, n int64) error {
	return c.IncrementCountWithExpiry(ctx, key, field, n, countExpiration)
}

func (c *collector) SetAddWithExpiry(ctx context.Context, key string, expiry time.Duration, members ...string) error {
	membersIface := make([]interface{}, len(members))
	for i, member := range members {
		membersIface[i] = member
	}
	// Buffer writes to avoid high load on Redis.
	if err := c.rbuf.SAdd(ctx, key, membersIface...); err != nil {
		return err
	}
	// Set an expiration so keys don't stick around forever if we fail to clean
	// up.
	return c.rbuf.Expire(ctx, key, expiry)
}

func (c *collector) SetAdd(ctx context.Context, key string, members ...string) error {
	return c.SetAddWithExpiry(ctx, key, countExpiration, members...)
}

func (c *collector) SetMembers(ctx context.Context, key string) ([]string, error) {
	// TODO: convert redis.Nil to empty map?
	return c.rdb.SMembers(ctx, key).Result()
}

func (c *collector) HashSet(ctx context.Context, key, field string, value interface{}) error {
	return c.rbuf.HSet(ctx, key, field, value)
}

func (c *collector) HashGetAll(ctx context.Context, key string) (map[string]string, error) {
	// TODO: convert redis.Nil to empty map?
	return c.rdb.HGetAll(ctx, key).Result()
}

func (c *collector) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return c.rbuf.Set(ctx, key, value, expiration)
}

func (c *collector) Get(ctx context.Context, key string) (string, error) {
	return c.rdb.Get(ctx, key).Result()
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

func (c *collector) Flush(ctx context.Context) error {
	return c.rbuf.Flush(ctx)
}
