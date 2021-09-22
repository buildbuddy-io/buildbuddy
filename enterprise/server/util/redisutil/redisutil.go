package redisutil

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/extra/redisotel/v8"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"

	redsync_goredis "github.com/go-redsync/redsync/v4/redis/goredis/v8"
)

func isRedisURI(redisTarget string) bool {
	return strings.HasPrefix(redisTarget, "redis://") ||
		strings.HasPrefix(redisTarget, "rediss://") ||
		strings.HasPrefix(redisTarget, "unix://")
}

func TargetToOptions(redisTarget string) *redis.Options {
	if !isRedisURI(redisTarget) {
		return &redis.Options{
			Addr:     redisTarget,
			Password: "", // no password set
			DB:       0,  // use default DB
		}
	} else if opt, err := redis.ParseURL(redisTarget); err == nil {
		return opt
	} else {
		log.Println(err)
		log.Println(
			"The supported redis URI formats are:\n" +
				"redis[s]://[[USER][:PASSWORD]@][HOST][:PORT]" +
				"[/DATABASE]\nor:\nunix://[[USER][:PASSWORD]@]" +
				"SOCKET_PATH[?db=DATABASE]\nIf using a socket path, " +
				"path must be an absolute unix path.")
		return &redis.Options{}
	}
}

type HealthChecker struct {
	Rdb *redis.Client
}

func (c *HealthChecker) Check(ctx context.Context) error {
	return c.Rdb.Ping(ctx).Err()
}

func NewClient(redisTarget string, checker interfaces.HealthChecker, healthCheckName string) *redis.Client {
	return NewClientWithOpts(TargetToOptions(redisTarget), checker, healthCheckName)
}

func NewClientWithOpts(opts *redis.Options, checker interfaces.HealthChecker, healthCheckName string) *redis.Client {
	redisClient := redis.NewClient(opts)
	redisClient.AddHook(redisotel.NewTracingHook())
	checker.AddHealthCheck(healthCheckName, &HealthChecker{Rdb: redisClient})
	return redisClient
}

type redlock struct {
	rmu *redsync.Mutex
}

// NewWeakRedlock returns a distributed lock implemented using a single Redis
// client. This provides a "weak" implementation of the Redlock algorithm
// in the sense that:
//
// (a) It is not resilient to Redis nodes failing (even if
//     the given Redis client points to a Redis cluster)
// (b) It does not retry failed locking attempts.
//
// See https://redis.io/topics/distlock
func NewWeakLock(rdb *redis.Client, key string, expiry time.Duration) *redlock {
	pool := redsync_goredis.NewPool(rdb)
	rs := redsync.New(pool)
	rmu := rs.NewMutex(key, redsync.WithTries(1), redsync.WithExpiry(expiry))
	return &redlock{rmu}
}

func (r *redlock) Lock(ctx context.Context) error {
	err := r.rmu.LockContext(ctx)
	if err == nil {
		return nil
	}
	if err == redsync.ErrFailed {
		return status.ResourceExhaustedErrorf("Failed to acquire redis lock: %s", err)
	}
	return err
}

func (r *redlock) Unlock(ctx context.Context) error {
	_, err := r.rmu.UnlockContext(ctx)
	return err
}
