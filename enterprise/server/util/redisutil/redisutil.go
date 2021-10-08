package redisutil

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/extra/redisotel/v8"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"

	redsync_goredis "github.com/go-redsync/redsync/v4/redis/goredis/v8"
)

const (
	// How long to wait between flushing CommandBuffer instances to Redis.
	commandBufferFlushInterval = 250 * time.Millisecond
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
		log.Errorf(
			"Failed to parse redis URI %q: %s - The supported redis URI formats are:\n"+
				"redis[s]://[[USER][:PASSWORD]@][HOST][:PORT]"+
				"[/DATABASE]\nor:\nunix://[[USER][:PASSWORD]@]"+
				"SOCKET_PATH[?db=DATABASE]\nIf using a socket path, "+
				"path must be an absolute unix path.", redisTarget, err)
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

// NewWeakLock returns a distributed lock implemented using a single Redis
// client. This provides a "weak" implementation of the Redlock algorithm,
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

// CommandBuffer buffers and aggregates Redis commands in-memory and allows
// periodically flushing the aggregate results in batch. This is useful for
// reducing the load placed on Redis in cases where a high volume of commands
// are operating on a relatively small subset of Redis keys (for example,
// counter values that are incremented several times over a short time period).
//
// When using this buffer, the caller is responsible for explicitly flushing the
// data. In most cases, flushing should be done at regular intervals, as well as
// after writing the last command to the buffer, if applicable.
//
// There may be a non-negligible delay between the time that the data is written
// to the buffer and the time that data is flushed, so callers may need extra
// synchronization in order to ensure that buffers are flushed (on all nodes, if
// applicable), before relying on the values of any keys updated via this
// buffer.
//
// It is safe for concurrent access.
//
// When the server is shutting down, all commands are issued synchronously
// rather than adding them to the buffer. This is to reduce the likelihood of
// data being lost from the buffer due to the server shutting down. In practice,
// not much new data should be written to the buffer during shutdown since we
// don't accept new RPCs while shutting down.
type CommandBuffer struct {
	rdb       *redis.Client
	stopFlush chan struct{}

	mu sync.Mutex // protects all fields below
	// Buffer for INCRBY commands.
	incr map[string]int64
	// Buffer for HINCRBY commands.
	hincr map[string]map[string]int64
	// Buffer for SADD commands.
	sadd map[string]map[interface{}]struct{}
	// Buffer for EXPIRE commands.
	expire map[string]time.Duration
	// Whether the server is shutting down.
	isShuttingDown bool
}

// NewCommandBuffer creates a buffer.
func NewCommandBuffer(rdb *redis.Client) *CommandBuffer {
	c := &CommandBuffer{rdb: rdb}
	c.init()
	return c
}

func (c *CommandBuffer) init() {
	c.incr = map[string]int64{}
	c.hincr = map[string]map[string]int64{}
	c.sadd = map[string]map[interface{}]struct{}{}
	c.expire = map[string]time.Duration{}
}

// IncrBy adds an INCRBY operation to the buffer.
//
// If the server is shutting down, the command will be issued to Redis
// synchronously using the given context. Otherwise, the command is added to
// the buffer and the context is ignored.
func (c *CommandBuffer) IncrBy(ctx context.Context, key string, increment int64) error {
	c.mu.Lock()
	if c.isShuttingDown {
		c.mu.Unlock()
		return c.rdb.IncrBy(ctx, key, increment).Err()
	}
	defer c.mu.Unlock()

	c.incr[key] += increment
	return nil
}

// HIncrBy adds an HINCRBY operation to the buffer.
//
// If the server is shutting down, the command will be issued to Redis
// synchronously using the given context. Otherwise, the command is added to
// the buffer and the context is ignored.
func (c *CommandBuffer) HIncrBy(ctx context.Context, key, field string, increment int64) error {
	c.mu.Lock()
	if c.isShuttingDown {
		c.mu.Unlock()
		return c.rdb.HIncrBy(ctx, key, field, increment).Err()
	}
	defer c.mu.Unlock()

	h, ok := c.hincr[key]
	if !ok {
		h = map[string]int64{}
		c.hincr[key] = h
	}
	h[field] += increment
	return nil
}

// SAdd adds an SADD operation to the buffer. All buffered members of the set
// will be added in a single SADD command.
//
// If the server is shutting down, the command will be issued to Redis
// synchronously using the given context. Otherwise, the command is added to
// the buffer and the context is ignored.
func (c *CommandBuffer) SAdd(ctx context.Context, key string, members ...interface{}) error {
	c.mu.Lock()
	if c.isShuttingDown {
		c.mu.Unlock()
		return c.rdb.SAdd(ctx, key, members...).Err()
	}
	defer c.mu.Unlock()

	set, ok := c.sadd[key]
	if !ok {
		set = make(map[interface{}]struct{}, len(members))
		c.sadd[key] = set
	}
	for _, m := range members {
		set[m] = struct{}{}
	}
	return nil
}

// Expire adds an EXPIRE operation to the buffer, overwriting any previous
// expiry currently buffered for the given key. The duration is applied
// as-is when flushed to Redis, meaning that any time elapsed until the flush
// does not count towards the expiration duration.
//
// Because the expirations are relative to when the buffer is flushed, the
// buffer does not actively apply expirations to its own keys or delete keys
// from Redis when the buffered expiration times have elapsed. This means there
// are some corner cases where a sequence of buffered commands does not behave
// the same as if those commands were issued directly to Redis. For example,
// consider the following sequence of buffered commands:
//
//     INCRBY key 1
//     EXPIRE key 0
//     EXPIRE key 3600
//
// The `EXPIRE key 0` command is effectively dropped from the buffer since the
// later `EXPIRE` command overwrites it, whereas Redis would delete the key
// immediately upon seeing the `EXPIRE key 0` command.
func (c *CommandBuffer) Expire(ctx context.Context, key string, duration time.Duration) error {
	c.mu.Lock()
	if c.isShuttingDown {
		c.mu.Unlock()
		return c.rdb.Expire(ctx, key, duration).Err()
	}
	defer c.mu.Unlock()

	c.expire[key] = duration
	return nil
}

// Flush flushes the buffered commands to Redis. The commands needed to flush
// the buffers are issued in serial. If a command fails, the error is returned
// and the rest of the commands in the series are not executed, and their data
// is dropped from the buffer.
func (c *CommandBuffer) Flush(ctx context.Context) error {
	c.mu.Lock()
	incr := c.incr
	hincr := c.hincr
	sadd := c.sadd
	expire := c.expire
	// Set all fields to fresh values so the current ones can be flushed without
	// keeping a hold on the lock.
	c.init()
	c.mu.Unlock()

	for key, increment := range incr {
		if _, err := c.rdb.IncrBy(ctx, key, increment).Result(); err != nil {
			return err
		}
	}
	for key, h := range hincr {
		for field, increment := range h {
			if _, err := c.rdb.HIncrBy(ctx, key, field, increment).Result(); err != nil {
				return err
			}
		}
	}
	for key, set := range sadd {
		members := []interface{}{}
		for member, _ := range set {
			members = append(members, member)
		}
		if _, err := c.rdb.SAdd(ctx, key, members...).Result(); err != nil {
			return err
		}
	}
	for key, duration := range expire {
		if _, err := c.rdb.Expire(ctx, key, duration).Result(); err != nil {
			return err
		}
	}

	return nil
}

// StartPeriodicFlush starts a loop that periodically flushes buffered commands
// to Redis.
func (c *CommandBuffer) StartPeriodicFlush(ctx context.Context) {
	c.stopFlush = make(chan struct{})
	go func() {
		for {
			select {
			case <-c.stopFlush:
				return
			case <-time.After(commandBufferFlushInterval):
				if err := c.Flush(ctx); err != nil {
					log.Errorf("Failed to flush Redis command buffer: %s", err)
				}
			}
		}
	}()
}

// StopPeriodicFlush stops flushing the buffer to Redis. If a flush is currently
// in progress, this will block until the flush is complete.
func (c *CommandBuffer) StopPeriodicFlush(ctx context.Context) error {
	c.stopFlush <- struct{}{}

	c.mu.Lock()
	// Set a flag to make any newly written commands to go directly to Redis.
	c.isShuttingDown = true
	c.mu.Unlock()

	// Since all newly written commands will now go directly to Redis and we
	// just stopped the background flusher, we're on the hook for flushing any
	// remaining buffer contents.
	return c.Flush(ctx)
}
