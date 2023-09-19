package redis_client

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	remote_execution_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/config"
)

var (
	defaultRedisTarget          = flag.String("app.default_redis_target", "", "A Redis target for storing remote shared state. To ease migration, the redis target from the remote execution config will be used if this value is not specified.", flag.Secret)
	defaultRedisShards          = flag.Slice("app.default_sharded_redis.shards", []string{}, "Ordered list of Redis shard addresses.")
	defaultShardedRedisUsername = flag.String("app.default_sharded_redis.username", "", "Redis username")
	defaultShardedRedisPassword = flag.String("app.default_sharded_redis.password", "", "Redis password", flag.Secret)

	// Cache Redis
	// TODO: We need to deprecate one of the redis targets here or distinguish them
	cacheRedisTargetFallback  = flag.String("cache.redis_target", "", "A redis target for improved Caching/RBE performance. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **", flag.Secret)
	cacheRedisTarget          = flag.String("cache.redis.redis_target", "", "A redis target for improved Caching/RBE performance. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **", flag.Secret)
	cacheRedisShards          = flag.Slice("cache.redis.sharded.shards", []string{}, "Ordered list of Redis shard addresses.")
	cacheShardedRedisUsername = flag.String("cache.redis.sharded.username", "", "Redis username")
	cacheShardedRedisPassword = flag.String("cache.redis.sharded.password", "", "Redis password", flag.Secret)

	// Remote Execution Redis
	remoteExecRedisTarget          = flag.String("remote_execution.redis_target", "", "A Redis target for storing remote execution state. Falls back to app.default_redis_target if unspecified. Required for remote execution. To ease migration, the redis target from the cache config will be used if neither this value nor app.default_redis_target are specified.", flag.Secret)
	remoteExecRedisShards          = flag.Slice("remote_execution.sharded_redis.shards", []string{}, "Ordered list of Redis shard addresses.")
	remoteExecShardedRedisUsername = flag.String("remote_execution.sharded_redis.username", "", "Redis username")
	remoteExecShardedRedisPassword = flag.String("remote_execution.sharded_redis.password", "", "Redis password", flag.Secret)
)

type ShardedRedisConfig struct {
	Shards   []string `yaml:"shards" usage:"Ordered list of Redis shard addresses."`
	Username string   `yaml:"username" usage:"Redis username"`
	Password string   `yaml:"password" usage:"Redis password" config:"secret"`
}

func defaultRedisClientOptsNoFallback() *redisutil.Opts {
	if opts := redisutil.ShardsToOpts(*defaultRedisShards, *defaultShardedRedisUsername, *defaultShardedRedisPassword); opts != nil {
		return opts
	}
	return redisutil.TargetToOpts(*defaultRedisTarget)
}

func cacheRedisClientOptsNoFallback() *redisutil.Opts {
	// Prefer the client configs from Redis sub-config, is present.
	if opts := redisutil.ShardsToOpts(*cacheRedisShards, *cacheShardedRedisUsername, *cacheShardedRedisPassword); opts != nil {
		return opts
	}
	if opts := redisutil.TargetToOpts(*cacheRedisTarget); opts != nil {
		return opts
	}
	return redisutil.TargetToOpts(*cacheRedisTargetFallback)
}

func remoteExecutionRedisClientOptsNoFallback() *redisutil.Opts {
	if !remote_execution_config.RemoteExecutionEnabled() {
		return nil
	}
	if opts := redisutil.ShardsToOpts(*remoteExecRedisShards, *remoteExecShardedRedisUsername, *remoteExecShardedRedisPassword); opts != nil {
		return opts
	}
	return redisutil.TargetToOpts(*remoteExecRedisTarget)
}

func DefaultRedisClientOpts() *redisutil.Opts {
	if cfg := defaultRedisClientOptsNoFallback(); cfg != nil {
		return cfg
	}

	if cfg := cacheRedisClientOptsNoFallback(); cfg != nil {
		// Fall back to the cache redis client config if default redis target is not specified.
		return cfg
	}

	// Otherwise, fall back to the remote exec redis target.
	return remoteExecutionRedisClientOptsNoFallback()
}

func CacheRedisClientOpts() *redisutil.Opts {
	return cacheRedisClientOptsNoFallback()
}

func RemoteExecutionRedisClientOpts() *redisutil.Opts {
	if cfg := remoteExecutionRedisClientOptsNoFallback(); cfg != nil {
		return cfg
	}

	if cfg := defaultRedisClientOptsNoFallback(); cfg != nil {
		return cfg
	}

	return CacheRedisClientOpts()
}

func RegisterRemoteExecutionRedisClient(env environment.Env) error {
	opts := RemoteExecutionRedisClientOpts()
	if opts == nil {
		return nil
	}
	redisClient, err := redisutil.NewClientWithOpts(opts, env.GetHealthChecker(), "remote_execution_redis")
	if err != nil {
		return status.InternalErrorf("Failed to create Remote Execution redis client: %s", err)
	}
	env.SetRemoteExecutionRedisClient(redisClient)
	return nil
}

func RegisterDefault(env environment.Env) error {
	opts := DefaultRedisClientOpts()
	if opts == nil {
		return nil
	}
	rdb, err := redisutil.NewClientWithOpts(opts, env.GetHealthChecker(), "default_redis")
	if err != nil {
		return status.InvalidArgumentErrorf("Invalid redis config: %s", err)
	}
	env.SetDefaultRedisClient(rdb)

	rbuf := redisutil.NewCommandBuffer(rdb)
	rbuf.StartPeriodicFlush(context.Background())
	env.GetHealthChecker().RegisterShutdownFunction(rbuf.StopPeriodicFlush)
	return nil
}
