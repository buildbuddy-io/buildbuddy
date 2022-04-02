package redis_client

import (
	"context"
	"flag"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	remote_execution_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/config"
)

var (
	defaultRedisTarget          = flag.String("app.default_redis_target", "", "A Redis target for storing remote shared state. To ease migration, the redis target from the remote execution config will be used if this value is not specified.")
	defaultRedisShards          = flagutil.StringSlice("app.default_sharded_redis.shards", []string{}, "Ordered list of Redis shard addresses.")
	defaultShardedRedisUsername = flag.String("app.default_sharded_redis.username", "", "Redis username")
	defaultShardedRedisPassword = flag.String("app.default_sharded_redis.password", "", "Redis password")

	// Cache Redis
	// TODO: We need to deprecate one of the redis targets here or distinguish them
	cacheRedisTargetFallback  = flag.String("cache.redis_target", "", "A redis target for improved Caching/RBE performance. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **")
	cacheRedisTarget          = flag.String("cache.redis.redis_target", "", "A redis target for improved Caching/RBE performance. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **")
	cacheRedisShards          = flagutil.StringSlice("cache.redis.sharded.shards", []string{}, "Ordered list of Redis shard addresses.")
	cacheShardedRedisUsername = flag.String("cache.redis.sharded.username", "", "Redis username")
	cacheShardedRedisPassword = flag.String("cache.redis.sharded.password", "", "Redis password")

	// Remote Execution Redis
	remoteExecRedisTarget          = flag.String("remote_execution.redis_target", "", "A Redis target for storing remote execution state. Falls back to app.default_redis_target if unspecified. Required for remote execution. To ease migration, the redis target from the cache config will be used if neither this value nor app.default_redis_target are specified.")
	remoteExecRedisShards          = flagutil.StringSlice("remote_execution.sharded_redis.shards", []string{}, "Ordered list of Redis shard addresses.")
	remoteExecShardedRedisUsername = flag.String("remote_execution.sharded_redis.username", "", "Redis username")
	remoteExecShardedRedisPassword = flag.String("remote_execution.sharded_redis.password", "", "Redis password")
)

func defaultRedisClientConfigNoFallback() *config.RedisClientConfig {
	if len(*defaultRedisShards) > 0 {
		return &config.RedisClientConfig{
			ShardedConfig: &config.ShardedRedisConfig{
				Shards:   *defaultRedisShards,
				Username: *defaultShardedRedisUsername,
				Password: *defaultShardedRedisPassword,
			},
		}
	}

	if *defaultRedisTarget != "" {
		return &config.RedisClientConfig{SimpleTarget: *defaultRedisTarget}
	}
	return nil
}

func cacheRedisClientConfigNoFallback() *config.RedisClientConfig {
	// Prefer the client configs from Redis sub-config, is present.
	if len(*cacheRedisShards) > 0 {
		return &config.RedisClientConfig{
			ShardedConfig: &config.ShardedRedisConfig{
				Shards:   *cacheRedisShards,
				Username: *cacheShardedRedisUsername,
				Password: *cacheShardedRedisPassword,
			},
		}
	}
	if *cacheRedisTarget != "" {
		return &config.RedisClientConfig{SimpleTarget: *cacheRedisTarget}
	}

	if *cacheRedisTargetFallback != "" {
		return &config.RedisClientConfig{SimpleTarget: *cacheRedisTargetFallback}
	}
	return nil
}

func remoteExecutionRedisClientConfigNoFallback() *config.RedisClientConfig {
	if !remote_execution_config.RemoteExecutionEnabled() {
		return nil
	}
	if len(*remoteExecRedisShards) > 0 {
		return &config.RedisClientConfig{
			ShardedConfig: &config.ShardedRedisConfig{
				Shards:   *remoteExecRedisShards,
				Username: *remoteExecShardedRedisUsername,
				Password: *remoteExecShardedRedisPassword,
			},
		}
	}
	if *remoteExecRedisTarget != "" {
		return &config.RedisClientConfig{SimpleTarget: *remoteExecRedisTarget}
	}
	return nil
}

func DefaultRedisClientConfig() *config.RedisClientConfig {
	if cfg := defaultRedisClientConfigNoFallback(); cfg != nil {
		return cfg
	}

	if cfg := cacheRedisClientConfigNoFallback(); cfg != nil {
		// Fall back to the cache redis client config if default redis target is not specified.
		return cfg
	}

	// Otherwise, fall back to the remote exec redis target.
	return remoteExecutionRedisClientConfigNoFallback()
}

func CacheRedisClientConfig() *config.RedisClientConfig {
	return cacheRedisClientConfigNoFallback()
}

func RemoteExecutionRedisClientConfig() *config.RedisClientConfig {
	if cfg := remoteExecutionRedisClientConfigNoFallback(); cfg != nil {
		return cfg
	}

	if cfg := defaultRedisClientConfigNoFallback(); cfg != nil {
		return cfg
	}

	return CacheRedisClientConfig()
}

func RegisterRemoteExecutionRedisClient(env environment.Env) error {
	redisConfig := RemoteExecutionRedisClientConfig()
	if redisConfig == nil {
		return nil
	}
	redisClient, err := redisutil.NewClientFromConfig(redisConfig, env.GetHealthChecker(), "remote_execution_redis")
	if err != nil {
		return status.InternalErrorf("Failed to create Remote Execution redis client: %s", err)
	}
	env.SetRemoteExecutionRedisClient(redisClient)
	return nil
}

func RegisterDefault(env environment.Env) error {
	redisConfig := DefaultRedisClientConfig()
	if redisConfig == nil {
		return nil
	}
	rdb, err := redisutil.NewClientFromConfig(redisConfig, env.GetHealthChecker(), "default_redis")
	if err != nil {
		return status.InvalidArgumentErrorf("Invalid redis config: %s", err)
	}
	env.SetDefaultRedisClient(rdb)

	rbuf := redisutil.NewCommandBuffer(rdb)
	rbuf.StartPeriodicFlush(context.Background())
	env.GetHealthChecker().RegisterShutdownFunction(rbuf.StopPeriodicFlush)
	return nil
}
