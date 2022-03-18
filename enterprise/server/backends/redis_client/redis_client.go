package redis_client

import (
	"context"
	"flag"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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

func DefaultRedisClientConfig(env environment.Env) *config.RedisClientConfig {
	if cfg := defaultRedisClientConfigNoFallback(); cfg != nil {
		return cfg
	}

	if cfg := cacheRedisClientConfigNoFallback(); cfg != nil {
		// Fall back to the cache redis client config if default redis target is not specified.
		return cfg
	}

	// Otherwise, fall back to the remote exec redis target.
	return env.GetConfigurator().GetRemoteExecutionRedisClientConfig()
}

func CacheRedisClientConfig() *config.RedisClientConfig {
	return cacheRedisClientConfigNoFallback()
}

func RegisterDefault(env environment.Env) error {
	redisConfig := DefaultRedisClientConfig(env)
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
