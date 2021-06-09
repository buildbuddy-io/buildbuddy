package enterprise_testenv

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

type Options struct {
	RedisTarget string
}

func GetCustomTestEnv(t *testing.T, opts *Options) *testenv.TestEnv {
	env := testenv.GetTestEnv(t)
	if opts.RedisTarget != "" {
		healthChecker := healthcheck.NewTestingHealthChecker()
		redisClient := redisutil.NewClient(opts.RedisTarget, healthChecker, "cache_redis")
		env.SetCacheRedisClient(redisClient)
		env.SetRemoteExecutionRedisClient(redisClient)
		env.SetRemoteExecutionRedisPubSubClient(redisClient)
		log.Info("Using redis cache")
		cache := redis_cache.NewCache(redisClient, 500000000 /*=maxValueSizeBytes*/)
		env.SetCache(cache)
	}
	return env
}
