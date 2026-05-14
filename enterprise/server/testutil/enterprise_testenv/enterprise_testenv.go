package enterprise_testenv

import (
	"flag"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/authdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/userdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_client"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Options struct {
	RedisTarget string
	RedisClient redis.UniversalClient
}

func New(t *testing.T) *testenv.TestEnv {
	return GetCustomTestEnv(t, &Options{})
}

func GetCustomTestEnv(t *testing.T, opts *Options) *testenv.TestEnv {
	env := testenv.GetTestEnv(t)

	redisClient := opts.RedisClient
	if redisClient == nil && opts.RedisTarget != "" {
		redisClient = redisutil.NewSimpleClient(opts.RedisTarget, env.GetHealthChecker(), "cache_redis")
		if flag.Lookup("cache.distributed_cache.redis_target") != nil {
			flags.Set(t, "cache.distributed_cache.redis_target", opts.RedisTarget)
		}
	} else if opts.RedisTarget != "" {
		require.FailNow(t, "cannot specify both RedisTarget and RedisClient")
	}
	if redisClient != nil {
		env.SetRemoteExecutionRedisClient(redisClient)
		env.SetRemoteExecutionRedisPubSubClient(redisClient)
		env.SetDefaultRedisClient(redisClient)
		log.Info("Using redis cache")
		if flag.Lookup("cache.redis.max_value_size_bytes") != nil {
			flags.Set(t, "cache.redis.max_value_size_bytes", 500_000_000)
		}
		cache := redis_cache.NewCache(redisClient)
		env.SetCache(cache)
		byte_stream_client.RegisterPooledBytestreamClient(env)
	}

	// Use Cloud userdb settings by default.
	flags.Set(t, "app.add_user_to_domain_group", true)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)

	db, err := authdb.NewAuthDB(env, env.GetDBHandle())
	require.NoError(t, err)
	env.SetAuthDB(db)
	userDB, err := userdb.NewUserDB(env, env.GetDBHandle())
	if err != nil {
		assert.FailNow(t, "could not create user DB", err.Error())
	}
	env.SetUserDB(userDB)
	return env
}

func AddClientIdentity(t *testing.T, te *testenv.TestEnv, client string) {
	flags.Set(t, "app.client_identity.client", client)
	key, err := random.RandomString(16)
	require.NoError(t, err)
	flags.Set(t, "app.client_identity.key", string(key))
	require.NoError(t, err)
	err = clientidentity.Register(te)
	require.NoError(t, err)
	require.NotNil(t, te.GetClientIdentityService())
}
