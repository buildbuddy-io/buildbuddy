package redis_client

import (
	"flag"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	remote_execution_config "github.com/buildbuddy-io/buildbuddy/server/remote_execution/config"
)

var redisPubSubPoolSize = flag.Int("remote_execution.redis_pubsub_pool_size", 10_000, "Maximum number of connections used for waiting for execution updates.")

func RegisterRemoteExecutionClient(env *real_environment.RealEnv) error {
	if !remote_execution_config.RemoteExecutionEnabled() {
		return nil
	}

	// Fulfill internal remote execution requests locally.
	grpc_port, err := flagutil.GetDereferencedValue[int]("grpc_port")
	if err != nil {
		return status.InternalErrorf("Error initializing remote execution client: %s", err)
	}
	conn, err := grpc_client.DialInternal(env, fmt.Sprintf("grpc://localhost:%d", grpc_port))
	if err != nil {
		return status.InternalErrorf("Error initializing remote execution client: %s", err)
	}
	env.SetRemoteExecutionClient(repb.NewExecutionClient(conn))
	return nil
}

func RegisterRemoteExecutionRedisPubSubClient(env *real_environment.RealEnv) error {
	opts := redis_client.RemoteExecutionRedisClientOpts()
	if opts == nil {
		if !remote_execution_config.RemoteExecutionEnabled() {
			return nil
		}
		return status.InternalErrorf("Invalid Remote Execution Redis config.")
	}
	// This Redis client is used for potentially long running blocking operations.
	// We ideally would not want to  have an upper bound on the # of connections but the redis client library
	// does not  provide such an option so we  set the pool size to a high value to prevent this redis client
	// from being the bottleneck.
	opts.PoolSize = *redisPubSubPoolSize
	opts.IdleTimeout = 1 * time.Minute
	opts.IdleCheckFrequency = 1 * time.Minute
	opts.PoolTimeout = 5 * time.Second

	// The retry settings are tuned to play along with the Bazel execution retry settings. If there's an issue
	// with a Redis shard, we want to at least have a chance to mark it down internally and remove it from the
	// ring before we ask Bazel to retry to avoid the situation with Bazel retrying very quickly and exhausting
	// its attempts placing work on a Redis shard that's down.
	opts.MinRetryBackoff = 128 * time.Millisecond
	opts.MaxRetryBackoff = 1 * time.Second
	opts.MaxRetries = 5

	redisClient, err := redisutil.NewClientWithOpts(opts, env.GetHealthChecker(), "remote_execution_redis_pubsub")
	if err != nil {
		return status.InternalErrorf("Failed to create Remote Execution PubSub redis client: %s", err)
	}
	env.SetRemoteExecutionRedisPubSubClient(redisClient)
	return nil
}
