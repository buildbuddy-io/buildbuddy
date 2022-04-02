package rbeutil

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_client"
	remote_execution_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/config"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// Certain RBE features rely on predictable names for resources so that different apps can co-operate
	// without needing any direct communication or shared state. For example, a client may be waiting for execution
	// progress updates on one app and updates may be published by other apps. This co-operation is possible because
	// all the apps generate the same PubSub channel name based on the execution ID.
	// Occasionally we need to introduce changes that affect these shared resources. To be able to roll out these
	// changes safely, we encode the scheme being used as part of the execution ID. This allows apps to determine which
	// naming/protocol they should for a given execution without knowing anything about other apps.

	// V2ExecutionIDPrefix is used for rolling out changes to the Redis PubSub stream keys used for remote execution
	// status updates and for task key updates. Notably under the new scheme both the PubSub stream keys and the task
	// keys use the task ID as the hash for the ring client to ensure that PubSub channel and task metadata for a single
	// task are placed on the same Redis shard. The other change is that we publish a dummy marker message to the PubSub
	// stream when the PubSub channel is created so that we can properly detect whether the PubSub stream has
	// disappeared from redis.
	V2ExecutionIDPrefix = "v2/"
)

var (
	defaultPoolName                   = flag.String("remote_execution.default_pool_name", "", "The default executor pool to use if one is not specified.")
	enableWorkflows                   = flag.Bool("remote_execution.enable_workflows", false, "Whether to enable BuildBuddy workflows.")
	workflowsPoolName                 = flag.String("remote_execution.workflows_pool_name", "", "The executor pool to use for workflow actions. Defaults to the default executor pool if not specified.")
	workflowsDefaultImage             = flag.String("remote_execution.workflows_default_image", "", "The default docker image to use for running workflows.")
	workflowsCIRunnerDebug            = flag.Bool("remote_execution.workflows_ci_runner_debug", false, "Whether to run the CI runner in debug mode.")
	workflowsCIRunnerBazelCommand     = flag.String("remote_execution.workflows_ci_runner_bazel_command", "", "Bazel command to be used by the CI runner.")
	sharedExecutorPoolGroupID         = flag.String("remote_execution.shared_executor_pool_group_id", "", "Group ID that owns the shared executor pool.")
	redisPubSubPoolSize               = flag.Int("remote_execution.redis_pubsub_pool_size", 0, "Maximum number of connections used for waiting for execution updates.")
	requireExecutorAuthorization      = flag.Bool("remote_execution.require_executor_authorization", false, "If true, executors connecting to this server must provide a valid executor API key.")
	enableUserOwnedExecutors          = flag.Bool("remote_execution.enable_user_owned_executors", false, "If enabled, users can register their own executors with the scheduler.")
	forceUserOwnedDarwinExecutors     = flag.Bool("remote_execution.force_user_owned_darwin_executors", false, "If enabled, darwin actions will always run on user-owned executors.")
	enableExecutorKeyCreation         = flag.Bool("remote_execution.enable_executor_key_creation", false, "If enabled, UI will allow executor keys to be created.")
	enableRedisAvailabilityMonitoring = flag.Bool("remote_execution.enable_redis_availability_monitoring", false, "If enabled, the execution server will detect if Redis has lost state and will ask Bazel to retry executions.")
)

func IsV2ExecutionID(executionID string) bool {
	return strings.HasPrefix(executionID, V2ExecutionIDPrefix)
}

func RegisterRemoteExecutionClient(env *real_environment.RealEnv) error {
	if !remote_execution_config.RemoteExecutionEnabled() {
		return nil
	}

	grpcPort := flag.Lookup("grpc_port")
	if grpcPort == nil {
		return status.FailedPreconditionError("Failed registering remote execution client, 'grpc_port' flag does not exist.")
	}
	// Fulfill internal remote execution requests locally.
	conn, err := grpc_client.DialTarget(fmt.Sprintf("grpc://localhost:%s", grpcPort.Value.String()))
	if err != nil {
		return status.InternalErrorf("Error initializing remote execution client: %s", err)
	}
	env.SetRemoteExecutionClient(repb.NewExecutionClient(conn))
	return nil
}

func RegisterRemoteExecutionRedisPubSubClient(env *real_environment.RealEnv) error {
	redisConfig := redis_client.RemoteExecutionRedisClientConfig()
	if redisConfig == nil || !remote_execution_config.RemoteExecutionEnabled() {
		return nil
	}
	opts, err := redisutil.ConfigToOpts(redisConfig)
	if err != nil {
		return status.InternalErrorf("Invalid Remote Execution Redis config: %s", err)
	}
	// This Redis client is used for potentially long running blocking operations.
	// We ideally would not want to  have an upper bound on the # of connections but the redis client library
	// does not  provide such an option so we  set the pool size to a high value to prevent this redis client
	// from being the bottleneck.
	opts.PoolSize = 10_000
	if RedisPubSubPoolSize() != 0 {
		opts.PoolSize = RedisPubSubPoolSize()
	}
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

func DefaultPoolName() string {
	return *defaultPoolName
}

func WorkflowsEnabled() bool {
	return *enableWorkflows
}

func WorkflowsPoolName() string {
	return *workflowsPoolName
}

func WorkflowsDefaultImage() string {
	return *workflowsDefaultImage
}

func WorkflowsCIRunnerDebug() bool {
	return *workflowsCIRunnerDebug
}

func WorkflowsCIRunnerBazelCommand() string {
	return *workflowsCIRunnerBazelCommand
}

func SharedExecutorPoolGroupID() string {
	return *sharedExecutorPoolGroupID
}

func RedisPubSubPoolSize() int {
	return *redisPubSubPoolSize
}

func RequireExecutorAuthorization() bool {
	return *requireExecutorAuthorization
}

func UserOwnedExecutorsEnabled() bool {
	return *enableUserOwnedExecutors
}

func ForceUserOwnedDarwinExecutors() bool {
	return *forceUserOwnedDarwinExecutors
}

func ExecutorKeyCreationEnabled() bool {
	return *enableExecutorKeyCreation
}

func RedisAvailabilityMonitoringEnabled() bool {
	return *enableRedisAvailabilityMonitoring
}
