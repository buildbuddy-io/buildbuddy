package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/api"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/authdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/distributed"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/gcs_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/memcache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pubsub"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_kvstore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/s3_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/userdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/composable_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/execution_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/hostedrunner"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_search_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_stat_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/execution_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/saml"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/task_router"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/selfauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/splash"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/bitbucket"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/janitor"
	"github.com/buildbuddy-io/buildbuddy/server/libmain"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/static"
	"github.com/buildbuddy-io/buildbuddy/server/telemetry"
	"github.com/buildbuddy-io/buildbuddy/server/util/fileresolver"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/version"
	"google.golang.org/api/option"

	bundle "github.com/buildbuddy-io/buildbuddy/enterprise"
	raft_cache "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/cache"
	telserver "github.com/buildbuddy-io/buildbuddy/enterprise/server/telemetry"
	workflow "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	httpfilters "github.com/buildbuddy-io/buildbuddy/server/http/filters"
)

var (
	memcacheTargets      = flagutil.StringSlice("cache.memcache_targets", []string{}, "Deprecated. Use Redis Target instead.")
	oauthProviders       = []config.OauthProvider{}
	enableSelfAuth =       flag.Bool("auth.enable_self_auth", false, "If true, enables a single user login via an oauth provider on the buildbuddy server. Recommend use only when server is behind a firewall; this option may allow anyone with access to the webpage admin rights to your buildbuddy installation. ** Enterprise only **")

	// Redis
	// TODO: We need to deprecate one of the redis targets here or distinguish them
	cacheRedisTargetFallback    = flag.String("cache.redis_target", "", "A redis target for improved Caching/RBE performance. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **")
	cacheRedisTarget            = flag.String("cache.redis.redis_target", "", "A redis target for improved Caching/RBE performance. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **")
	cacheRedisShards            = flagutil.StringSlice("cache.redis.sharded.shards", []string{}, "Ordered list of Redis shard addresses.")
	cacheRedisUsername          = flag.String("cache.redis.sharded.username", "", "Redis username")
	cacheRedisPassword          = flag.String("cache.redis.sharded.password", "", "Redis password")
	cacheRedisMaxValueSizeBytes = flag.Int64("cache.redis.max_value_size_bytes", 0, "The maximum value size to cache in redis (in bytes).")

	// Distributed cache
	cacheDistributedListenAddr        = flag.String("cache.distributed_cache.listen_addr", "", "The address to listen for local BuildBuddy distributed cache traffic on.")
	cacheDistributedRedisTarget       = flag.String("cache.distributed_cache.redis_target", "", "A redis target for improved Caching/RBE performance. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **")
	cacheDistributedGroupName         = flag.String("cache.distributed_cache.group_name", "", "A unique name for this distributed cache group. ** Enterprise only **")
	cacheDistributedNodes             = flagutil.StringSlice("cache.distributed_cache.nodes", []string{}, "The hardcoded list of peer distributed cache nodes. If this is set, redis_target will be ignored. ** Enterprise only **")
	cacheDistributedReplicationFactor = flag.Int("cache.distributed_cache.replication_factor", 0, "How many total servers the data should be replicated to. Must be >= 1. ** Enterprise only **")
	cacheDistributedClusterSize       = flag.Int("cache.distributed_cache.cluster_size", 0, "The total number of nodes in this cluster. Required for health checking. ** Enterprise only **")

	// Raft cache config
	cacheRaftRootDirectory = flag.String("cache.raft.root_directory", "", "The root directory to use for storing cached data.")
	cacheRaftListenAddr    = flag.String("cache.raft.listen_addr", "", "The address to listen for local gossip traffic on. Ex. 'localhost:1991")
	cacheRaftJoin          = flagutil.StringSlice("cache.raft.join", []string{}, "The list of nodes to use when joining clusters Ex. '1.2.3.4:1991,2.3.4.5:1991...'")
	cacheRaftHTTPPort      = flag.Int("cache.raft.http_port", 0, "The address to listen for HTTP raft traffic. Ex. '1992'")
	cacheRaftGRPCPort      = flag.Int("cache.raft.grpc_port", 0, "The address to listen for internal API traffic on. Ex. '1993'")

	// GCS flags
	gcsBucket          = flag.String("cache.gcs.bucket", "", "The name of the GCS bucket to store cache files in.")
	gcsCredentialsFile = flag.String("cache.gcs.credentials_file", "", "A path to a JSON credentials file that will be used to authenticate to GCS.")
	gcsProjectID       = flag.String("cache.gcs.project_id", "", "The Google Cloud project ID of the project owning the above credentials and GCS bucket.")

	// AWS S3 flags
	s3Region                   = flag.String("cache.s3.region", "", "The AWS region.")
	s3Bucket                   = flag.String("cache.s3.bucket", "", "The AWS S3 bucket to store files in.")
	s3CredentialsProfile       = flag.String("cache.s3.credentials_profile", "", "A custom credentials profile to use.")
	s3WebIdentityTokenFilePath = flag.String("cache.s3.web_identity_token_file", "", "The file path to the web identity token file.")
	s3RoleARN                  = flag.String("cache.s3.role_arn", "", "The role ARN to use for web identity auth.")
	s3RoleSessionName          = flag.String("cache.s3.role_session_name", "", "The role session name to use for web identity auth.")
	s3Endpoint                 = flag.String("cache.s3.endpoint", "", "The AWS endpoint to use, useful for configuring the use of MinIO.")
	s3StaticCredentialsID      = flag.String("cache.s3.static_credentials_id", "", "Static credentials ID to use, useful for configuring the use of MinIO.")
	s3StaticCredentialsSecret  = flag.String("cache.s3.static_credentials_secret", "", "Static credentials secret to use, useful for configuring the use of MinIO.")
	s3StaticCredentialsToken   = flag.String("cache.s3.static_credentials_token", "", "Static credentials token to use, useful for configuring the use of MinIO.")
	s3DisableSSL               = flag.Bool("cache.s3.disable_ssl", false, "Disables the use of SSL, useful for configuring the use of MinIO.")
	s3ForcePathStyle           = flag.Bool("cache.s3.s3_force_path_style", false, "Force path style urls for objects, useful for configuring the use of MinIO.")

	// Remote Execution Config
	remoteExecDefaultPoolName                   = flag.String("remote_execution.default_pool_name", "", "The default executor pool to use if one is not specified.")
	remoteExecEnableWorkflows                   = flag.Bool("remote_execution.enable_workflows", false, "Whether to enable BuildBuddy workflows.")
	remoteExecWorkflowsPoolName                 = flag.String("remote_execution.workflows_pool_name", "", "The executor pool to use for workflow actions. Defaults to the default executor pool if not specified.")
	remoteExecWorkflowsDefaultImage             = flag.String("remote_execution.workflows_default_image", "", "The default docker image to use for running workflows.")
	remoteExecWorkflowsCIRunnerDebug            = flag.Bool("remote_execution.workflows_ci_runner_debug", false, "Whether to run the CI runner in debug mode.")
	remoteExecWorkflowsCIRunnerBazelCommand     = flag.String("remote_execution.workflows_ci_runner_bazel_command", "", "Bazel command to be used by the CI runner.")
	remoteExecRedisTarget                       = flag.String("remote_execution.redis_target", "", "A Redis target for storing remote execution state. Falls back to app.default_redis_target if unspecified. Required for remote execution. To ease migration, the redis target from the cache config will be used if neither this value nor app.default_redis_target are specified.")
	remoteExecRedisShards                       = flagutil.StringSlice("remote_execution.sharded_redis.shards", []string{}, "Ordered list of Redis shard addresses.")
	remoteExecRedisUsername                     = flag.String("remote_execution.sharded_redis.username", "", "Redis username")
	remoteExecRedisPassword                     = flag.String("remote_execution.sharded_redis.password", "", "Redis password")
	remoteExecSharedExecutorPoolGroupID         = flag.String("remote_execution.shared_executor_pool_group_id", "", "Group ID that owns the shared executor pool.")
	remoteExecRedisPubSubPoolSize               = flag.Int("remote_execution.redis_pubsub_pool_size", 0, "Maximum number of connections used for waiting for execution updates.")
	remoteExecEnableRemoteExec                  = flag.Bool("remote_execution.enable_remote_exec", false, "If true, enable remote-exec. ** Enterprise only **")
	remoteExecRequireExecutorAuthorization      = flag.Bool("remote_execution.require_executor_authorization", false, "If true, executors connecting to this server must provide a valid executor API key.")
	remoteExecEnableUserOwnedExecutors          = flag.Bool("remote_execution.enable_user_owned_executors", false, "If enabled, users can register their own executors with the scheduler.")
	remoteExecForceUserOwnedDarwinExecutors     = flag.Bool("remote_execution.force_user_owned_darwin_executors", false, "If enabled, darwin actions will always run on user-owned executors.")
	remoteExecEnableExecutorKeyCreation         = flag.Bool("remote_execution.enable_executor_key_creation", false, "If enabled, UI will allow executor keys to be created.")
	remoteExecEnableRedisAvailabilityMonitoring = flag.Bool("remote_execution.enable_redis_availability_monitoring", false, "If enabled, the execution server will detect if Redis has lost state and will ask Bazel to retry executions.")

	serverType = flag.String("server_type", "buildbuddy-server", "The server type to match on health checks")
)

func init() {
	flagutil.StructSliceVar(&oauthProviders, "auth.oauth_providers", "")
}

func configureFilesystemsOrDie(realEnv *real_environment.RealEnv) {
	// Ensure we always override the app filesystem because the enterprise
	// binary bundles a different app than the OS one does.
	// The static filesystem is the same, however, so we set it if the flag
	// is set, but we do not fall back to an embedded staticFS.
	realEnv.SetAppFilesystem(nil)
	if staticDirectoryFlag := flag.Lookup("static_directory"); staticDirectoryFlag != nil {
		if staticDirectory := staticDirectoryFlag.Value.String(); staticDirectory != "" {
			staticFS, err := static.FSFromRelPath(staticDirectory)
			if err != nil {
				log.Fatalf("Error getting static FS from relPath: %q: %s", staticDirectory, err)
			}
			realEnv.SetStaticFilesystem(staticFS)
		}
	}
	if appDirectoryFlag := flag.Lookup("app_directory"); appDirectoryFlag != nil {
		if appDirectory := appDirectoryFlag.Value.String(); appDirectory != "" {
			appFS, err := static.FSFromRelPath(appDirectory)
			if err != nil {
				log.Fatalf("Error getting app FS from relPath: %q: %s", appDirectory, err)
			}
			realEnv.SetAppFilesystem(appFS)
		}
	}
	bundleFS, err := bundle.Get()
	if err != nil {
		log.Fatalf("Error getting bundle FS: %s", err)
	}
	realEnv.SetFileResolver(fileresolver.New(bundleFS, "enterprise"))
	if realEnv.GetAppFilesystem() == nil {
		if realEnv.GetAppFilesystem() == nil {
			appFS, err := fs.Sub(bundleFS, "app")
			if err != nil {
				log.Fatalf("Error getting app FS from bundle: %s", err)
			}
			log.Debug("Using bundled enterprise app filesystem.")
			realEnv.SetAppFilesystem(appFS)
		}
	}
}

// NB: Most of the logic you'd typically find in a main.go file is in
// libmain.go. We put it there to reduce the duplicated code between the open
// source main() entry point and the enterprise main() entry point, both of
// which import from libmain.go.
func convertToProdOrDie(ctx context.Context, env *real_environment.RealEnv) {
	env.SetAuthDB(authdb.NewAuthDB(env.GetDBHandle()))
	configureFilesystemsOrDie(env)

	authConfigs := env.GetConfigurator().GetAuthOauthProviders()
	if env.GetConfigurator().GetSelfAuthEnabled() {
		authConfigs = append(
			authConfigs,
			selfauth.Provider(env),
		)
	}
	var authenticator interfaces.Authenticator
	authenticator, err := auth.NewOpenIDAuthenticator(ctx, env, authConfigs)
	if err == nil {
		if env.GetConfigurator().GetSAMLConfig().CertFile != "" {
			log.Info("SAML auth configured.")
			authenticator = saml.NewSAMLAuthenticator(env, authenticator)
		}
		env.SetAuthenticator(authenticator)
	} else {
		log.Warningf("No authentication will be configured: %s", err)
	}

	userDB, err := userdb.NewUserDB(env, env.GetDBHandle())
	if err != nil {
		log.Fatalf("Error setting up prod user DB: %s", err)
	}
	env.SetUserDB(userDB)

	stat := invocation_stat_service.NewInvocationStatService(env, env.GetDBHandle())
	env.SetInvocationStatService(stat)

	search := invocation_search_service.NewInvocationSearchService(env, env.GetDBHandle())
	env.SetInvocationSearchService(search)

	if err := usage_service.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

	apiServer := api.NewAPIServer(env)
	env.SetAPIService(apiServer)

	workflowService := workflow.NewWorkflowService(env)
	env.SetWorkflowService(workflowService)
	env.SetGitProviders([]interfaces.GitProvider{
		github.NewProvider(),
		bitbucket.NewProvider(),
	})

	runnerService, err := hostedrunner.New(env)
	if err != nil {
		log.Fatalf("Error setting up runner: %s", err)
	}
	env.SetRunnerService(runnerService)

	env.SetSplashPrinter(&splash.Printer{})
}

func main() {
	rootContext := context.Background()
	version.Print()

	configurator, err := config.ParseAndReconcileFlagsAndConfig("")
	if err != nil {
		log.Fatalf("Error loading config from file: %s", err)
	}
	healthChecker := healthcheck.NewHealthChecker(*serverType)
	realEnv := libmain.GetConfiguredEnvironmentOrDie(configurator, healthChecker)
	if err := tracing.Configure(healthChecker); err != nil {
		log.Fatalf("Could not configure tracing: %s", err)
	}

	// Setup the prod fanciness in our environment
	convertToProdOrDie(rootContext, realEnv)

	// Install any prod-specific backends here.
	if gcsCacheConfig := configurator.GetCacheGCSConfig(); gcsCacheConfig != nil {
		opts := make([]option.ClientOption, 0)
		if gcsCacheConfig.CredentialsFile != "" {
			opts = append(opts, option.WithCredentialsFile(gcsCacheConfig.CredentialsFile))
		}
		gcsCache, err := gcs_cache.NewGCSCache(gcsCacheConfig.Bucket, gcsCacheConfig.ProjectID, gcsCacheConfig.TTLDays, opts...)
		if err != nil {
			log.Fatalf("Error configuring GCS cache: %s", err)
		}
		realEnv.SetCache(gcsCache)
	}

	if s3CacheConfig := configurator.GetCacheS3Config(); s3CacheConfig != nil {
		s3Cache, err := s3_cache.NewS3Cache(s3CacheConfig)
		if err != nil {
			log.Fatalf("Error configuring S3 cache: %s", err)
		}
		realEnv.SetCache(s3Cache)
	}

	if redisConfig := configurator.GetCacheRedisClientConfig(); redisConfig != nil {
		redisClient, err := redisutil.NewClientFromConfig(redisConfig, healthChecker, "cache_redis")
		if err != nil {
			log.Fatalf("Error configuring cache Redis client: %s", err)
		}
		realEnv.SetCacheRedisClient(redisClient)
	}

	if err := redisutil.RegisterDefaultRedisClient(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := redis_kvstore.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := redis_metrics_collector.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err = usage.RegisterTracker(realEnv); err != nil {
		log.Fatalf("%v", err)
	}

	if redisConfig := configurator.GetRemoteExecutionRedisClientConfig(); redisConfig != nil {
		redisClient, err := redisutil.NewClientFromConfig(redisConfig, healthChecker, "remote_execution_redis")
		if err != nil {
			log.Fatalf("Failed to create Remote Execution redis client: %s", err)
		}
		realEnv.SetRemoteExecutionRedisClient(redisClient)

		// Task router uses the remote execution redis client.
		taskRouter, err := task_router.New(realEnv)
		if err != nil {
			log.Fatalf("Failed to create server: %s", err)
		}
		realEnv.SetTaskRouter(taskRouter)
	}

	if rbeConfig := configurator.GetRemoteExecutionConfig(); rbeConfig != nil {
		if redisConfig := configurator.GetRemoteExecutionRedisClientConfig(); redisConfig != nil {
			opts, err := redisutil.ConfigToOpts(redisConfig)
			if err != nil {
				log.Fatalf("Invalid Remote Execution Redis config: %s", err)
			}
			// This Redis client is used for potentially long running blocking operations.
			// We ideally would not want to  have an upper bound on the # of connections but the redis client library
			// does not  provide such an option so we  set the pool size to a high value to prevent this redis client
			// from being the bottleneck.
			opts.PoolSize = 10_000
			if rbeConfig.RedisPubSubPoolSize != 0 {
				opts.PoolSize = rbeConfig.RedisPubSubPoolSize
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

			redisClient, err := redisutil.NewClientWithOpts(opts, healthChecker, "remote_execution_redis_pubsub")
			if err != nil {
				log.Fatalf("Failed to create Remote Execution PubSub redis client: %s", err)
			}
			realEnv.SetRemoteExecutionRedisPubSubClient(redisClient)
		}
	}

	if dcc := configurator.GetDistributedCacheConfig(); dcc != nil {
		dcConfig := distributed.CacheConfig{
			ListenAddr:        dcc.ListenAddr,
			GroupName:         dcc.GroupName,
			ReplicationFactor: dcc.ReplicationFactor,
			Nodes:             dcc.Nodes,
			ClusterSize:       dcc.ClusterSize,
		}
		log.Infof("Enabling distributed cache with config: %+v", dcConfig)
		if len(dcConfig.Nodes) == 0 {
			dcConfig.PubSub = pubsub.NewPubSub(redisutil.NewSimpleClient(dcc.RedisTarget, healthChecker, "distributed_cache_redis"))
		}
		dc, err := distributed.NewDistributedCache(realEnv, realEnv.GetCache(), dcConfig, healthChecker)
		if err != nil {
			log.Fatalf("Error enabling distributed cache: %s", err.Error())
		}
		dc.StartListening()
		realEnv.SetCache(dc)
	}

	if rcc := configurator.GetRaftCacheConfig(); rcc != nil {
		rcConfig := &raft_cache.Config{
			RootDir:       rcc.RootDirectory,
			ListenAddress: rcc.ListenAddr,
			Join:          rcc.Join,
			HTTPPort:      rcc.HTTPPort,
			GRPCPort:      rcc.GRPCPort,
		}
		rc, err := raft_cache.NewRaftCache(realEnv, rcConfig)
		if err != nil {
			log.Fatalf("Error enabling raft cache: %s", err.Error())
		}
		defer rc.Stop()
		realEnv.SetCache(rc)
	}

	if mcTargets := configurator.GetCacheMemcacheTargets(); len(mcTargets) > 0 {
		if realEnv.GetCache() == nil {
			log.Fatalf("Memcache layer requires a base cache; but one was not configured; please also enable a gcs/s3/disk cache")
		}
		log.Infof("Enabling memcache layer with targets: %s", mcTargets)
		mc := memcache.NewCache(mcTargets...)
		realEnv.SetCache(composable_cache.NewComposableCache(mc, realEnv.GetCache(), composable_cache.ModeReadThrough|composable_cache.ModeWriteThrough))
	} else if redisClientConfig := configurator.GetCacheRedisClientConfig(); redisClientConfig != nil {
		if realEnv.GetCache() == nil {
			log.Fatalf("Redis layer requires a base cache; but one was not configured; please also enable a gcs/s3/disk cache")
		}
		log.Infof("Enabling redis layer with targets: %s", redisClientConfig)
		maxValueSizeBytes := int64(0)
		if redisConfig := configurator.GetCacheRedisConfig(); redisConfig != nil {
			maxValueSizeBytes = redisConfig.MaxValueSizeBytes
		}
		r := redis_cache.NewCache(realEnv.GetCacheRedisClient(), maxValueSizeBytes)
		realEnv.SetCache(composable_cache.NewComposableCache(r, realEnv.GetCache(), composable_cache.ModeReadThrough|composable_cache.ModeWriteThrough))
	}

	if remoteExecConfig := configurator.GetRemoteExecutionConfig(); remoteExecConfig != nil {
		// Make sure capabilities server reflect that we're running
		// remote execution.
		executionServer, err := execution_server.NewExecutionServer(realEnv)
		if err != nil {
			log.Fatalf("Error initializing ExecutionServer: %s", err)
		}
		realEnv.SetRemoteExecutionService(executionServer)

		schedulerServer, err := scheduler_server.NewSchedulerServer(realEnv)
		if err != nil {
			log.Fatalf("Error configuring scheduler server: %v", err)
		}
		realEnv.SetSchedulerService(schedulerServer)

		// Fulfill internal remote execution requests locally.
		conn, err := grpc_client.DialTarget(fmt.Sprintf("grpc://localhost:%d", *libmain.GRPCPort))
		if err != nil {
			log.Fatalf("Error initializing remote execution client: %s", err)
		}
		realEnv.SetRemoteExecutionClient(repb.NewExecutionClient(conn))
	}

	executionService := execution_service.NewExecutionService(realEnv)
	realEnv.SetExecutionService(executionService)

	telemetryServer := telserver.NewTelemetryServer(realEnv, realEnv.GetDBHandle())
	telemetryServer.StartOrDieIfEnabled()

	telemetryClient := telemetry.NewTelemetryClient(realEnv)
	telemetryClient.Start()
	defer telemetryClient.Stop()

	cleanupService := janitor.NewJanitor(realEnv)
	cleanupService.Start()
	defer cleanupService.Stop()

	if realEnv.GetConfigurator().GetSelfAuthEnabled() {
		oauth, err := selfauth.NewSelfAuth(realEnv)
		if err != nil {
			log.Fatalf("Error initializing self auth: %s", err)
		}
		mux := realEnv.GetMux()
		mux.Handle(oauth.AuthorizationEndpoint().Path, httpfilters.SetSecurityHeaders(http.HandlerFunc(oauth.Authorize)))
		mux.Handle(oauth.TokenEndpoint().Path, httpfilters.SetSecurityHeaders(http.HandlerFunc(oauth.AccessToken)))
		mux.Handle(oauth.JwksEndpoint().Path, httpfilters.SetSecurityHeaders(http.HandlerFunc(oauth.Jwks)))
		mux.Handle("/.well-known/openid-configuration", httpfilters.SetSecurityHeaders(http.HandlerFunc(oauth.WellKnownOpenIDConfiguration)))
	}
	libmain.StartAndRunServices(realEnv) // Returns after graceful shutdown
}
