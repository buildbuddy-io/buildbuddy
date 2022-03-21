package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/gcs_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/memcache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/s3_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/composable_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_task_scheduler"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/selfauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/fileresolver"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/xcode"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/test/bufconn"

	bundle "github.com/buildbuddy-io/buildbuddy/enterprise"
	remote_executor "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	_ "google.golang.org/grpc/encoding/gzip" // imported for side effects; DO NOT REMOVE.
)

var (
	memcacheTargets        = flagutil.StringSlice("cache.memcache_targets", []string{}, "Deprecated. Use Redis Target instead.")
	zstdTranscodingEnabled = flag.Bool("cache.zstd_transcoding_enabled", false, "Whether to accept requests to read/write zstd-compressed blobs, compressing/decompressing outgoing/incoming blobs on the fly.")
	oauthProviders       = []config.OauthProvider{}
	enableSelfAuth =       flag.Bool("auth.enable_self_auth", false, "If true, enables a single user login via an oauth provider on the buildbuddy server. Recommend use only when server is behind a firewall; this option may allow anyone with access to the webpage admin rights to your buildbuddy installation. ** Enterprise only **")

	//Redis
	redisTarget                 = flag.String("cache.redis_target", "", "A redis target for improved Caching/RBE performance. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **")
	cacheRedisTarget            = flag.String("cache.redis.redis_target", "", "A redis target for improved Caching/RBE performance. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **")
	cacheRedisShards            = flagutil.StringSlice("cache.redis.sharded.shards", []string{}, "Ordered list of Redis shard addresses.")
	cacheRedisUsername          = flag.String("cache.redis.sharded.username", "", "Redis username")
	cacheRedisPassword          = flag.String("cache.redis.sharded.password", "", "Redis password")
	cacheRedisMaxValueSizeBytes = flag.Int64("cache.redis.max_value_size_bytes", 0, "The maximum value size to cache in redis (in bytes).")

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

	// Executor config
	appTarget                     = flag.String("executor.app_target", "", "The GRPC url of a buildbuddy app server.")
	pool                          = flag.String("executor.pool", "", "Executor pool name. Only one of this config option or the MY_POOL environment variable should be specified.")
	rootDirectory                 = flag.String("executor.root_directory", "", "The root directory to use for build files.")
	hostRootDirectory             = flag.String("executor.host_root_directory", "", "Path on the host where the executor container root directory is mounted.")
	localCacheDirectory           = flag.String("executor.local_cache_directory", "", "A local on-disk cache directory. Must be on the same device (disk partition, Docker volume, etc.) as the configured root_directory, since files are hard-linked to this cache for performance reasons. Otherwise, 'Invalid cross-device link' errors may result.")
	localCacheSizeBytes           = flag.Int64("executor.local_cache_size_bytes", 0, "The maximum size, in bytes, to use for the local on-disk cache")
	disableLocalCache             = flag.Bool("executor.disable_local_cache", false, "If true, a local file cache will not be used.")
	dockerSocket                  = flag.String("executor.docker_socket", "", "If set, run execution commands in docker using the provided socket.")
	aPIKey                        = flag.String("executor.api_key", "", "API Key used to authorize the executor with the BuildBuddy app server.")
	dockerMountMode               = flag.String("executor.docker_mount_mode", "", "Sets the mount mode of volumes mounted to docker images. Useful if running on SELinux https://www.projectatomic.io/blog/2015/06/using-volumes-with-docker-can-cause-problems-with-selinux/")
	maxRunnerCount                = flag.Int("executor.runner_pool.max_runner_count", 0, "Maximum number of recycled RBE runners that can be pooled at once. Defaults to a value derived from estimated CPU usage, max RAM, allocated CPU, and allocated memory.")
	maxRunnerDiskSizeBytes        = flag.Int64("executor.runner_pool.max_runner_disk_size_bytes", 0, "Maximum disk size for a recycled runner; runners exceeding this threshold are not recycled. Defaults to 16GB.")
	maxRunnerMemoryUsageBytes     = flag.Int64("executor.runner_pool.max_runner_memory_usage_bytes", 0, "Maximum memory usage for a recycled runner; runners exceeding this threshold are not recycled. Defaults to 1/10 of total RAM allocated to the executor. (Only supported for Docker-based executors).")
	dockerNetHost                 = flag.Bool("executor.docker_net_host", false, "Sets --net=host on the docker command. Intended for local development only.")
	dockerSiblingContainers       = flag.Bool("executor.docker_sibling_containers", false, "If set, mount the configured Docker socket to containers spawned for each action, to enable Docker-out-of-Docker (DooD). Takes effect only if docker_socket is also set. Should not be set by executors that can run untrusted code.")
	dockerInheritUserIDs          = flag.Bool("executor.docker_inherit_user_ids", false, "If set, run docker containers using the same uid and gid as the user running the executor process.")
	defaultXcodeVersion           = flag.String("executor.default_xcode_version", "", "Sets the default Xcode version number to use if an action doesn't specify one. If not set, /Applications/Xcode.app/ is used.")
	defaultIsolationType          = flag.String("executor.default_isolation_type", "", "The default workload isolation type when no type is specified in an action. If not set, we use the first of the following that is set: docker, firecracker, podman, or barerunner")
	enableBareRunner              = flag.Bool("executor.enable_bare_runner", false, "Enables running execution commands directly on the host without isolation.")
	enablePodman                  = flag.Bool("executor.enable_podman", false, "Enables running execution commands inside podman container.")
	enableFirecracker             = flag.Bool("executor.enable_firecracker", false, "Enables running execution commands inside of firecracker VMs")
	firecrackerMountWorkspaceFile = flag.Bool("executor.firecracker_mount_workspace_file", false, "Enables mounting workspace filesystem to improve performance of copying action outputs.")
	containerRegistries           = []config.ContainerRegistryConfig{}
	enableVFS                     = flag.Bool("executor.enable_vfs", false, "Whether FUSE based filesystem is enabled.")
	defaultImage                  = flag.String("executor.default_image", "", "The default docker image to use to warm up executors or if no platform property is set. Ex: gcr.io/flame-public/executor-docker-default:enterprise-v1.5.4")
	warmupTimeoutSecs             = flag.Int64("executor.warmup_timeout_secs", 0, "The default time (in seconds) to wait for an executor to warm up i.e. download the default docker image. Default is 120s")
	startupWarmupMaxWaitSecs      = flag.Int64("executor.startup_warmup_max_wait_secs", 0, "Maximum time to block startup while waiting for default image to be pulled. Default is no wait.")
	exclusiveTaskScheduling       = flag.Bool("executor.exclusive_task_scheduling", false, "If true, only one task will be scheduled at a time. Default is false")
	memoryBytes                   = flag.Int64("executor.memory_bytes", 0, "Optional maximum memory to allocate to execution tasks (approximate). Cannot set both this option and the SYS_MEMORY_BYTES env var.")
	milliCPU                      = flag.Int64("executor.millicpu", 0, "Optional maximum CPU milliseconds to allocate to execution tasks (approximate). Cannot set both this option and the SYS_MILLICPU env var.")

	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port           = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	monitoringPort = flag.Int("monitoring_port", 9090, "The port to listen for monitoring traffic on")
	gRPCPort       = flag.Int("grpc_port", 1987, "The port to listen for gRPC traffic on")
	serverType     = flag.String("server_type", "prod-buildbuddy-executor", "The server type to match on health checks")
)

func init() {
	flagutil.StructSliceVar(&oauthProviders, "auth.oauth_providers", "")
	flagutil.StructSliceVar(&containerRegistries, "executor.container_registries", "")
}

var localListener *bufconn.Listener

func InitializeCacheClientsOrDie(cacheTarget string, realEnv *real_environment.RealEnv, useLocal bool) {
	var conn *grpc.ClientConn
	var err error
	if useLocal {
		log.Infof("Using local cache!")
		dialOptions := grpc_client.CommonGRPCClientOptions()
		dialOptions = append(dialOptions, grpc.WithContextDialer(bufDialer))
		dialOptions = append(dialOptions, grpc.WithInsecure())

		conn, err = grpc.DialContext(context.Background(), "bufnet", dialOptions...)
		if err != nil {
			log.Fatalf("Failed to dial bufnet: %v", err)
		}
		log.Debugf("Connecting to local cache over bufnet")
	} else {
		if cacheTarget == "" {
			log.Fatalf("No cache target was set. Run a local cache or specify one in the config")
		}
		conn, err = grpc_client.DialTarget(cacheTarget)
		if err != nil {
			log.Fatalf("Unable to connect to cache '%s': %s", cacheTarget, err)
		}
		log.Infof("Connecting to cache target: %s", cacheTarget)
	}

	realEnv.GetHealthChecker().AddHealthCheck(
		"grpc_cache_connection", interfaces.CheckerFunc(
			func(ctx context.Context) error {
				connState := conn.GetState()
				if connState == connectivity.Ready {
					return nil
				} else if connState == connectivity.Idle {
					conn.Connect()
				}
				return fmt.Errorf("gRPC connection not yet ready (state: %s)", connState)
			},
		),
	)

	realEnv.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	realEnv.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	realEnv.SetActionCacheClient(repb.NewActionCacheClient(conn))
}

func GetConfiguredEnvironmentOrDie(configurator *config.Configurator, healthChecker *healthcheck.HealthChecker) environment.Env {
	realEnv := real_environment.NewRealEnv(configurator, healthChecker)

	executorConfig := configurator.GetExecutorConfig()
	if executorConfig == nil {
		log.Fatal("Executor config not found")
	}

	if executorConfig.Pool != "" && resources.GetPoolName() != "" {
		log.Fatal("Only one of the `MY_POOL` environment variable and `executor.pool` config option may be set")
	}
	if err := resources.Configure(realEnv); err != nil {
		log.Fatal(status.Message(err))
	}

	bundleFS, err := bundle.Get()
	if err != nil {
		log.Fatalf("Failed to initialize bundle: %s", err)
	}
	realEnv.SetFileResolver(fileresolver.New(bundleFS, "enterprise"))

	authConfigs := realEnv.GetConfigurator().GetAuthOauthProviders()
	if realEnv.GetConfigurator().GetSelfAuthEnabled() {
		authConfigs = append(
			authConfigs,
			selfauth.Provider(realEnv),
		)
	}
	authenticator, err := auth.NewOpenIDAuthenticator(context.Background(), realEnv, authConfigs)
	if err == nil {
		realEnv.SetAuthenticator(authenticator)
	} else {
		log.Infof("No authentication will be configured: %s", err)
	}

	xl := xcode.NewXcodeLocator()
	realEnv.SetXcodeLocator(xl)

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

	// OK, here on below we will layer several caches together with
	// composable cache, and the final result will become our digestCache.
	if mcTargets := configurator.GetCacheMemcacheTargets(); len(mcTargets) > 0 {
		log.Infof("Enabling memcache layer with targets: %s", mcTargets)
		mc := memcache.NewCache(mcTargets...)
		realEnv.SetCache(composable_cache.NewComposableCache(mc, realEnv.GetCache(), composable_cache.ModeReadThrough|composable_cache.ModeWriteThrough))
	} else if redisClientConfig := configurator.GetCacheRedisClientConfig(); redisClientConfig != nil {
		log.Infof("Enabling redis layer with targets: %s", redisClientConfig)

		redisClient, err := redisutil.NewClientFromConfig(redisClientConfig, healthChecker, "cache_redis")
		if err != nil {
			log.Fatalf("Error configuring cache Redis client: %s", err)
		}

		maxValueSizeBytes := int64(0)
		if redisConfig := configurator.GetCacheRedisConfig(); redisConfig != nil {
			maxValueSizeBytes = redisConfig.MaxValueSizeBytes
		}
		r := redis_cache.NewCache(redisClient, maxValueSizeBytes)
		realEnv.SetCache(composable_cache.NewComposableCache(r, realEnv.GetCache(), composable_cache.ModeReadThrough|composable_cache.ModeWriteThrough))
	}

	useLocalCache := realEnv.GetCache() != nil
	InitializeCacheClientsOrDie(executorConfig.GetAppTarget(), realEnv, useLocalCache)

	if executorConfig.GetLocalCacheDirectory() != "" && executorConfig.GetLocalCacheSizeBytes() != 0 {
		log.Infof("Enabling filecache in %q (size %d bytes)", executorConfig.GetLocalCacheDirectory(), executorConfig.GetLocalCacheSizeBytes())
		if fc, err := filecache.NewFileCache(executorConfig.GetLocalCacheDirectory(), executorConfig.GetLocalCacheSizeBytes()); err == nil {
			realEnv.SetFileCache(fc)
		}
	}

	if executorConfig.GetAppTarget() != "" {
		conn, err := grpc_client.DialTarget(executorConfig.GetAppTarget())
		if err != nil {
			log.Fatalf("Unable to connect to app '%s': %s", executorConfig.GetAppTarget(), err)
		}
		log.Infof("Connecting to app target: %s", executorConfig.GetAppTarget())

		realEnv.GetHealthChecker().AddHealthCheck(
			"grpc_app_connection", interfaces.CheckerFunc(
				func(ctx context.Context) error {
					connState := conn.GetState()
					if connState == connectivity.Ready {
						return nil
					} else if connState == connectivity.Idle {
						conn.Connect()
					}
					return fmt.Errorf("gRPC connection not yet ready (state: %s)", connState)
				},
			),
		)
		realEnv.SetSchedulerClient(scpb.NewSchedulerClient(conn))
		realEnv.SetRemoteExecutionClient(repb.NewExecutionClient(conn))
	}

	return realEnv
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return localListener.Dial()
}

func main() {
	// The default umask (0022) has the effect of clearing the group-write and
	// others-write bits when setting up workspace directories, regardless of the
	// permissions bits passed to mkdir. We want to create these directories with
	// 0777 permissions in some cases, because we need those to be writable by the
	// container user, and it is too costly to enable those permissions via
	// explicit chown or chmod calls on those directories. So, we clear the umask
	// here to allow group-write and others-write permissions.
	syscall.Umask(0)

	rootContext := context.Background()

	configurator, err := config.ParseAndReconcileFlagsAndConfig("")
	if err != nil {
		log.Fatalf("Error loading config from file: %s", err)
	}

	if err := log.Configure(); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1)
	}

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	localListener = bufconn.Listen(1024 * 1024 * 10 /* 10MB buffer? Seems ok. */)

	if err := tracing.Configure(healthChecker); err != nil {
		log.Fatalf("Could not configure tracing: %s", err)
	}

	env := GetConfiguredEnvironmentOrDie(configurator, healthChecker)

	grpcOptions := grpc_server.CommonGRPCServerOptions(env)
	localServer := grpc.NewServer(grpcOptions...)

	// Start Build-Event-Protocol and Remote-Cache services.
	executorConfig := configurator.GetExecutorConfig()
	if executorConfig == nil {
		log.Fatal("Executor config not found")
	}
	executorUUID, err := uuid.NewRandom()
	if err != nil {
		log.Fatalf("Failed to generate executor instance ID: %s", err)
	}
	executorID := executorUUID.String()
	executor, err := remote_executor.NewExecutor(env, executorID, &remote_executor.Options{})
	if err != nil {
		log.Fatalf("Error initializing ExecutionServer: %s", err)
	}
	taskScheduler := priority_task_scheduler.NewPriorityTaskScheduler(env, executor, &priority_task_scheduler.Options{})
	if err := taskScheduler.Start(); err != nil {
		log.Fatalf("Error starting task scheduler: %v", err)
	}

	// OPTIONAL CACHE API -- only enable if configured.
	// Install any prod-specific backends here.
	enableCache := env.GetCache() != nil
	if enableCache {
		// Register to handle content addressable storage (CAS) messages.
		casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
		if err != nil {
			log.Fatalf("Error initializing ContentAddressableStorageServer: %s", err)
		}
		repb.RegisterContentAddressableStorageServer(localServer, casServer)

		// Register to handle bytestream (upload and download) messages.
		byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
		if err != nil {
			log.Fatalf("Error initializing ByteStreamServer: %s", err)
		}
		bspb.RegisterByteStreamServer(localServer, byteStreamServer)

		// Register to handle action cache (upload and download) messages.
		actionCacheServer, err := action_cache_server.NewActionCacheServer(env)
		if err != nil {
			log.Fatalf("Error initializing ActionCacheServer: %s", err)
		}
		repb.RegisterActionCacheServer(localServer, actionCacheServer)
	}

	monitoring.StartMonitoringHandler(fmt.Sprintf("%s:%d", *listen, *monitoringPort))

	http.Handle("/healthz", env.GetHealthChecker().LivenessHandler())
	http.Handle("/readyz", env.GetHealthChecker().ReadinessHandler())

	schedulerOpts := &scheduler_client.Options{}
	reg, err := scheduler_client.NewRegistration(env, taskScheduler, executorID, executor.HostID(), schedulerOpts)
	if err != nil {
		log.Fatalf("Error initializing executor registration: %s", err)
	}

	warmupDone := make(chan struct{})
	go func() {
		executor.Warmup()
		close(warmupDone)
	}()
	go func() {
		if executorConfig.StartupWarmupMaxWaitSecs != 0 {
			warmupMaxWait := time.Duration(executorConfig.StartupWarmupMaxWaitSecs) * time.Second
			select {
			case <-warmupDone:
			case <-time.After(warmupMaxWait):
				log.Warningf("Warmup did not finish within %s, resuming startup", warmupMaxWait)
			}
		}
		log.Infof("Registering executor with server.")
		reg.Start(rootContext)
	}()

	go func() {
		localServer.Serve(localListener)
	}()

	go func() {
		http.ListenAndServe(fmt.Sprintf("%s:%d", *listen, *port), nil)
	}()
	env.GetHealthChecker().WaitForGracefulShutdown()
}
