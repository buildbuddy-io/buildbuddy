package main

import (
	"context"
	"errors"
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
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor"
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
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	_ "google.golang.org/grpc/encoding/gzip" // imported for side effects; DO NOT REMOVE.
)

var (
	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port           = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	monitoringPort = flag.Int("monitoring_port", 9090, "The port to listen for monitoring traffic on")
	gRPCPort       = flag.Int("grpc_port", 1987, "The port to listen for gRPC traffic on")
	configFile     = flag.String("config_file", "", "The path to a buildbuddy config file")
	serverType     = flag.String("server_type", "prod-buildbuddy-executor", "The server type to match on health checks")
)

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

	xl, err := xcode.NewXcodeLocator()
	if err != nil {
		log.Fatalf("Failed to set XCodeLocator: %s", err)
	}
	realEnv.SetXCodeLocator(xl)

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
	} else if redisTarget := configurator.GetCacheRedisTarget(); redisTarget != "" {
		log.Infof("Enabling redis layer with targets: %s", redisTarget)

		redisClient := redisutil.NewClient(redisTarget, healthChecker, "cache_redis")

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

	// Parse all flags, once and for all.
	flag.Parse()
	rootContext := context.Background()

	configFilePath := *configFile
	if configFilePath == "" {
		_, err := os.Stat("/config.yaml")
		if err == nil {
			configFilePath = "/config.yaml"
		} else if !errors.Is(err, os.ErrNotExist) {
			log.Fatalf("Error loading config file from file: %s", err)
		}
	}

	configurator, err := config.NewConfigurator(configFilePath)
	if err != nil {
		log.Fatalf("Error loading config from file: %s", err)
	}

	opts := log.Opts{
		Level:                  configurator.GetAppLogLevel(),
		EnableShortFileName:    configurator.GetAppLogIncludeShortFileName(),
		EnableGCPLoggingFormat: configurator.GetAppLogEnableGCPLoggingFormat(),
		EnableStructured:       configurator.GetAppEnableStructuredLogging(),
	}
	if err := log.Configure(opts); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1)
	}

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	localListener = bufconn.Listen(1024 * 1024 * 10 /* 10MB buffer? Seems ok. */)

	if err := tracing.Configure(configurator, healthChecker); err != nil {
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
	executionServer, err := executor.NewExecutor(env, executorID, &executor.Options{})
	if err != nil {
		log.Fatalf("Error initializing ExecutionServer: %s", err)
	}
	taskScheduler := priority_task_scheduler.NewPriorityTaskScheduler(env, executionServer, &priority_task_scheduler.Options{})
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
	reg, err := scheduler_client.NewRegistration(env, taskScheduler, executorID, schedulerOpts)
	if err != nil {
		log.Fatalf("Error initializing executor registration: %s", err)
	}

	warmupDone := make(chan struct{})
	go func() {
		executionServer.Warmup()
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
