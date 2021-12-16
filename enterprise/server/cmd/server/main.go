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
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/version"
	"google.golang.org/api/option"

	bundle "github.com/buildbuddy-io/buildbuddy/enterprise"
	telserver "github.com/buildbuddy-io/buildbuddy/enterprise/server/telemetry"
	workflow "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	httpfilters "github.com/buildbuddy-io/buildbuddy/server/http/filters"
)

var (
	configFile = flag.String("config_file", "/config.yaml", "The path to a buildbuddy config file")
	serverType = flag.String("server_type", "buildbuddy-server", "The server type to match on health checks")
)

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

	if env.GetConfigurator().GetAppUsageEnabled() {
		env.SetUsageService(usage_service.New(env))
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
	flag.Parse()
	rootContext := context.Background()
	version.Print()

	configurator, err := config.NewConfigurator(*configFile)
	if err != nil {
		log.Fatalf("Error loading config from file: %s", err)
	}
	healthChecker := healthcheck.NewHealthChecker(*serverType)
	realEnv := libmain.GetConfiguredEnvironmentOrDie(rootContext, configurator, healthChecker)
	if err := tracing.Configure(configurator, healthChecker); err != nil {
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

	if redisTarget := configurator.GetCacheRedisTarget(); redisTarget != "" {
		redisClient := redisutil.NewClient(redisTarget, healthChecker, "cache_redis")
		realEnv.SetCacheRedisClient(redisClient)
	}

	if redisTarget := configurator.GetDefaultRedisTarget(); redisTarget != "" {
		rdb := redisutil.NewClient(redisTarget, healthChecker, "default_redis")
		realEnv.SetDefaultRedisClient(rdb)

		rbuf := redisutil.NewCommandBuffer(rdb)
		rbuf.StartPeriodicFlush(context.Background())
		realEnv.GetHealthChecker().RegisterShutdownFunction(rbuf.StopPeriodicFlush)

		rkv := redis_kvstore.New(rdb)
		realEnv.SetKeyValStore(rkv)
		rmc := redis_metrics_collector.New(rdb, rbuf)
		realEnv.SetMetricsCollector(rmc)

		if configurator.GetAppUsageTrackingEnabled() {
			region := realEnv.GetConfigurator().GetAppRegion()
			if region == "" {
				log.Fatalf("Usage tracking requires app.region to be configured.")
			}
			opts := &usage.TrackerOpts{Region: region}
			ut := usage.NewTracker(
				realEnv, timeutil.NewClock(), usage.NewFlushLock(realEnv), rbuf, opts)
			realEnv.SetUsageTracker(ut)

			ut.StartDBFlush()
			healthChecker.RegisterShutdownFunction(func(ctx context.Context) error {
				ut.StopDBFlush()
				return nil
			})
		}
	} else if configurator.GetAppUsageTrackingEnabled() {
		log.Fatalf("Usage tracking is enabled, but no Redis client is configured.")
	}

	if redisTarget := configurator.GetRemoteExecutionRedisTarget(); redisTarget != "" {
		redisClient := redisutil.NewClient(redisTarget, healthChecker, "remote_execution_redis")
		realEnv.SetRemoteExecutionRedisClient(redisClient)

		// Task router uses the remote execution redis client.
		taskRouter, err := task_router.New(realEnv)
		if err != nil {
			log.Fatalf("Failed to create server: %s", err)
		}
		realEnv.SetTaskRouter(taskRouter)
	}

	if rbeConfig := configurator.GetRemoteExecutionConfig(); rbeConfig != nil {
		if redisTarget := configurator.GetRemoteExecutionRedisTarget(); redisTarget != "" {
			opts := redisutil.TargetToOptions(redisTarget)
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
			redisClient := redisutil.NewClientWithOpts(opts, healthChecker, "remote_execution_redis_pubsub")
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
			dcConfig.PubSub = pubsub.NewPubSub(redisutil.NewClient(dcc.RedisTarget, healthChecker, "distributed_cache_redis"))
		}
		dc, err := distributed.NewDistributedCache(realEnv, realEnv.GetCache(), dcConfig, healthChecker)
		if err != nil {
			log.Fatalf("Error enabling distributed cache: %s", err.Error())
		}
		dc.StartListening()
		realEnv.SetCache(dc)
	}

	if mcTargets := configurator.GetCacheMemcacheTargets(); len(mcTargets) > 0 {
		log.Infof("Enabling memcache layer with targets: %s", mcTargets)
		mc := memcache.NewCache(mcTargets...)
		realEnv.SetCache(composable_cache.NewComposableCache(mc, realEnv.GetCache(), composable_cache.ModeReadThrough|composable_cache.ModeWriteThrough))
	} else if redisTarget := configurator.GetCacheRedisTarget(); redisTarget != "" {
		log.Infof("Enabling redis layer with targets: %s", redisTarget)
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
