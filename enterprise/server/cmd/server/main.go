package main

import (
	"context"
	"flag"
	"io/fs"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/api"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/authdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/distributed"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/gcs_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/kms"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/memcache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/migration_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_execution_collector"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_kvstore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/s3_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/userdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/execution_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/hostedrunner"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_search_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_stat_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/quota"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/execution_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/saml"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/task_router"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/secrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/selfauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/splash"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/suggestion"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/bitbucket"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/janitor"
	"github.com/buildbuddy-io/buildbuddy/server/libmain"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/static"
	"github.com/buildbuddy-io/buildbuddy/server/telemetry"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/fileresolver"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/version"

	bundle "github.com/buildbuddy-io/buildbuddy/enterprise"
	raft_cache "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/cache"
	remote_execution_redis_client "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/redis_client"
	telserver "github.com/buildbuddy-io/buildbuddy/enterprise/server/telemetry"
	workflow "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
)

var serverType = flag.String("server_type", "buildbuddy-server", "The server type to match on health checks")

func configureFilesystemsOrDie(realEnv *real_environment.RealEnv) {
	// Ensure we always override the app filesystem because the enterprise
	// binary bundles a different app than the OS one does.
	// The static filesystem is the same, however, so we set it if the flag
	// is set, but we do not fall back to an embedded staticFS.
	realEnv.SetAppFilesystem(nil)
	if staticDirectory, err := flagutil.GetDereferencedValue[string]("static_directory"); err == nil && staticDirectory != "" {
		staticFS, err := static.FSFromRelPath(staticDirectory)
		if err != nil {
			log.Fatalf("Error getting static FS from relPath: %q: %s", staticDirectory, err)
		}
		realEnv.SetStaticFilesystem(staticFS)
	}
	if appDirectory, err := flagutil.GetDereferencedValue[string]("app_directory"); err == nil && appDirectory != "" {
		appFS, err := static.FSFromRelPath(appDirectory)
		if err != nil {
			log.Fatalf("Error getting app FS from relPath: %q: %s", appDirectory, err)
		}
		realEnv.SetAppFilesystem(appFS)
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
	env.SetAuthDB(authdb.NewAuthDB(env, env.GetDBHandle()))
	configureFilesystemsOrDie(env)

	if err := auth.Register(ctx, env); err != nil {
		if err := auth.RegisterNullAuth(env); err != nil {
			log.Fatalf("%v", err)
		}
		log.Warningf("No authentication will be configured: %s", err)
	}
	if err := saml.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

	userDB, err := userdb.NewUserDB(env, env.GetDBHandle())
	if err != nil {
		log.Fatalf("Error setting up prod user DB: %s", err)
	}
	env.SetUserDB(userDB)

	if err := clickhouse.Register(env); err != nil {
		log.Fatalf("%v", err)
	}
	stat := invocation_stat_service.NewInvocationStatService(env, env.GetDBHandle(), env.GetOLAPDBHandle())
	env.SetInvocationStatService(stat)

	search := invocation_search_service.NewInvocationSearchService(env, env.GetDBHandle())
	env.SetInvocationSearchService(search)

	if err := usage_service.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

	if err := api.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

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

	if err := config.Load(); err != nil {
		log.Fatalf("Error loading config from file: %s", err)
	}

	config.ReloadOnSIGHUP()

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	realEnv := libmain.GetConfiguredEnvironmentOrDie(healthChecker)
	if err := tracing.Configure(realEnv); err != nil {
		log.Fatalf("Could not configure tracing: %s", err)
	}

	// Setup the prod fanciness in our environment
	convertToProdOrDie(rootContext, realEnv)

	if err := gcs_cache.Register(realEnv); err != nil {
		log.Fatal(err.Error())
	}
	if err := s3_cache.Register(realEnv); err != nil {
		log.Fatal(err.Error())
	}
	if err := pebble_cache.Register(realEnv); err != nil {
		log.Fatal(err.Error())
	}
	if err := migration_cache.Register(realEnv); err != nil {
		log.Fatal(err.Error())
	}
	if err := redis_client.RegisterDefault(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := redis_kvstore.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := redis_metrics_collector.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := redis_execution_collector.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := usage.RegisterTracker(realEnv); err != nil {
		log.Fatalf("%v", err)
	}

	if err := quota.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}

	if err := redis_client.RegisterRemoteExecutionRedisClient(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := task_router.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := tasksize.Register(realEnv); err != nil {
		log.Fatal(err.Error())
	}

	if err := remote_execution_redis_client.RegisterRemoteExecutionClient(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := remote_execution_redis_client.RegisterRemoteExecutionRedisPubSubClient(realEnv); err != nil {
		log.Fatalf("%v", err)
	}

	if err := distributed.Register(realEnv); err != nil {
		log.Fatal(err.Error())
	}

	if err := raft_cache.Register(realEnv); err != nil {
		log.Fatal(err.Error())
	}

	if err := memcache.Register(realEnv); err != nil {
		log.Fatal(err.Error())
	}
	if err := redis_cache.Register(realEnv); err != nil {
		log.Fatal(err.Error())
	}

	if err := execution_server.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := scheduler_server.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := remote_execution_redis_client.RegisterRemoteExecutionClient(realEnv); err != nil {
		log.Fatalf("%v", err)
	}

	if err := registry.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}

	if err := kms.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := secrets.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := suggestion.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
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

	if err := selfauth.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}

	libmain.StartAndRunServices(realEnv) // Returns after graceful shutdown
}
