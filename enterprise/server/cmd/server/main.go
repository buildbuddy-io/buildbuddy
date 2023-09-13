package main

import (
	"context"
	"flag"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/api"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auditlog"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/authdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/distributed"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/gcs_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/kms"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/memcache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/migration_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/prom"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_execution_collector"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_kvstore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/s3_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/userdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/execution_search_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/execution_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/githubapp"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/hostedrunner"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_search_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_stat_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/iprules"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/quota"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/execution_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/task_router"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/secrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/selfauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/sociartifactstore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/splash"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/suggestion"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/dsingleflight"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/bitbucket"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/janitor"
	"github.com/buildbuddy-io/buildbuddy/server/libmain"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/telemetry"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/version"

	enterprise_app_bundle "github.com/buildbuddy-io/buildbuddy/enterprise/app"
	raft_cache "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/cache"
	remote_execution_redis_client "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/redis_client"
	telserver "github.com/buildbuddy-io/buildbuddy/enterprise/server/telemetry"
	workflow "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
)

var serverType = flag.String("server_type", "buildbuddy-server", "The server type to match on health checks")

// NB: Most of the logic you'd typically find in a main.go file is in
// libmain.go. We put it there to reduce the duplicated code between the open
// source main() entry point and the enterprise main() entry point, both of
// which import from libmain.go.
func convertToProdOrDie(ctx context.Context, env *real_environment.RealEnv) {
	db, err := authdb.NewAuthDB(env, env.GetDBHandle())
	if err != nil {
		log.Fatalf("Could not setup auth DB: %s", err)
	}
	env.SetAuthDB(db)

	if err := auth.Register(ctx, env); err != nil {
		if err := auth.RegisterNullAuth(env); err != nil {
			log.Fatalf("%v", err)
		}
		log.Warningf("No authentication will be configured: %s", err)
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

	search := invocation_search_service.NewInvocationSearchService(env, env.GetDBHandle(), env.GetOLAPDBHandle())
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
	if err := githubapp.Register(env); err != nil {
		log.Fatalf("Failed to register GitHub app: %s", err)
	}

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

	// Flags must be parsed before config secrets integration is enabled since
	// that feature itself depends on flag values.
	flag.Parse()
	if err := configsecrets.Configure(); err != nil {
		log.Fatalf("Could not prepare config secrets provider: %s", err)
	}
	if err := config.Load(); err != nil {
		log.Fatalf("Error loading config from file: %s", err)
	}

	config.ReloadOnSIGHUP()

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	enterpriseAppFS, err := enterprise_app_bundle.GetAppFS()
	if err != nil {
		log.Fatalf("Error getting enterprise app FS from bundle: %s", err)
	}
	realEnv := libmain.GetConfiguredEnvironmentOrDie(healthChecker, enterpriseAppFS)
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

	if err := kms.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := secrets.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := suggestion.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := crypter_service.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := dsingleflight.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := sociartifactstore.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := prom.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := auditlog.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}

	if err := iprules.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}

	executionService := execution_service.NewExecutionService(realEnv)
	realEnv.SetExecutionService(executionService)

	executionSearchService := execution_search_service.NewExecutionSearchService(realEnv, realEnv.GetDBHandle(), realEnv.GetOLAPDBHandle())
	realEnv.SetExecutionSearchService(executionSearchService)

	telemetryServer := telserver.NewTelemetryServer(realEnv, realEnv.GetDBHandle())
	telemetryServer.StartOrDieIfEnabled()

	telemetryClient := telemetry.NewTelemetryClient(realEnv)
	telemetryClient.Start()
	defer telemetryClient.Stop()

	invocationCleanupService := janitor.NewInvocationJanitor(realEnv)
	invocationCleanupService.Start()
	defer invocationCleanupService.Stop()
	executionCleanupService := janitor.NewExecutionJanitor(realEnv)
	executionCleanupService.Start()
	defer executionCleanupService.Stop()

	if err := selfauth.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}

	libmain.StartAndRunServices(realEnv) // Returns after graceful shutdown
}
