package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/gcs_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/memcache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/s3_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/runner"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_task_scheduler"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/buildbuddy-io/buildbuddy/server/xcode"
	"github.com/google/uuid"
	"google.golang.org/grpc"

	remote_executor "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	_ "github.com/buildbuddy-io/buildbuddy/server/util/grpc_server" // imported for grpc_port flag definition to avoid breaking old configs; DO NOT REMOVE.
	bspb "google.golang.org/genproto/googleapis/bytestream"
	_ "google.golang.org/grpc/encoding/gzip" // imported for side effects; DO NOT REMOVE.
)

var (
	appTarget                = flag.String("executor.app_target", "grpcs://remote.buildbuddy.io", "The GRPC url of a buildbuddy app server.")
	disableLocalCache        = flag.Bool("executor.disable_local_cache", false, "If true, a local file cache will not be used.")
	localCacheDirectory      = flag.String("executor.local_cache_directory", "/tmp/buildbuddy/filecache", "A local on-disk cache directory. Must be on the same device (disk partition, Docker volume, etc.) as the configured root_directory, since files are hard-linked to this cache for performance reasons. Otherwise, 'Invalid cross-device link' errors may result.")
	localCacheSizeBytes      = flag.Int64("executor.local_cache_size_bytes", 1_000_000_000 /* 1 GB */, "The maximum size, in bytes, to use for the local on-disk cache")
	startupWarmupMaxWaitSecs = flag.Int64("executor.startup_warmup_max_wait_secs", 0, "Maximum time to block startup while waiting for default image to be pulled. Default is no wait.")

	listen            = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port              = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	monitoringPort    = flag.Int("monitoring_port", 9090, "The port to listen for monitoring traffic on")
	monitoringSSLPort = flag.Int("monitoring.ssl_port", -1, "If non-negative, the SSL port to listen for monitoring traffic on. `ssl` config must have `ssl_enabled: true` and be properly configured.")
	serverType        = flag.String("server_type", "prod-buildbuddy-executor", "The server type to match on health checks")
)

func InitializeCacheClientsOrDie(cacheTarget string, realEnv *real_environment.RealEnv) {
	var conn *grpc.ClientConn
	var err error
	if cacheTarget == "" {
		log.Fatalf("No cache target was set. Run a local cache or specify one in the config")
	} else if u, err := url.Parse(cacheTarget); err == nil && u.Hostname() == "cloud.buildbuddy.io" {
		log.Warning("You are using the old BuildBuddy endpoint, cloud.buildbuddy.io. Migrate `executor.app_target` to remote.buildbuddy.io for improved performance.")
	}
	conn, err = grpc_client.DialTarget(cacheTarget)
	if err != nil {
		log.Fatalf("Unable to connect to cache '%s': %s", cacheTarget, err)
	}
	log.Infof("Connecting to cache target: %s", cacheTarget)

	realEnv.GetHealthChecker().AddHealthCheck(
		"grpc_cache_connection", healthcheck.NewGRPCHealthCheck(conn))

	realEnv.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	realEnv.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	realEnv.SetActionCacheClient(repb.NewActionCacheClient(conn))
	realEnv.SetCapabilitiesClient(repb.NewCapabilitiesClient(conn))
}

func GetConfiguredEnvironmentOrDie(healthChecker *healthcheck.HealthChecker) environment.Env {
	realEnv := real_environment.NewRealEnv(healthChecker)

	if err := resources.Configure(); err != nil {
		log.Fatal(status.Message(err))
	}
	// Note: Using math.Floor here to match the int64() conversions in
	// scheduler_server.go
	metrics.RemoteExecutionAssignableMilliCPU.Set(math.Floor(float64(resources.GetAllocatedCPUMillis()) * tasksize.MaxResourceCapacityRatio))
	metrics.RemoteExecutionAssignableRAMBytes.Set(math.Floor(float64(resources.GetAllocatedRAMBytes()) * tasksize.MaxResourceCapacityRatio))

	if err := auth.Register(context.Background(), realEnv); err != nil {
		if err := auth.RegisterNullAuth(realEnv); err != nil {
			log.Fatalf("%v", err)
		}
		log.Infof("No authentication will be configured: %s", err)
	}

	xl := xcode.NewXcodeLocator()
	realEnv.SetXcodeLocator(xl)

	if err := gcs_cache.Register(realEnv); err != nil {
		log.Fatal(err.Error())
	}
	if err := s3_cache.Register(realEnv); err != nil {
		log.Fatal(err.Error())
	}

	if err := memcache.Register(realEnv); err != nil {
		log.Fatal(err.Error())
	}
	if err := redis_cache.Register(realEnv); err != nil {
		log.Fatal(err.Error())
	}

	// Identify ourselves as an executor client in gRPC requests to the app.
	usageutil.SetClientType("executor")

	InitializeCacheClientsOrDie(*appTarget, realEnv)

	if !*disableLocalCache {
		log.Infof("Enabling filecache in %q (size %d bytes)", *localCacheDirectory, *localCacheSizeBytes)
		if fc, err := filecache.NewFileCache(*localCacheDirectory, *localCacheSizeBytes); err == nil {
			realEnv.SetFileCache(fc)
		}
	}

	conn, err := grpc_client.DialTarget(*appTarget)
	if err != nil {
		log.Fatalf("Unable to connect to app '%s': %s", *appTarget, err)
	}
	log.Infof("Connecting to app target: %s", *appTarget)

	realEnv.GetHealthChecker().AddHealthCheck(
		"grpc_app_connection", healthcheck.NewGRPCHealthCheck(conn))
	realEnv.SetSchedulerClient(scpb.NewSchedulerClient(conn))
	realEnv.SetRemoteExecutionClient(repb.NewExecutionClient(conn))

	return realEnv
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

	if err := log.Configure(); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1)
	}

	setupNetworking(rootContext)

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := GetConfiguredEnvironmentOrDie(healthChecker)

	if err := tracing.Configure(env); err != nil {
		log.Fatalf("Could not configure tracing: %s", err)
	}

	executorUUID, err := uuid.NewRandom()
	if err != nil {
		log.Fatalf("Failed to generate executor instance ID: %s", err)
	}
	executorID := executorUUID.String()

	runnerPool, err := runner.NewPool(env, &runner.PoolOptions{})
	if err != nil {
		log.Fatalf("Failed to initialize runner pool: %s", err)
	}

	opts := &remote_executor.Options{}
	executor, err := remote_executor.NewExecutor(env, executorID, runnerPool, opts)
	if err != nil {
		log.Fatalf("Error initializing ExecutionServer: %s", err)
	}
	taskScheduler := priority_task_scheduler.NewPriorityTaskScheduler(env, executor, runnerPool, &priority_task_scheduler.Options{})
	if err := taskScheduler.Start(); err != nil {
		log.Fatalf("Error starting task scheduler: %v", err)
	}

	container.Metrics.Start(rootContext)
	monitoring.StartMonitoringHandler(fmt.Sprintf("%s:%d", *listen, *monitoringPort))

	// Setup SSL for monitoring endpoints (optional).
	if *monitoringSSLPort >= 0 {
		if err := ssl.Register(env); err != nil {
			log.Fatal(err.Error())
		}
		if err := monitoring.StartSSLMonitoringHandler(env, fmt.Sprintf("%s:%d", *listen, *monitoringSSLPort)); err != nil {
			log.Fatal(err.Error())
		}
	}

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
		if warmupMaxWait := time.Duration(*startupWarmupMaxWaitSecs) * time.Second; warmupMaxWait != 0 {
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
		http.ListenAndServe(fmt.Sprintf("%s:%d", *listen, *port), nil)
	}()
	env.GetHealthChecker().WaitForGracefulShutdown()
}
