package main

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/gcs_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/memcache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/s3_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/runner"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaputil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_task_scheduler"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/hostid"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/vtprotocodec"
	"github.com/buildbuddy-io/buildbuddy/server/xcode"
	"github.com/google/uuid"

	_ "github.com/buildbuddy-io/buildbuddy/server/util/grpc_server" // imported for grpc_port flag definition to avoid breaking old configs; DO NOT REMOVE.
	_ "google.golang.org/grpc/encoding/gzip"                        // imported for side effects; DO NOT REMOVE.

	remote_executor "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	appTarget                 = flag.String("executor.app_target", "grpcs://remote.buildbuddy.io", "The GRPC url of a buildbuddy app server.")
	disableLocalCache         = flag.Bool("executor.disable_local_cache", false, "If true, a local file cache will not be used.")
	deleteFileCacheOnStartup  = flag.Bool("executor.delete_filecache_on_startup", false, "If true, delete the file cache on startup")
	deleteBuildRootOnStartup  = flag.Bool("executor.delete_build_root_on_startup", false, "If true, delete the build root on startup")
	executorMedadataDirectory = flag.String("executor.metadata_directory", "", "Location where executor host_id and other metadata is stored. Defaults to executor.local_cache_directory/../")
	localCacheDirectory       = flag.String("executor.local_cache_directory", "/tmp/buildbuddy/filecache", "A local on-disk cache directory. Must be on the same device (disk partition, Docker volume, etc.) as the configured root_directory, since files are hard-linked to this cache for performance reasons. Otherwise, 'Invalid cross-device link' errors may result.")
	localCacheSizeBytes       = flag.Int64("executor.local_cache_size_bytes", 1_000_000_000 /* 1 GB */, "The maximum size, in bytes, to use for the local on-disk cache")
	startupWarmupMaxWaitSecs  = flag.Int64("executor.startup_warmup_max_wait_secs", 0, "Maximum time to block startup while waiting for default image to be pulled. Default is no wait.")

	listen            = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port              = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	monitoringPort    = flag.Int("monitoring_port", 9090, "The port to listen for monitoring traffic on")
	monitoringSSLPort = flag.Int("monitoring.ssl_port", -1, "If non-negative, the SSL port to listen for monitoring traffic on. `ssl` config must have `ssl_enabled: true` and be properly configured.")
	serverType        = flag.String("server_type", "prod-buildbuddy-executor", "The server type to match on health checks")
)

func init() {
	// Register the codec for all RPC servers and clients.
	vtprotocodec.Register()
}

func InitializeCacheClientsOrDie(cacheTarget string, realEnv *real_environment.RealEnv) {
	var err error
	if cacheTarget == "" {
		log.Fatalf("No cache target was set. Run a local cache or specify one in the config")
	} else if u, err := url.Parse(cacheTarget); err == nil && u.Hostname() == "cloud.buildbuddy.io" {
		log.Warning("You are using the old BuildBuddy endpoint, cloud.buildbuddy.io. Migrate `executor.app_target` to remote.buildbuddy.io for improved performance.")
	}
	conn, err := grpc_client.DialInternal(realEnv, cacheTarget)
	if err != nil {
		log.Fatalf("Unable to connect to cache '%s': %s", cacheTarget, err)
	}
	log.Infof("Connecting to cache target: %s", cacheTarget)

	realEnv.GetHealthChecker().AddHealthCheck("grpc_cache_connection", conn)

	realEnv.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	realEnv.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	realEnv.SetActionCacheClient(repb.NewActionCacheClient(conn))
	realEnv.SetCapabilitiesClient(repb.NewCapabilitiesClient(conn))
}

func getExecutorHostID() string {
	mdDir := *executorMedadataDirectory
	if mdDir == "" {
		mdDir = filepath.Join(filepath.Dir(*localCacheDirectory), "metadata")
	}
	var hostID string
	if err := disk.EnsureDirectoryExists(mdDir); err == nil {
		if h, err := hostid.GetHostID(mdDir); err == nil {
			hostID = h
		}
	}
	if hostID == "" {
		log.Warning("Unable to get stable BuildBuddy HostID; filecache will not be reused across process restarts.")
		hostID = hostid.GetFailsafeHostID(mdDir)
	}
	return hostID
}

func GetConfiguredEnvironmentOrDie(healthChecker *healthcheck.HealthChecker) *real_environment.RealEnv {
	realEnv := real_environment.NewRealEnv(healthChecker)

	snapshotSharingEnabled := *snaputil.EnableLocalSnapshotSharing || *snaputil.EnableRemoteSnapshotSharing
	if err := resources.Configure(snapshotSharingEnabled); err != nil {
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
	if err := clientidentity.Register(realEnv); err != nil {
		log.Fatalf(err.Error())
	}

	// Identify ourselves as an executor client in gRPC requests to the app.
	usageutil.SetClientType("executor")

	InitializeCacheClientsOrDie(*appTarget, realEnv)

	if !*disableLocalCache {
		fcDir := filepath.Join(*localCacheDirectory, getExecutorHostID())
		log.Infof("Enabling filecache in %q (size %d bytes)", fcDir, *localCacheSizeBytes)
		if fc, err := filecache.NewFileCache(fcDir, *localCacheSizeBytes, *deleteFileCacheOnStartup); err == nil {
			realEnv.SetFileCache(fc)
		}
	}

	conn, err := grpc_client.DialInternal(realEnv, *appTarget)
	if err != nil {
		log.Fatalf("Unable to connect to app '%s': %s", *appTarget, err)
	}
	log.Infof("Connecting to app target: %s", *appTarget)

	realEnv.GetHealthChecker().AddHealthCheck("grpc_app_connection", conn)
	realEnv.SetSchedulerClient(scpb.NewSchedulerClient(conn))
	realEnv.SetRemoteExecutionClient(repb.NewExecutionClient(conn))
	realEnv.SetCommandRunner(&commandutil.CommandRunner{})

	return realEnv
}

func main() {
	setUmask()

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

	// Note: cleanupFUSEMounts needs to happen before deleteBuildRootOnStartup.
	cleanupFUSEMounts()

	if *deleteBuildRootOnStartup {
		rootDir := runner.GetBuildRoot()
		if err := os.RemoveAll(rootDir); err != nil {
			log.Warningf("Failed to remove build root dir: %s", err)
		}
		if err := disk.EnsureDirectoryExists(rootDir); err != nil {
			log.Warningf("Failed to create build root dir: %s", err)
		}
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

	imageCacheAuth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	env.SetImageCacheAuthenticator(imageCacheAuth)

	runnerPool, err := runner.NewPool(env, &runner.PoolOptions{})
	if err != nil {
		log.Fatalf("Failed to initialize runner pool: %s", err)
	}

	opts := &remote_executor.Options{}
	executor, err := remote_executor.NewExecutor(env, executorID, getExecutorHostID(), runnerPool, opts)
	if err != nil {
		log.Fatalf("Error initializing ExecutionServer: %s", err)
	}
	taskScheduler := priority_task_scheduler.NewPriorityTaskScheduler(env, executor, runnerPool, &priority_task_scheduler.Options{})
	if err := taskScheduler.Start(); err != nil {
		log.Fatalf("Error starting task scheduler: %v", err)
	}

	container.Metrics.Start(rootContext)
	monitoring.StartMonitoringHandler(env, fmt.Sprintf("%s:%d", *listen, *monitoringPort))

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
