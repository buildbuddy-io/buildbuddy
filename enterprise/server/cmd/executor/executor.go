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
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/runner"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaputil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_task_scheduler"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/task_leaser"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cpuset"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/hostid"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/util/canary"
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
	"github.com/buildbuddy-io/buildbuddy/server/version"
	"github.com/buildbuddy-io/buildbuddy/server/xcode"
	"github.com/google/uuid"

	"google.golang.org/grpc"

	_ "github.com/buildbuddy-io/buildbuddy/server/util/grpc_server" // imported for grpc_port flag definition to avoid breaking old configs; DO NOT REMOVE.
	_ "google.golang.org/grpc/encoding/gzip"                        // imported for side effects; DO NOT REMOVE.

	remote_executor "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	appTarget                 = flag.String("executor.app_target", "grpcs://remote.buildbuddy.io", "The GRPC url of a buildbuddy app server.")
	cacheTarget               = flag.String("executor.cache_target", "", "The GRPC url of the remote cache to use. If empty, the value from --executor.app_target is used.")
	cacheTargetTrafficPercent = flag.Int("executor.cache_target_traffic_percent", 100, "The percent of cache traffic to send to --executor.cache_target. If not 100, the remainder will be sent to --executor.app_target.")
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

func isOldEndpoint(endpoint string) bool {
	u, err := url.Parse(endpoint)
	return err == nil && u.Hostname() == "cloud.buildbuddy.io"
}

type cacheClient interface {
	interfaces.Checker
	grpc.ClientConnInterface
}

func dialCacheOrDie(target string, env environment.Env) *grpc_client.ClientConnPool {
	conn, err := grpc_client.DialInternal(env, target)
	if err != nil {
		log.Fatalf("Unable to connect to cache '%s': %s", target, err)
	}
	log.Infof("Connecting to cache target: %s", target)
	return conn
}

func initializeCacheClientsOrDie(appTarget, cacheTarget string, cacheTargetTrafficPercent int, realEnv *real_environment.RealEnv) {
	if isOldEndpoint(appTarget) || isOldEndpoint(cacheTarget) {
		log.Warning("You are using the old BuildBuddy endpoint, cloud.buildbuddy.io. Migrate `executor.app_target` and `executor.cache_target` (if applicable) to remote.buildbuddy.io for improved performance.")
	}

	if appTarget == "" {
		log.Fatalf("No app target was set. Run a local app or specify one in the config")
	} else if cacheTargetTrafficPercent < 0 || cacheTargetTrafficPercent > 100 {
		log.Fatal("--executor.cache_target_traffic_percent must be between 0 and 100 (inclusive)")
	} else if cacheTarget == "" && cacheTargetTrafficPercent > 0 {
		log.Warning("--executor.cache_target_traffic_percent is >0, but --executor.cache_target is empty. Ignoring and using --executor.app_target as cache backend instead.")
	}

	var client cacheClient
	if cacheTarget == "" || cacheTargetTrafficPercent == 0 {
		client = dialCacheOrDie(appTarget, realEnv)
	} else if cacheTargetTrafficPercent == 100 {
		client = dialCacheOrDie(cacheTarget, realEnv)
	} else {
		appClient := dialCacheOrDie(appTarget, realEnv)
		cacheClient := dialCacheOrDie(cacheTarget, realEnv)
		appTargetTrafficPercent := 100 - cacheTargetTrafficPercent
		log.Infof("Sending %d%% of executor-to-cache traffic to %s and %d%% of executor-to-cache traffic to %s",
			appTargetTrafficPercent, appTarget, cacheTargetTrafficPercent, cacheTarget)
		trafficAllocation := map[*grpc_client.ClientConnPool]int{
			appClient:   appTargetTrafficPercent,
			cacheClient: cacheTargetTrafficPercent,
		}
		var err error
		client, err = grpc_client.NewClientConnPoolSplitter(trafficAllocation)
		if err != nil {
			log.Fatalf("Error initialized ClientConnPoolSplitter: %s", err)
		}
	}

	realEnv.GetHealthChecker().AddHealthCheck("grpc_cache_connection", client)

	realEnv.SetByteStreamClient(bspb.NewByteStreamClient(client))
	realEnv.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(client))
	realEnv.SetActionCacheClient(repb.NewActionCacheClient(client))
	realEnv.SetCapabilitiesClient(repb.NewCapabilitiesClient(client))
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

func GetConfiguredEnvironmentOrDie(cacheRoot string, healthChecker *healthcheck.HealthChecker) *real_environment.RealEnv {
	realEnv := real_environment.NewRealEnv(healthChecker)

	mmapLRUEnabled := *platform.EnableFirecracker && snaputil.IsChunkedSnapshotSharingEnabled()
	if err := resources.Configure(mmapLRUEnabled); err != nil {
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

	leaser, err := cpuset.NewLeaser()
	if err != nil {
		log.Fatal(err.Error())
	}
	realEnv.SetCPULeaser(leaser)

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
		log.Fatal(err.Error())
	}

	// Identify ourselves as an executor client in gRPC requests to the app.
	usageutil.SetClientType("executor")

	initializeCacheClientsOrDie(*appTarget, *cacheTarget, *cacheTargetTrafficPercent, realEnv)

	if !*disableLocalCache {
		log.Infof("Enabling filecache in %q (size %d bytes)", cacheRoot, *localCacheSizeBytes)
		if fc, err := filecache.NewFileCache(cacheRoot, *localCacheSizeBytes, *deleteFileCacheOnStartup); err == nil {
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
	version.Print("BuildBuddy executor")

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
		deleteBuildRoot(rootContext, runner.GetBuildRoot())
	}

	setupNetworking(rootContext)

	cacheRoot := filepath.Join(*localCacheDirectory, getExecutorHostID())
	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := GetConfiguredEnvironmentOrDie(cacheRoot, healthChecker)

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

	tasksCgroupParent, err := setupCgroups()
	if err != nil {
		log.Fatalf("cgroup setup failed: %s", err)
	}

	runnerPool, err := runner.NewPool(env, cacheRoot, &runner.PoolOptions{
		CgroupParent: tasksCgroupParent,
	})
	if err != nil {
		log.Fatalf("Failed to initialize runner pool: %s", err)
	}

	executor, err := remote_executor.NewExecutor(env, executorID, getExecutorHostID(), runnerPool)
	if err != nil {
		log.Fatalf("Error initializing ExecutionServer: %s", err)
	}
	taskLeaser := task_leaser.NewTaskLeaser(env, executorID)
	taskScheduler := priority_task_scheduler.NewPriorityTaskScheduler(env, executor, runnerPool, taskLeaser, &priority_task_scheduler.Options{})
	if err := taskScheduler.Start(); err != nil {
		log.Fatalf("Error starting task scheduler: %v", err)
	}

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

func deleteBuildRoot(ctx context.Context, rootDir string) {
	log.Infof("Deleting build root dir at %q", rootDir)
	stop := canary.StartWithLateFn(1*time.Minute, func(timeTaken time.Duration) {
		log.Infof("Still deleting build root dir (%s elapsed)", timeTaken)
	}, func(timeTaken time.Duration) {})
	defer stop()

	if err := disk.ForceRemove(ctx, rootDir); err != nil {
		log.Warningf("Failed to remove build root dir: %s", err)
	}
	if err := disk.EnsureDirectoryExists(rootDir); err != nil {
		log.Warningf("Failed to create build root dir: %s", err)
	}
}
