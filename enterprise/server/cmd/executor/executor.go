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
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/podman"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/runner"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_task_scheduler"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_client"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/xcode"
	"github.com/google/uuid"
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
	appTarget                = flag.String("executor.app_target", "grpcs://remote.buildbuddy.io", "The GRPC url of a buildbuddy app server.")
	disableLocalCache        = flag.Bool("executor.disable_local_cache", false, "If true, a local file cache will not be used.")
	localCacheDirectory      = flag.String("executor.local_cache_directory", "/tmp/buildbuddy/filecache", "A local on-disk cache directory. Must be on the same device (disk partition, Docker volume, etc.) as the configured root_directory, since files are hard-linked to this cache for performance reasons. Otherwise, 'Invalid cross-device link' errors may result.")
	localCacheSizeBytes      = flag.Int64("executor.local_cache_size_bytes", 1_000_000_000 /* 1 GB */, "The maximum size, in bytes, to use for the local on-disk cache")
	startupWarmupMaxWaitSecs = flag.Int64("executor.startup_warmup_max_wait_secs", 0, "Maximum time to block startup while waiting for default image to be pulled. Default is no wait.")

	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port           = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	monitoringPort = flag.Int("monitoring_port", 9090, "The port to listen for monitoring traffic on")
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

func GetConfiguredEnvironmentOrDie(healthChecker *healthcheck.HealthChecker) environment.Env {
	realEnv := real_environment.NewRealEnv(healthChecker)

	if err := resources.Configure(); err != nil {
		log.Fatal(status.Message(err))
	}

	bundleFS, err := bundle.Get()
	if err != nil {
		log.Fatalf("Failed to initialize bundle: %s", err)
	}
	realEnv.SetFileResolver(fileresolver.New(bundleFS, "enterprise"))

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

	useLocalCache := realEnv.GetCache() != nil
	InitializeCacheClientsOrDie(*appTarget, realEnv, useLocalCache)

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

	flag.Parse()
	if err := flagutil.PopulateFlagsFromFile(config.Path()); err != nil {
		log.Fatalf("Error loading config from file: %s", err)
	}

	if err := log.Configure(); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1)
	}

	if err := networking.ConfigurePolicyBasedRoutingForSecondaryNetwork(rootContext); err != nil {
		fmt.Printf("Error configuring secondary network: %s", err)
		os.Exit(1)
	}

	if networking.IsSecondaryNetworkEnabled() {
		if err := podman.ConfigureSecondaryNetwork(rootContext); err != nil {
			fmt.Printf("Error configuring secondary network for podman: %s", err)
			os.Exit(1)
		}
	}

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	localListener = bufconn.Listen(1024 * 1024 * 10 /* 10MB buffer? Seems ok. */)

	env := GetConfiguredEnvironmentOrDie(healthChecker)

	if err := tracing.Configure(env); err != nil {
		log.Fatalf("Could not configure tracing: %s", err)
	}

	grpcOptions := grpc_server.CommonGRPCServerOptions(env)
	localServer := grpc.NewServer(grpcOptions...)

	// Start Build-Event-Protocol and Remote-Cache services.
	executorUUID, err := uuid.NewRandom()
	if err != nil {
		log.Fatalf("Failed to generate executor instance ID: %s", err)
	}
	executorID := executorUUID.String()

	runnerPool, err := runner.NewPool(env)
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
		localServer.Serve(localListener)
	}()

	go func() {
		http.ListenAndServe(fmt.Sprintf("%s:%d", *listen, *port), nil)
	}()
	env.GetHealthChecker().WaitForGracefulShutdown()
}
