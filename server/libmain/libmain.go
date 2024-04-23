package libmain

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"runtime"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/github"
	"github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_kvstore"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/server/backends/repo_downloader"
	"github.com/buildbuddy-io/buildbuddy/server/backends/slack"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_proxy"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_server"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/webhooks"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/http/protolet"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_asset/fetch_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_asset/push_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_client"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/splash"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/static"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/rlimit"
	"github.com/buildbuddy-io/buildbuddy/server/util/scratchspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/vtprotocodec"

	"google.golang.org/grpc"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	socipb "github.com/buildbuddy-io/buildbuddy/proto/soci"
	bburl "github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	static_bundle "github.com/buildbuddy-io/buildbuddy/static"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	channelzservice "google.golang.org/grpc/channelz/service"
)

var (
	listen           = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port             = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	sslPort          = flag.Int("ssl_port", 8081, "The port to listen for HTTPS traffic on")
	internalHTTPPort = flag.Int("internal_http_port", 0, "The port to listen for internal HTTP traffic")
	monitoringPort   = flag.Int("monitoring_port", 9090, "The port to listen for monitoring traffic on")

	staticDirectory = flag.String("static_directory", "", "the directory containing static files to host")
	appDirectory    = flag.String("app_directory", "", "the directory containing app binary files to host")

	exitWhenReady = flag.Bool("exit_when_ready", false, "If set, the app will exit as soon as it becomes ready (useful for migrations)")

	mutexProfileFraction = flag.Int("mutex_profile_fraction", 0, "The fraction of mutex contention events reported. (1/rate, 0 disables)")
	blockProfileRate     = flag.Int("block_profile_rate", 0, "The fraction of goroutine blocking events reported. (1/rate, 0 disables)")

	// URL path prefixes that should be handled by serving the app's HTML.
	appRoutes = []string{
		"/compare/",
		"/docs/",
		"/history/",
		"/invocation/",
		"/join/",
		"/org/",
		"/settings/",
		"/tests/",
		"/trends/",
		"/usage/",
		"/workflows/",
		"/executors/",
		"/code/",
		"/search/",
		"/audit-logs/",
		"/repo/",
		"/reviews/",
	}
)

func init() {
	grpc.EnableTracing = false
	// Register the codec for all RPC servers and clients.
	vtprotocodec.Register()
}

func configureFilesystemsOrDie(realEnv *real_environment.RealEnv, appBundleFS fs.FS) {
	if err := scratchspace.Init(); err != nil {
		log.Fatalf("Failed to initialize temp storage directory: %s", err)
	}
	if *staticDirectory != "" {
		staticFS, err := static.FSFromRelPath(*staticDirectory)
		if err != nil {
			log.Fatalf("Error getting static FS from relPath: %q: %s", *staticDirectory, err)
		}
		realEnv.SetStaticFilesystem(staticFS)
	}
	if *appDirectory != "" {
		appFS, err := static.FSFromRelPath(*appDirectory)
		if err != nil {
			log.Fatalf("Error getting app FS from relPath: %q: %s", *appDirectory, err)
		}
		realEnv.SetAppFilesystem(appFS)
	}
	if realEnv.GetStaticFilesystem() == nil {
		staticFS, err := static_bundle.GetStaticFS()
		if err != nil {
			log.Fatalf("Error getting static FS from bundle: %s", err)
		}
		log.Debug("Using bundled static filesystem.")
		realEnv.SetStaticFilesystem(staticFS)
	}
	if realEnv.GetAppFilesystem() == nil {
		log.Debug("Using bundled app filesystem.")
		realEnv.SetAppFilesystem(appBundleFS)
	}
}

// Normally this code would live in main.go -- we put it here for now because
// the environments used by the open-core version and the enterprise version are
// not substantially different enough yet to warrant the extra complexity of
// always updating both main files.
func GetConfiguredEnvironmentOrDie(healthChecker *healthcheck.HealthChecker, appBundleFS fs.FS) *real_environment.RealEnv {
	if err := log.Configure(); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1)
	}
	realEnv := real_environment.NewRealEnv(healthChecker)
	realEnv.SetMux(tracing.NewHttpServeMux(http.NewServeMux()))
	realEnv.SetInternalHTTPMux(tracing.NewHttpServeMux(http.NewServeMux()))
	realEnv.SetAuthenticator(&nullauth.NullAuthenticator{})
	configureFilesystemsOrDie(realEnv, appBundleFS)

	dbHandle, err := db.GetConfiguredDatabase(context.Background(), realEnv)
	if err != nil {
		log.Fatalf("Error configuring database: %s", err)
	}
	log.Infof("Successfully configured %s database.", dbHandle.GORM(context.Background(), "dialector").Dialector.Name())
	realEnv.SetDBHandle(dbHandle)
	realEnv.SetInvocationDB(invocationdb.NewInvocationDB(realEnv, dbHandle))

	bs, err := blobstore.GetConfiguredBlobstore(realEnv)
	if err != nil {
		log.Fatalf("Error configuring blobstore: %s", err)
	}
	realEnv.SetBlobstore(bs)

	realEnv.SetWebhooks(make([]interfaces.Webhook, 0))
	if err := slack.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	if err := webhooks.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}

	if err := build_event_proxy.Register(realEnv); err != nil {
		log.Fatalf("%v", err)
	}
	realEnv.SetBuildEventHandler(build_event_handler.NewBuildEventHandler(realEnv))

	// If configured, enable the cache.
	if err := memory_cache.Register(realEnv); err != nil {
		log.Fatal(err.Error())
	}
	if err := disk_cache.Register(realEnv); err != nil {
		log.Fatal(err.Error())
	}
	if realEnv.GetCache() != nil {
		log.Printf("Cache: BuildBuddy cache API enabled!")
	}

	realEnv.SetSplashPrinter(&splash.Printer{})

	if err := gossip.Register(realEnv); err != nil {
		log.Fatal(err.Error())
	}

	collector, err := memory_metrics_collector.NewMemoryMetricsCollector()
	if err != nil {
		log.Fatalf("Error configuring in-memory metrics collector: %s", err.Error())
	}
	realEnv.SetMetricsCollector(collector)

	keyValStore, err := memory_kvstore.NewMemoryKeyValStore()
	if err != nil {
		log.Fatalf("Error configuring in-memory proto store: %s", err.Error())
	}
	realEnv.SetKeyValStore(keyValStore)

	realEnv.SetRepoDownloader(repo_downloader.NewRepoDownloader())
	return realEnv
}

func startInternalGRPCServers(env *real_environment.RealEnv) error {
	b, err := grpc_server.New(env, grpc_server.InternalGRPCPort(), false /*=ssl*/, grpc_server.GRPCServerConfig{})
	if err != nil {
		return err
	}
	registerInternalServices(env, b.GetServer())
	if err = b.Start(); err != nil {
		return err
	}
	env.SetInternalGRPCServer(b.GetServer())

	if env.GetSSLService().IsEnabled() {
		sb, err := grpc_server.New(env, grpc_server.InternalGRPCSPort(), true /*=ssl*/, grpc_server.GRPCServerConfig{})
		if err != nil {
			return err
		}
		registerInternalServices(env, sb.GetServer())
		if err = sb.Start(); err != nil {
			return err
		}
		env.SetInternalGRPCSServer(sb.GetServer())
	}
	return nil
}

func registerInternalServices(env *real_environment.RealEnv, grpcServer *grpc.Server) {
	if sociArtifactStoreServer := env.GetSociArtifactStoreServer(); sociArtifactStoreServer != nil {
		socipb.RegisterSociArtifactStoreServer(grpcServer, sociArtifactStoreServer)
	}
	channelzservice.RegisterChannelzServiceToServer(grpcServer)
}

func startGRPCServers(env *real_environment.RealEnv) error {
	b, err := grpc_server.New(env, grpc_server.GRPCPort(), false /*=ssl*/, grpc_server.GRPCServerConfig{})
	if err != nil {
		return err
	}
	registerServices(env, b.GetServer())
	if err = b.Start(); err != nil {
		return err
	}
	env.SetGRPCServer(b.GetServer())
	grpc_server.EnableGRPCOverHTTP(env, b.GetServer())

	if env.GetSSLService().IsEnabled() {
		sb, err := grpc_server.New(env, grpc_server.GRPCSPort(), true /*=ssl*/, grpc_server.GRPCServerConfig{})
		if err != nil {
			return err
		}
		registerServices(env, sb.GetServer())
		if err = sb.Start(); err != nil {
			return err
		}
		env.SetGRPCSServer(sb.GetServer())
	}
	return nil
}

func registerServices(env *real_environment.RealEnv, grpcServer *grpc.Server) {
	// Start Build-Event-Protocol and Remote-Cache services.
	pepb.RegisterPublishBuildEventServer(grpcServer, env.GetBuildEventServer())

	if casServer := env.GetCASServer(); casServer != nil {
		// Register to handle content addressable storage (CAS) messages.
		repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	}
	if bsServer := env.GetByteStreamServer(); bsServer != nil {
		// Register to handle bytestream (upload and download) messages.
		bspb.RegisterByteStreamServer(grpcServer, bsServer)
	}
	if acServer := env.GetActionCacheServer(); acServer != nil {
		// Register to handle action cache (upload and download) messages.
		repb.RegisterActionCacheServer(grpcServer, acServer)
	}
	if pushServer := env.GetPushServer(); pushServer != nil {
		rapb.RegisterPushServer(grpcServer, pushServer)
	}
	if fetchServer := env.GetFetchServer(); fetchServer != nil {
		rapb.RegisterFetchServer(grpcServer, fetchServer)
	}
	if rexec := env.GetRemoteExecutionService(); rexec != nil {
		repb.RegisterExecutionServer(grpcServer, rexec)
	}
	if scheduler := env.GetSchedulerService(); scheduler != nil {
		scpb.RegisterSchedulerServer(grpcServer, scheduler)
	}
	repb.RegisterCapabilitiesServer(grpcServer, env.GetCapabilitiesServer())

	bbspb.RegisterBuildBuddyServiceServer(grpcServer, env.GetBuildBuddyServer())

	// Register API Server as a gRPC service.
	if api := env.GetAPIService(); api != nil {
		apipb.RegisterApiServiceServer(grpcServer, api)
	}
}

func registerLocalGRPCClients(env *real_environment.RealEnv) error {
	// Identify ourselves as an app client in gRPC requests to other apps.
	usageutil.SetClientType("app")
	byte_stream_client.RegisterPooledBytestreamClient(env)

	// TODO(jdhollen): Share this pool with the cache above.  Not a huge deal for now.
	conn, err := grpc_client.DialInternal(env, fmt.Sprintf("grpc://localhost:%d", grpc_server.GRPCPort()))
	if err != nil {
		return status.InternalErrorf("Error initializing ByteStreamClient: %s", err)
	}
	if env.GetByteStreamServer() != nil {
		env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	}
	if env.GetActionCacheServer() != nil {
		env.SetActionCacheClient(repb.NewActionCacheClient(conn))
	}
	return nil
}

func StartAndRunServices(env *real_environment.RealEnv) {
	env.SetListenAddr(*listen)

	if err := rlimit.MaxRLimit(); err != nil {
		log.Printf("Error raising open files limit: %s", err)
	}

	runtime.SetMutexProfileFraction(*mutexProfileFraction)
	runtime.SetBlockProfileRate(*blockProfileRate)

	appBundleHash, err := static.AppBundleHash(env.GetAppFilesystem())
	if err != nil {
		log.Fatalf("Error reading app bundle hash: %s", err)
	}

	staticFileServer, err := static.NewStaticFileServer(env, env.GetStaticFilesystem(), appRoutes, appBundleHash)
	if err != nil {
		log.Fatalf("Error initializing static file server: %s", err)
	}

	afs, err := static.NewStaticFileServer(env, env.GetAppFilesystem(), []string{}, "")
	if err != nil {
		log.Fatalf("Error initializing app server: %s", err)
	}

	if err := ssl.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

	// Register to handle BuildBuddy API messages (over gRPC)
	if err := buildbuddy_server.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

	monitoring.StartMonitoringHandler(env, fmt.Sprintf("%s:%d", *listen, *monitoringPort))

	if err := build_event_server.Register(env); err != nil {
		log.Fatalf("%v", err)
	}
	if err := content_addressable_storage_server.Register(env); err != nil {
		log.Fatalf("%v", err)
	}
	if err := byte_stream_server.Register(env); err != nil {
		log.Fatalf("%v", err)
	}
	if err := action_cache_server.Register(env); err != nil {
		log.Fatalf("%v", err)
	}
	if err := push_server.Register(env); err != nil {
		log.Fatalf("%v", err)
	}
	if err := registerLocalGRPCClients(env); err != nil {
		log.Fatal(err.Error())
	}
	if err := fetch_server.Register(env); err != nil {
		log.Fatalf("%v", err)
	}
	if err := capabilities_server.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

	if err := startInternalGRPCServers(env); err != nil {
		log.Fatalf("%v", err)
	}

	if err := startGRPCServers(env); err != nil {
		log.Fatalf("%v", err)
	}

	// Generate HTTP (protolet) handlers for the BuildBuddy API, so it
	// can be called over HTTP(s).
	protoletHandler, err := protolet.GenerateHTTPHandlers("/rpc/BuildBuddyService/", "buildbuddy.service.BuildBuddyService", env.GetBuildBuddyServer(), env.GetGRPCServer())
	if err != nil {
		log.Fatalf("Error initializing RPC over HTTP handlers for BuildBuddy server: %s", err)
	}

	mux := env.GetMux()
	// Register all of our HTTP handlers on the default mux.
	mux.Handle("/", interceptors.WrapExternalHandler(env, staticFileServer))
	for _, appRoute := range appRoutes {
		// this causes the muxer to handle redirects from e. g. /path -> /path/
		mux.Handle(appRoute, interceptors.WrapExternalHandler(env, staticFileServer))
	}
	mux.Handle("/app/", interceptors.WrapExternalHandler(env, http.StripPrefix("/app", afs)))
	mux.Handle("/rpc/BuildBuddyService/", interceptors.WrapAuthenticatedExternalProtoletHandler(env, "/rpc/BuildBuddyService/", protoletHandler))
	mux.Handle("/file/download", interceptors.WrapAuthenticatedExternalHandler(env, env.GetBuildBuddyServer()))
	mux.Handle("/healthz", env.GetHealthChecker().LivenessHandler())
	mux.Handle("/readyz", env.GetHealthChecker().ReadinessHandler())

	auth := env.GetAuthenticator()
	mux.Handle("/login/", interceptors.SetSecurityHeaders(interceptors.RedirectOnError(auth.Login)))
	mux.Handle("/auth/", interceptors.SetSecurityHeaders(interceptors.RedirectOnError(auth.Auth)))
	mux.Handle("/logout/", interceptors.SetSecurityHeaders(interceptors.RedirectOnError(auth.Logout)))

	if err := github.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

	// Register API as an HTTP service.
	if api := env.GetAPIService(); api != nil {
		apiProtoHandlers, err := protolet.GenerateHTTPHandlers("/api/v1/", "api.v1", api, env.GetGRPCServer())
		if err != nil {
			log.Fatalf("Error initializing RPC over HTTP handlers for API: %s", err)
		}
		mux.Handle("/api/v1/", interceptors.WrapAuthenticatedExternalProtoletHandler(env, "/api/v1/", apiProtoHandlers))
		// Protolet doesn't currently support streaming RPCs, so we'll register a regular old http handler.
		mux.Handle("/api/v1/GetFile", interceptors.WrapAuthenticatedExternalHandler(env, api.GetFileHandler()))
		mux.Handle("/api/v1/metrics", interceptors.WrapAuthenticatedExternalHandler(env, api.GetMetricsHandler()))
	}

	if scim := env.GetSCIMService(); scim != nil {
		scim.RegisterHandlers(mux)
	}

	if wfs := env.GetWorkflowService(); wfs != nil {
		mux.Handle("/webhooks/workflow/", interceptors.WrapExternalHandler(env, wfs))
	}
	if gha := env.GetGitHubApp(); gha != nil {
		mux.Handle("/webhooks/github/app", interceptors.WrapExternalHandler(env, gha.WebhookHandler()))
		mux.Handle("/auth/github/app/link/", interceptors.WrapAuthenticatedExternalHandler(env, gha.OAuthHandler()))
	}

	if gcp := env.GetGCPService(); gcp != nil {
		mux.Handle("/auth/gcp/link/", interceptors.WrapAuthenticatedExternalHandler(env, interceptors.RedirectOnError(gcp.Link)))
	}

	if sp := env.GetSplashPrinter(); sp != nil {
		sp.PrintSplashScreen(bburl.WithPath("").Hostname(), *port, grpc_server.GRPCPort())
	}

	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", *listen, *port),
		Handler: env.GetMux(),
	}

	env.GetHTTPServerWaitGroup().Add(1)
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		defer env.GetHTTPServerWaitGroup().Done()
		err := server.Shutdown(ctx)
		return err
	})

	if *internalHTTPPort != 0 {
		lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *listen, *internalHTTPPort))
		if err != nil {
			log.Fatalf("could not listen on internal HTTP port: %s", err)
		}

		internalHTTPServer := &http.Server{
			Handler: env.GetInternalHTTPMux(),
		}

		env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
			err := internalHTTPServer.Shutdown(ctx)
			return err
		})

		go func() {
			_ = internalHTTPServer.Serve(lis)
		}()
	}

	if env.GetSSLService().IsEnabled() {
		tlsConfig, sslHandler := env.GetSSLService().ConfigureTLS(server.Handler)
		sslServer := &http.Server{
			Addr:      fmt.Sprintf("%s:%d", *listen, *sslPort),
			Handler:   server.Handler,
			TLSConfig: tlsConfig,
		}
		go func() {
			sslServer.ListenAndServeTLS("", "")
		}()
		go func() {
			http.ListenAndServe(fmt.Sprintf("%s:%d", *listen, *port), interceptors.RedirectIfNotForwardedHTTPS(sslHandler))
		}()
	} else {
		// If no SSL is enabled, we'll just serve things as-is.
		go func() {
			server.ListenAndServe()
		}()
	}

	if *exitWhenReady {
		env.GetHealthChecker().Shutdown()
		os.Exit(0)
	}
	env.GetHealthChecker().WaitForGracefulShutdown()
}
