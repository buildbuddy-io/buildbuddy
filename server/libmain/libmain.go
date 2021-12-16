package libmain

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"

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
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/protolet"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_asset/fetch_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_asset/push_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/splash"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/static"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/fileresolver"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/rlimit"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	bundle "github.com/buildbuddy-io/buildbuddy"
	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	httpfilters "github.com/buildbuddy-io/buildbuddy/server/http/filters"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	_ "google.golang.org/grpc/encoding/gzip" // imported for side effects; DO NOT REMOVE.
)

var (
	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port           = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	sslPort        = flag.Int("ssl_port", 8081, "The port to listen for HTTPS traffic on")
	GRPCPort       = flag.Int("grpc_port", 1985, "The port to listen for gRPC traffic on")
	gRPCSPort      = flag.Int("grpcs_port", 1986, "The port to listen for gRPCS traffic on")
	monitoringPort = flag.Int("monitoring_port", 9090, "The port to listen for monitoring traffic on")

	staticDirectory = flag.String("static_directory", "", "the directory containing static files to host")
	appDirectory    = flag.String("app_directory", "", "the directory containing app binary files to host")

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
	}
)

func init() {
	grpc.EnableTracing = false
}

func configureFilesystemsOrDie(realEnv *real_environment.RealEnv) {
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
	bundleFS, err := bundle.Get()
	if err != nil {
		log.Fatalf("Error getting bundle FS: %s", err)
	}
	realEnv.SetFileResolver(fileresolver.New(bundleFS, ""))
	if realEnv.GetStaticFilesystem() == nil || realEnv.GetAppFilesystem() == nil {
		if realEnv.GetStaticFilesystem() == nil {
			staticFS, err := fs.Sub(bundleFS, "static")
			if err != nil {
				log.Fatalf("Error getting static FS from bundle: %s", err)
			}
			log.Debug("Using bundled static filesystem.")
			realEnv.SetStaticFilesystem(staticFS)
		}
		if realEnv.GetAppFilesystem() == nil {
			appFS, err := fs.Sub(bundleFS, "app")
			if err != nil {
				log.Fatalf("Error getting app FS from bundle: %s", err)
			}
			log.Debug("Using bundled app filesystem.")
			realEnv.SetAppFilesystem(appFS)
		}
	}
}

// Normally this code would live in main.go -- we put it here for now because
// the environments used by the open-core version and the enterprise version are
// not substantially different enough yet to warrant the extra complexity of
// always updating both main files.
func GetConfiguredEnvironmentOrDie(ctx context.Context, configurator *config.Configurator, healthChecker *healthcheck.HealthChecker) *real_environment.RealEnv {
	opts := log.Opts{
		Level:                  configurator.GetAppLogLevel(),
		EnableShortFileName:    configurator.GetAppLogIncludeShortFileName(),
		EnableGCPLoggingFormat: configurator.GetAppLogEnableGCPLoggingFormat(),
		EnableStructured:       configurator.GetAppEnableStructuredLogging(),
		EnableStackTraces:      configurator.GetAppLogErrorStackTraces(),
	}
	if err := log.Configure(opts); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1)
	}
	bs, err := blobstore.GetConfiguredBlobstore(ctx, configurator)
	if err != nil {
		log.Fatalf("Error configuring blobstore: %s", err)
	}
	dbHandle, err := db.GetConfiguredDatabase(configurator, healthChecker)
	if err != nil {
		log.Fatalf("Error configuring database: %s", err)
	}

	realEnv := real_environment.NewRealEnv(configurator, healthChecker)
	realEnv.SetMux(tracing.NewHttpServeMux(http.NewServeMux()))
	configureFilesystemsOrDie(realEnv)
	realEnv.SetDBHandle(dbHandle)
	realEnv.SetBlobstore(bs)
	realEnv.SetInvocationDB(invocationdb.NewInvocationDB(realEnv, dbHandle))
	realEnv.SetAuthenticator(&nullauth.NullAuthenticator{})

	hooks := make([]interfaces.Webhook, 0)
	appURL := configurator.GetAppBuildBuddyURL()
	if sc := configurator.GetIntegrationsSlackConfig(); sc != nil {
		if sc.WebhookURL != "" {
			hooks = append(hooks, slack.NewSlackWebhook(sc.WebhookURL, appURL))
		}
	}
	if configurator.GetIntegrationsInvocationUploadConfig().Enabled {
		hooks = append(hooks, webhooks.NewInvocationUploadHook(realEnv))
	}
	realEnv.SetWebhooks(hooks)

	buildEventProxyClients := make([]pepb.PublishBuildEventClient, 0)
	for _, target := range configurator.GetBuildEventProxyHosts() {
		// NB: This can block for up to a second on connecting. This would be a
		// great place to have our health checker and mark these as optional.
		buildEventProxyClients = append(buildEventProxyClients, build_event_proxy.NewBuildEventProxyClient(realEnv, target))
		log.Printf("Proxy: forwarding build events to: %s", target)
	}
	realEnv.SetBuildEventProxyClients(buildEventProxyClients)
	realEnv.SetBuildEventHandler(build_event_handler.NewBuildEventHandler(realEnv))

	// If configured, enable the cache.
	var cache interfaces.Cache
	if configurator.GetCacheInMemory() {
		maxSizeBytes := configurator.GetCacheMaxSizeBytes()
		if maxSizeBytes == 0 {
			log.Fatalf("Cache size must be greater than 0 if in_memory cache is enabled!")
		}
		c, err := memory_cache.NewMemoryCache(maxSizeBytes)
		if err != nil {
			log.Fatalf("Error configuring in-memory cache: %s", err)
		}
		cache = c
	} else if configurator.GetCacheDiskConfig() != nil {
		diskConfig := configurator.GetCacheDiskConfig()
		c, err := disk_cache.NewDiskCache(realEnv, diskConfig, configurator.GetCacheMaxSizeBytes())
		if err != nil {
			log.Fatalf("Error configuring cache: %s", err)
		}
		cache = c
	}
	if cache != nil {
		realEnv.SetCache(cache)
		log.Printf("Cache: BuildBuddy cache API enabled!")
	}

	realEnv.SetSplashPrinter(&splash.Printer{})

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

func StartBuildEventServicesOrDie(env environment.Env, grpcServer *grpc.Server) {
	// Register to handle build event protocol messages.
	buildEventServer, err := build_event_server.NewBuildEventProtocolServer(env)
	if err != nil {
		log.Fatalf("Error initializing BuildEventProtocolServer: %s", err)
	}
	pepb.RegisterPublishBuildEventServer(grpcServer, buildEventServer)

	enableCache := env.GetCache() != nil
	// OPTIONAL CACHE API -- only enable if configured.
	if enableCache {
		// Register to handle content addressable storage (CAS) messages.
		casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
		if err != nil {
			log.Fatalf("Error initializing ContentAddressableStorageServer: %s", err)
		}
		repb.RegisterContentAddressableStorageServer(grpcServer, casServer)

		// Register to handle bytestream (upload and download) messages.
		byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
		if err != nil {
			log.Fatalf("Error initializing ByteStreamServer: %s", err)
		}
		bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)

		// Register to handle action cache (upload and download) messages.
		actionCacheServer, err := action_cache_server.NewActionCacheServer(env)
		if err != nil {
			log.Fatalf("Error initializing ActionCacheServer: %s", err)
		}
		repb.RegisterActionCacheServer(grpcServer, actionCacheServer)

		pushServer := push_server.NewPushServer(env)
		rapb.RegisterPushServer(grpcServer, pushServer)

		fetchServer, err := fetch_server.NewFetchServer(env)
		if err != nil {
			log.Fatalf("Error initializing FetchServer: %s", err)
		}
		rapb.RegisterFetchServer(grpcServer, fetchServer)

	}
	enableRemoteExec := false
	if rexec := env.GetRemoteExecutionService(); rexec != nil {
		enableRemoteExec = true
		repb.RegisterExecutionServer(grpcServer, rexec)
	}
	if scheduler := env.GetSchedulerService(); scheduler != nil {
		scpb.RegisterSchedulerServer(grpcServer, scheduler)
	}
	// Register to handle GetCapabilities messages, which tell the client
	// that this server supports CAS functionality.
	capabilitiesServer := capabilities_server.NewCapabilitiesServer( /*supportCAS=*/ enableCache /*supportRemoteExec=*/, enableRemoteExec)
	repb.RegisterCapabilitiesServer(grpcServer, capabilitiesServer)
}

func StartGRPCServiceOrDie(env environment.Env, buildBuddyServer *buildbuddy_server.BuildBuddyServer, port *int, credentialOption grpc.ServerOption) *grpc.Server {
	// Initialize our gRPC server (and fail early if that doesn't happen).
	hostAndPort := fmt.Sprintf("%s:%d", *listen, *port)

	lis, err := net.Listen("tcp", hostAndPort)
	if err != nil {
		log.Fatalf("Failed to listen: %s", err)
	}

	grpcOptions := grpc_server.CommonGRPCServerOptions(env)
	if credentialOption != nil {
		grpcOptions = append(grpcOptions, credentialOption)
		log.Printf("gRPCS listening on http://%s", hostAndPort)
	} else {
		log.Printf("gRPC listening on http://%s", hostAndPort)
	}

	grpcServer := grpc.NewServer(grpcOptions...)

	// Support reflection so that tools like grpc-cli (aka stubby) can
	// enumerate our services and call them.
	reflection.Register(grpcServer)

	// Support prometheus grpc metrics.
	grpc_prometheus.Register(grpcServer)
	// Enable prometheus latency metrics too.
	grpc_prometheus.EnableHandlingTimeHistogram()

	// Start Build-Event-Protocol and Remote-Cache services.
	StartBuildEventServicesOrDie(env, grpcServer)
	bbspb.RegisterBuildBuddyServiceServer(grpcServer, buildBuddyServer)

	// Register API Server as a gRPC service.
	apiConfig := env.GetConfigurator().GetAPIConfig()
	if api := env.GetAPIService(); apiConfig != nil && apiConfig.EnableAPI && api != nil {
		apipb.RegisterApiServiceServer(grpcServer, api)
	}

	go func() {
		grpcServer.Serve(lis)
	}()
	env.GetHealthChecker().RegisterShutdownFunction(grpc_server.GRPCShutdownFunc(grpcServer))
	return grpcServer
}

func StartAndRunServices(env environment.Env) {
	if err := rlimit.MaxRLimit(); err != nil {
		log.Printf("Error raising open files limit: %s", err)
	}

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

	sslService, err := ssl.NewSSLService(env)
	if err != nil {
		log.Fatalf("Error configuring SSL: %s", err)
	}

	// Register to handle BuildBuddy API messages (over gRPC)
	buildBuddyServer, err := buildbuddy_server.NewBuildBuddyServer(env, sslService)
	if err != nil {
		log.Fatalf("Error initializing BuildBuddyServer: %s", err)
	}

	// Generate HTTP (protolet) handlers for the BuildBuddy API, so it
	// can be called over HTTP(s).
	buildBuddyProtoHandlers, err := protolet.GenerateHTTPHandlers(buildBuddyServer)
	if err != nil {
		log.Fatalf("Error initializing RPC over HTTP handlers for BuildBuddy server: %s", err)
	}

	monitoring.StartMonitoringHandler(fmt.Sprintf("%s:%d", *listen, *monitoringPort))

	grpcServer := StartGRPCServiceOrDie(env, buildBuddyServer, GRPCPort, nil)

	if sslService.IsEnabled() {
		creds, err := sslService.GetGRPCSTLSCreds()
		if err != nil {
			log.Fatalf("Error getting SSL creds: %s", err)
		}

		StartGRPCServiceOrDie(env, buildBuddyServer, gRPCSPort, grpc.Creds(creds))
	}

	mux := env.GetMux()
	// Register all of our HTTP handlers on the default mux.
	mux.Handle("/", httpfilters.WrapExternalHandler(env, staticFileServer))
	mux.Handle("/app/", httpfilters.WrapExternalHandler(env, http.StripPrefix("/app", afs)))
	mux.Handle("/rpc/BuildBuddyService/", httpfilters.WrapAuthenticatedExternalProtoletHandler(env, "/rpc/BuildBuddyService/", buildBuddyProtoHandlers))
	mux.Handle("/file/download", httpfilters.WrapAuthenticatedExternalHandler(env, buildBuddyServer))
	mux.Handle("/healthz", env.GetHealthChecker().LivenessHandler())
	mux.Handle("/readyz", env.GetHealthChecker().ReadinessHandler())

	if auth := env.GetAuthenticator(); auth != nil {
		mux.Handle("/login/", httpfilters.SetSecurityHeaders(http.HandlerFunc(auth.Login)))
		mux.Handle("/auth/", httpfilters.SetSecurityHeaders(http.HandlerFunc(auth.Auth)))
		mux.Handle("/logout/", httpfilters.SetSecurityHeaders(http.HandlerFunc(auth.Logout)))
	}

	if githubConfig := env.GetConfigurator().GetGithubConfig(); githubConfig != nil {
		githubClient := github.NewGithubClient(env, "")
		mux.Handle("/auth/github/link/", httpfilters.WrapAuthenticatedExternalHandler(env, http.HandlerFunc(githubClient.Link)))
	}

	// Register API as an HTTP service.
	apiConfig := env.GetConfigurator().GetAPIConfig()
	if api := env.GetAPIService(); apiConfig != nil && apiConfig.EnableAPI && api != nil {
		apiProtoHandlers, err := protolet.GenerateHTTPHandlers(api)
		if err != nil {
			log.Fatalf("Error initializing RPC over HTTP handlers for API: %s", err)
		}
		mux.Handle("/api/v1/", httpfilters.WrapAuthenticatedExternalProtoletHandler(env, "/api/v1/", apiProtoHandlers))
		// Protolet doesn't currently support streaming RPCs, so we'll register a regular old http handler.
		mux.Handle("/api/v1/GetFile", httpfilters.WrapAuthenticatedExternalHandler(env, api))
	}

	if wfs := env.GetWorkflowService(); wfs != nil {
		mux.Handle("/webhooks/workflow/", httpfilters.WrapExternalHandler(env, wfs))
	}

	handler := http.Handler(mux)
	if env.GetConfigurator().GetGRPCOverHTTPPortEnabled() {
		handler = httpfilters.ServeGRPCOverHTTPPort(grpcServer, mux)
	}

	if sp := env.GetSplashPrinter(); sp != nil {
		sp.PrintSplashScreen(*port, *GRPCPort)
	}

	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", *listen, *port),
		Handler: handler,
	}

	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		err := server.Shutdown(ctx)
		return err
	})

	if sslService.IsEnabled() {
		tlsConfig, sslHandler := sslService.ConfigureTLS(handler)
		if err != nil {
			log.Fatalf("Error configuring TLS: %s", err)
		}
		sslServer := &http.Server{
			Addr:      fmt.Sprintf("%s:%d", *listen, *sslPort),
			Handler:   handler,
			TLSConfig: tlsConfig,
		}
		go func() {
			sslServer.ListenAndServeTLS("", "")
		}()
		go func() {
			http.ListenAndServe(fmt.Sprintf("%s:%d", *listen, *port), httpfilters.RedirectIfNotForwardedHTTPS(env, sslHandler))
		}()
	} else {
		// If no SSL is enabled, we'll just serve things as-is.
		go func() {
			server.ListenAndServe()
		}()
	}
	env.GetHealthChecker().WaitForGracefulShutdown()
}
