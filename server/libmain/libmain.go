package libmain

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
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

	bundle "github.com/buildbuddy-io/buildbuddy"
	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	httpfilters "github.com/buildbuddy-io/buildbuddy/server/http/filters"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port           = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	sslPort        = flag.Int("ssl_port", 8081, "The port to listen for HTTPS traffic on")
	monitoringPort = flag.Int("monitoring_port", 9090, "The port to listen for monitoring traffic on")

	staticDirectory = flag.String("static_directory", "", "the directory containing static files to host")
	appDirectory    = flag.String("app_directory", "", "the directory containing app binary files to host")

	exitWhenReady = flag.Bool("exit_when_ready", false, "If set, the app will exit as soon as it becomes ready (useful for migrations)")

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
func GetConfiguredEnvironmentOrDie(healthChecker *healthcheck.HealthChecker) *real_environment.RealEnv {
	if err := log.Configure(); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1)
	}
	realEnv := real_environment.NewRealEnv(healthChecker)
	realEnv.SetMux(tracing.NewHttpServeMux(http.NewServeMux()))
	realEnv.SetAuthenticator(&nullauth.NullAuthenticator{})
	configureFilesystemsOrDie(realEnv)

	dbHandle, err := db.GetConfiguredDatabase(healthChecker)
	if err != nil {
		log.Fatalf("Error configuring database: %s", err)
	}
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

func registerGRPCServices(grpcServer *grpc.Server, env environment.Env) {
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

func StartAndRunServices(env environment.Env) {
	env.SetListenAddr(*listen)

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

	if err := ssl.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

	// Register to handle BuildBuddy API messages (over gRPC)
	if err := buildbuddy_server.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

	// Generate HTTP (protolet) handlers for the BuildBuddy API, so it
	// can be called over HTTP(s).
	protoletHandler, err := protolet.GenerateHTTPHandlers(env.GetBuildBuddyServer())
	if err != nil {
		log.Fatalf("Error initializing RPC over HTTP handlers for BuildBuddy server: %s", err)
	}

	monitoring.StartMonitoringHandler(fmt.Sprintf("%s:%d", *listen, *monitoringPort))

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
	if err := fetch_server.Register(env); err != nil {
		log.Fatalf("%v", err)
	}
	if err := capabilities_server.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

	if err := grpc_server.RegisterGRPCServer(env, registerGRPCServices); err != nil {
		log.Fatalf("%v", err)
	}
	if err := grpc_server.RegisterGRPCSServer(env, registerGRPCServices); err != nil {
		log.Fatalf("%v", err)
	}

	mux := env.GetMux()
	// Register all of our HTTP handlers on the default mux.
	mux.Handle("/", httpfilters.WrapExternalHandler(env, staticFileServer))
	for _, appRoute := range appRoutes {
		// this causes the muxer to handle redirects from e. g. /path -> /path/
		mux.Handle(appRoute, httpfilters.WrapExternalHandler(env, staticFileServer))
	}
	mux.Handle("/app/", httpfilters.WrapExternalHandler(env, http.StripPrefix("/app", afs)))
	mux.Handle("/rpc/BuildBuddyService/", httpfilters.WrapAuthenticatedExternalProtoletHandler(env, "/rpc/BuildBuddyService/", protoletHandler))
	mux.Handle("/file/download", httpfilters.WrapAuthenticatedExternalHandler(env, env.GetBuildBuddyServer()))
	mux.Handle("/healthz", env.GetHealthChecker().LivenessHandler())
	mux.Handle("/readyz", env.GetHealthChecker().ReadinessHandler())

	if auth := env.GetAuthenticator(); auth != nil {
		mux.Handle("/login/", httpfilters.SetSecurityHeaders(http.HandlerFunc(auth.Login)))
		mux.Handle("/auth/", httpfilters.SetSecurityHeaders(http.HandlerFunc(auth.Auth)))
		mux.Handle("/logout/", httpfilters.SetSecurityHeaders(http.HandlerFunc(auth.Logout)))
	}

	if err := github.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

	// Register API as an HTTP service.
	if api := env.GetAPIService(); api != nil {
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

	if sp := env.GetSplashPrinter(); sp != nil {
		sp.PrintSplashScreen(*port, grpc_server.GRPCPort())
	}

	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", *listen, *port),
		Handler: env.GetMux(),
	}

	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		err := server.Shutdown(ctx)
		return err
	})

	if env.GetSSLService().IsEnabled() {
		tlsConfig, sslHandler := env.GetSSLService().ConfigureTLS(server.Handler)
		if err != nil {
			log.Fatalf("Error configuring TLS: %s", err)
		}
		sslServer := &http.Server{
			Addr:      fmt.Sprintf("%s:%d", *listen, *sslPort),
			Handler:   server.Handler,
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

	if *exitWhenReady {
		env.GetHealthChecker().Shutdown()
		os.Exit(0)
	}
	env.GetHealthChecker().WaitForGracefulShutdown()
}
