package libmain

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/executiondb"
	"github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/slack"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_proxy"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_server"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/protolet"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_execution/execution_server"
	"github.com/buildbuddy-io/buildbuddy/server/splash"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/static"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // imported for side effects; DO NOT REMOVE.
	"google.golang.org/grpc/reflection"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	bpb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"

	httpfilters "github.com/buildbuddy-io/buildbuddy/server/http/filters"
	rpcfilters "github.com/buildbuddy-io/buildbuddy/server/rpc/filters"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	listen    = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port      = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	sslPort   = flag.Int("ssl_port", 8081, "The port to listen for HTTPS traffic on")
	gRPCPort  = flag.Int("grpc_port", 1985, "The port to listen for gRPC traffic on")
	gRPCSPort = flag.Int("grpcs_port", 1986, "The port to listen for gRPCS traffic on")

	staticDirectory = flag.String("static_directory", "/static", "the directory containing static files to host")
	appDirectory    = flag.String("app_directory", "/app", "the directory containing app binary files to host")
)

// Normally this code would live in main.go -- we put it here for now because
// the environments used by the open-core version and the enterprise version are
// not substantially different enough yet to warrant the extra complexity of
// always updating both main files.
func GetConfiguredEnvironmentOrDie(configurator *config.Configurator, healthChecker *healthcheck.HealthChecker) *real_environment.RealEnv {
	bs, err := blobstore.GetConfiguredBlobstore(configurator)
	if err != nil {
		log.Fatalf("Error configuring blobstore: %s", err)
	}
	dbHandle, err := db.GetConfiguredDatabase(configurator)
	if err != nil {
		log.Fatalf("Error configuring database: %s", err)
	}

	realEnv := real_environment.NewRealEnv(configurator, healthChecker)
	realEnv.SetDBHandle(dbHandle)
	realEnv.SetBlobstore(bs)
	realEnv.SetInvocationDB(invocationdb.NewInvocationDB(realEnv, dbHandle))
	realEnv.SetAuthenticator(&nullauth.NullAuthenticator{})
	realEnv.SetExecutionDB(executiondb.NewExecutionDB(dbHandle))

	webhooks := make([]interfaces.Webhook, 0)
	appURL := configurator.GetAppBuildBuddyURL()
	if sc := configurator.GetIntegrationsSlackConfig(); sc != nil {
		if sc.WebhookURL != "" {
			webhooks = append(webhooks, slack.NewSlackWebhook(sc.WebhookURL, appURL))
		}
	}
	realEnv.SetWebhooks(webhooks)

	buildEventProxyClients := make([]*build_event_proxy.BuildEventProxyClient, 0)
	for _, target := range configurator.GetBuildEventProxyHosts() {
		// NB: This can block for up to a second on connecting. This would be a
		// great place to have our health checker and mark these as optional.
		buildEventProxyClients = append(buildEventProxyClients, build_event_proxy.NewBuildEventProxyClient(target))
		log.Printf("Proxy: forwarding build events to: %s", target)
	}
	realEnv.SetBuildEventProxyClients(buildEventProxyClients)

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
		c, err := disk_cache.NewDiskCache(diskConfig.RootDirectory, configurator.GetCacheMaxSizeBytes())
		if err != nil {
			log.Fatalf("Error configuring cache: %s", err)
		}
		cache = c
	}
	if cache != nil {
		cache.Start()
		realEnv.SetCache(cache)
		log.Printf("Cache: BuildBuddy cache API enabled!")
	}

	realEnv.SetSplashPrinter(&splash.Printer{})

	if remoteExecConfig := configurator.GetRemoteExecutionConfig(); remoteExecConfig != nil {
		for _, remoteExecTarget := range remoteExecConfig.RemoteExecutionTargets {
			propertyString := execution_server.HashProperties(remoteExecTarget.Properties)
			conn, err := grpc_client.DialTarget(remoteExecTarget.Target)
			if err != nil {
				log.Fatalf("Error connecting to remote execution backend: %s", err)
			}
			client := repb.NewExecutionClient(conn)
			maxExecutionDuration := time.Second * time.Duration(remoteExecTarget.MaxExecutionTimeoutSeconds)
			if err := realEnv.AddExecutionClient(propertyString, client, maxExecutionDuration); err != nil {
				log.Fatal(err)
			}
		}
	}
	return realEnv
}

func StartBuildEventServicesOrDie(env environment.Env, grpcServer *grpc.Server) {
	// Register to handle build event protocol messages.
	buildEventServer, err := build_event_server.NewBuildEventProtocolServer(env)
	if err != nil {
		log.Fatalf("Error initializing BuildEventProtocolServer: %s", err)
	}
	bpb.RegisterPublishBuildEventServer(grpcServer, buildEventServer)

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

	}
	enableRemoteExec := false

	// If there is a remote client registered into the environment, we'll
	// register our own server which calls that client and maintains state
	// in the database.
	if remoteExecConfig := env.GetConfigurator().GetRemoteExecutionConfig(); remoteExecConfig != nil {
		// Make sure capabilities server reflect that we're running
		// remote execution.
		enableRemoteExec = true
		log.Printf("Enabling remote execution!")
		executionServer, err := execution_server.NewExecutionServer(env)
		if err != nil {
			log.Fatalf("Error initializing ExecutionServer: %s", err)
		}
		repb.RegisterExecutionServer(grpcServer, executionServer)
	}
	// Register to handle GetCapabilities messages, which tell the client
	// that this server supports CAS functionality.
	capabilitiesServer := capabilities_server.NewCapabilitiesServer( /*supportCAS=*/ enableCache /*supportRemoteExec=*/, enableRemoteExec)
	repb.RegisterCapabilitiesServer(grpcServer, capabilitiesServer)
}

func StartGRPCServiceOrDie(env environment.Env, buildBuddyServer *buildbuddy_server.BuildBuddyServer, port *int, credentialOption grpc.ServerOption) {
	// Initialize our gRPC server (and fail early if that doesn't happen).
	hostAndPort := fmt.Sprintf("%s:%d", *listen, *port)

	lis, err := net.Listen("tcp", hostAndPort)
	if err != nil {
		log.Fatalf("Failed to listen: %s", err)
	}
	grpcOptions := []grpc.ServerOption{
		rpcfilters.GetUnaryInterceptor(env),
		rpcfilters.GetStreamInterceptor(env),
	}

	if credentialOption != nil {
		grpcOptions = append(grpcOptions, credentialOption)
		log.Printf("gRPCS listening on http://%s\n", hostAndPort)
	} else {
		log.Printf("gRPC listening on http://%s\n", hostAndPort)
	}

	grpcServer := grpc.NewServer(grpcOptions...)

	// Support reflection so that tools like grpc-cli (aka stubby) can
	// enumerate our services and call them.
	reflection.Register(grpcServer)

	// Start Build-Event-Protocol and Remote-Cache services.
	StartBuildEventServicesOrDie(env, grpcServer)
	bbspb.RegisterBuildBuddyServiceServer(grpcServer, buildBuddyServer)

	// Register API Server as a gRPC service.
	apiConfig := env.GetConfigurator().GetAPIConfig()
	if api := env.GetAPIService(); apiConfig != nil && apiConfig.EnableAPI && api != nil {
		apipb.RegisterApiServiceServer(grpcServer, api)
	}

	go func() {
		log.Fatal(grpcServer.Serve(lis))
	}()
}

func StartAndRunServices(env environment.Env) {
	staticFileServer, err := static.NewStaticFileServer(env, *staticDirectory, []string{"/invocation/", "/history/", "/docs/"})
	if err != nil {
		log.Fatalf("Error initializing static file server: %s", err)
	}

	afs, err := static.NewStaticFileServer(env, *appDirectory, []string{})
	if err != nil {
		log.Fatalf("Error initializing app server: %s", err)
	}

	// Register to handle BuildBuddy API messages (over gRPC)
	buildBuddyServer, err := buildbuddy_server.NewBuildBuddyServer(env)
	if err != nil {
		log.Fatalf("Error initializing BuildBuddyServer: %s", err)
	}

	// Generate HTTP (protolet) handlers for the BuildBuddy API, so it
	// can be called over HTTP(s).
	buildBuddyProtoHandler, err := protolet.GenerateHTTPHandlers(buildBuddyServer)
	if err != nil {
		log.Fatalf("Error initializing RPC over HTTP handlers for BuildBuddy server: %s", err)
	}

	StartGRPCServiceOrDie(env, buildBuddyServer, gRPCPort, nil)

	if ssl.IsEnabled(env) {
		creds, err := ssl.GetGRPCSTLSCreds(env)
		if err != nil {
			log.Fatal(err)
		}

		StartGRPCServiceOrDie(env, buildBuddyServer, gRPCSPort, grpc.Creds(creds))
	}

	mux := http.NewServeMux()
	// Register all of our HTTP handlers on the default mux.
	mux.Handle("/", httpfilters.WrapExternalHandler(staticFileServer))
	mux.Handle("/app/", httpfilters.WrapExternalHandler(http.StripPrefix("/app", afs)))
	mux.Handle("/rpc/BuildBuddyService/", httpfilters.WrapAuthenticatedExternalHandler(env,
		http.StripPrefix("/rpc/BuildBuddyService/", buildBuddyProtoHandler)))
	mux.Handle("/file/download", httpfilters.WrapExternalHandler(buildBuddyServer))
	mux.Handle("/healthz", env.GetHealthChecker())

	if auth := env.GetAuthenticator(); auth != nil {
		mux.Handle("/login/", httpfilters.RedirectHTTPS(http.HandlerFunc(auth.Login)))
		mux.Handle("/auth/", httpfilters.RedirectHTTPS(http.HandlerFunc(auth.Auth)))
		mux.Handle("/logout/", httpfilters.RedirectHTTPS(http.HandlerFunc(auth.Logout)))
	}

	// Register API as an HTTP service.
	apiConfig := env.GetConfigurator().GetAPIConfig()
	if api := env.GetAPIService(); apiConfig != nil && apiConfig.EnableAPI && api != nil {
		apiProtoHandler, err := protolet.GenerateHTTPHandlers(api)
		if err != nil {
			log.Fatalf("Error initializing RPC over HTTP handlers for API: %s", err)
		}
		mux.Handle("/api/v1/", httpfilters.WrapAuthenticatedExternalHandler(env,
			http.StripPrefix("/api/v1/", apiProtoHandler)))
		// Protolet doesn't currently support streaming RPCs, so we'll register a regular old http handler.
		mux.Handle("/api/v1/GetFile", httpfilters.WrapAuthenticatedExternalHandler(env, api))
	}

	if sp := env.GetSplashPrinter(); sp != nil {
		sp.PrintSplashScreen(*port, *gRPCPort)
	}

	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", *listen, *port),
		Handler: mux,
	}

	if ssl.IsEnabled(env) {
		tlsConfig, handler, err := ssl.ConfigureTLS(env, mux)
		if err != nil {
			log.Fatal(err)
		}
		sslServer := &http.Server{
			Addr:      fmt.Sprintf("%s:%d", *listen, *sslPort),
			Handler:   mux,
			TLSConfig: tlsConfig,
		}
		go func() {
			log.Fatal(sslServer.ListenAndServeTLS("", ""))
		}()
		go func() {
			log.Fatal(http.ListenAndServe(fmt.Sprintf("%s:%d", *listen, *port), handler))
		}()
	} else {
		// If no SSL is enabled, we'll just serve things as-is.
		go func() {
			log.Fatal(server.ListenAndServe())
		}()
	}
	select {}
}
