package libmain

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_server"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/protolet"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/static"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // imported for side effects; DO NOT REMOVE.
	"google.golang.org/grpc/reflection"

	bbspb "proto/buildbuddy_service"
	bpb "proto/publish_build_event"
	repb "proto/remote_execution"

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

func StartBuildEventServices(env environment.Env, grpcServer *grpc.Server) {
	// Register to handle build event protocol messages.
	buildEventServer, err := build_event_server.NewBuildEventProtocolServer(env)
	if err != nil {
		log.Fatalf("Error initializing BuildEventProtocolServer: %s", err)
	}
	bpb.RegisterPublishBuildEventServer(grpcServer, buildEventServer)

	// OPTIONAL CACHE API -- only enable if configured.
	if env.GetCache() != nil {
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

		// Register to handle GetCapabilities messages, which tell the client
		// that this server supports CAS functionality.
		capabilitiesServer := capabilities_server.NewCapabilitiesServer( /*supportCAS=*/ true /*supportRemoteExec=*/, false)
		repb.RegisterCapabilitiesServer(grpcServer, capabilitiesServer)
	}
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
	StartBuildEventServices(env, grpcServer)
	bbspb.RegisterBuildBuddyServiceServer(grpcServer, buildBuddyServer)

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

	// Generate HTTP (protolet) handlers for the BuildBuddy API too, so it
	// can be called over HTTP(s).
	protoHandler, err := protolet.GenerateHTTPHandlers(buildBuddyServer)
	if err != nil {
		log.Fatalf("Error initializing RPC over HTTP handlers: %s", err)
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
		http.StripPrefix("/rpc/BuildBuddyService/", protoHandler)))
	mux.Handle("/file/download", httpfilters.WrapExternalHandler(buildBuddyServer))
	mux.Handle("/healthz", env.GetHealthChecker())

	if auth := env.GetAuthenticator(); auth != nil {
		mux.Handle("/login/", httpfilters.RedirectHTTPS(http.HandlerFunc(auth.Login)))
		mux.Handle("/auth/", httpfilters.RedirectHTTPS(http.HandlerFunc(auth.Auth)))
		mux.Handle("/logout/", httpfilters.RedirectHTTPS(http.HandlerFunc(auth.Logout)))
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
