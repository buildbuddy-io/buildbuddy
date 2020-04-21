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
	listen   = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port     = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	gRPCPort = flag.Int("grpc_port", 1985, "The port to listen for gRPC traffic on")

	staticDirectory = flag.String("static_directory", "/static", "the directory containing static files to host")
	appDirectory    = flag.String("app_directory", "/app", "the directory containing app binary files to host")
)

func StartAndRunServices(env environment.Env) {
	staticFileServer, err := static.NewStaticFileServer(*staticDirectory, []string{"/invocation/", "/history/", "/docs/"})
	if err != nil {
		log.Fatalf("Error initializing static file server: %s", err)
	}

	afs, err := static.NewStaticFileServer(*appDirectory, []string{})
	if err != nil {
		log.Fatalf("Error initializing app server: %s", err)
	}

	// Initialize our gRPC server (and fail early if that doesn't happen).
	gRPCHostAndPort := fmt.Sprintf("%s:%d", *listen, *gRPCPort)
	log.Printf("gRPC listening on http://%s\n", gRPCHostAndPort)

	lis, err := net.Listen("tcp", gRPCHostAndPort)
	if err != nil {
		log.Fatalf("Failed to listen: %s", err)
	}
	grpcServer := grpc.NewServer(rpcfilters.GetUnaryInterceptor(env), rpcfilters.GetStreamInterceptor(env))

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

	// Support reflection so that tools like grpc-cli (aka stubby) can
	// enumerate our services and call them.
	reflection.Register(grpcServer)

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
	bbspb.RegisterBuildBuddyServiceServer(grpcServer, buildBuddyServer)

	// Register all of our HTTP handlers on the default mux.
	http.Handle("/", httpfilters.WrapExternalHandler(staticFileServer))
	http.Handle("/app/", httpfilters.WrapExternalHandler(http.StripPrefix("/app", afs)))
	http.Handle("/rpc/BuildBuddyService/", httpfilters.WrapAuthenticatedExternalHandler(env,
		http.StripPrefix("/rpc/BuildBuddyService/", protoHandler)))
	http.Handle("/file/download", httpfilters.WrapExternalHandler(buildBuddyServer))
	http.Handle("/healthz", env.GetHealthChecker())

	if auth := env.GetAuthenticator(); auth != nil {
		http.Handle("/login/", httpfilters.RedirectHTTPS(http.HandlerFunc(auth.Login)))
		http.Handle("/auth/", httpfilters.RedirectHTTPS(http.HandlerFunc(auth.Auth)))
		http.Handle("/logout/", httpfilters.RedirectHTTPS(http.HandlerFunc(auth.Logout)))
	}

	hostAndPort := fmt.Sprintf("%s:%d", *listen, *port)
	log.Printf("HTTP listening on http://%s\n", hostAndPort)

	if sp := env.GetSplashPrinter(); sp != nil {
		sp.PrintSplashScreen(*port, *gRPCPort)
	}

	// Run the HTTP server in a goroutine because we still want to start a gRPC
	// server below.
	go func() {
		log.Fatal(http.ListenAndServe(hostAndPort, nil))
	}()
	go func() {
		log.Fatal(grpcServer.Serve(lis))
	}()
	select {}
}
