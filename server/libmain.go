package libmain

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_server"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/httpfilters"
	"github.com/buildbuddy-io/buildbuddy/server/protolet"
	"github.com/buildbuddy-io/buildbuddy/server/static"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	bpb "proto"
	bbspb "proto/buildbuddy_service"
)

var (
	listen   = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port     = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	gRPCPort = flag.Int("grpc_port", 1985, "The port to listen for gRPC traffic on")

	staticDirectory = flag.String("static_directory", "/static", "the directory containing static files to host")
	appDirectory    = flag.String("app_directory", "/app", "the directory containing app binary files to host")
)

func StartAndRunServices(env environment.Env) {
	staticFileServer, err := static.NewStaticFileServer(*staticDirectory, []string{"/auth/", "/invocation/", "/history/"})
	if err != nil {
		log.Fatalf("Error initializing static file server: %s", err)
	}

	afs, err := static.NewStaticFileServer(*appDirectory, []string{})
	if err != nil {
		log.Fatalf("Error initializing app server: %s", err)
	}

	// Initialize our gRPC server (and fail early if not).
	gRPCHostAndPort := fmt.Sprintf("%s:%d", *listen, *gRPCPort)
	log.Printf("gRPC listening on http://%s\n", gRPCHostAndPort)

	lis, err := net.Listen("tcp", gRPCHostAndPort)
	if err != nil {
		log.Fatalf("Failed to listen: %s", err)
	}
	grpcServer := grpc.NewServer()
	buildEventServer, err := build_event_server.NewBuildEventProtocolServer(env)
	if err != nil {
		log.Fatalf("Error initializing BuildEventProtocolServer: %s", err)
	}
	bpb.RegisterPublishBuildEventServer(grpcServer, buildEventServer)
	reflection.Register(grpcServer)
	buildBuddyServer, err := buildbuddy_server.NewBuildBuddyServer(env)
	if err != nil {
		log.Fatalf("Error initializing BuildBuddyServer: %s", err)
	}
	protoHandler, err := protolet.GenerateHTTPHandlers(buildBuddyServer)
	if err != nil {
		log.Fatalf("Error initializing RPC over HTTP handlers: %s", err)
	}
	bbspb.RegisterBuildBuddyServiceServer(grpcServer, buildBuddyServer)

	http.Handle("/", httpfilters.WrapExternalHandler(staticFileServer))
	http.Handle("/app/", httpfilters.WrapExternalHandler(http.StripPrefix("/app", afs)))
	http.Handle("/rpc/BuildBuddyService/", httpfilters.WrapExternalHandler(http.StripPrefix("/rpc/BuildBuddyService/", protoHandler)))
	http.Handle("/healthz", env.GetHealthChecker())

	hostAndPort := fmt.Sprintf("%s:%d", *listen, *port)
	log.Printf("HTTP listening on http://%s\n", hostAndPort)

	// Print out a nice welcome message with getting started instructions.
	log.Printf("")
	log.Printf("\u001b[37;1m\u001b[1mYour Buildbuddy server is running!\u001b[0m")
	log.Printf("")
	log.Printf("Add the following lines to your \u001b[37;1m\u001b[1m.bazelrc\u001b[0m to start sending build events to your local server:")
	log.Printf("    \u001b[32;1mbuild --bes_results_url=http://localhost:%d/invocation/\u001b[0m", *port)
	log.Printf("   	\u001b[32;1mbuild --bes_backend=grpc://localhost:%d\u001b[0m", *gRPCPort)
	log.Printf("")
	log.Printf("You can now view Buildbuddy in the browser.")
	log.Printf("    \u001b[34;1mhttp://localhost:%d/\u001b[0m", *port)

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
