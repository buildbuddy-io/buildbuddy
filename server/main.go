package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/tryflame/buildbuddy/server/blobstore"
	"github.com/tryflame/buildbuddy/server/build_event_handler"
	"github.com/tryflame/buildbuddy/server/build_event_server"
	"github.com/tryflame/buildbuddy/server/buildbuddy_server"
	"github.com/tryflame/buildbuddy/server/config"
	"github.com/tryflame/buildbuddy/server/database"
	"github.com/tryflame/buildbuddy/server/static"
	"google.golang.org/grpc"

	bpb "proto"
	bbspb "proto/buildbuddy_service"
)

var (
	listen     = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port       = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	gRPCPort   = flag.Int("grpc_port", 1985, "The port to listen for gRPC traffic on")
	configFile = flag.String("config_file", "config/buildbuddy.local.yaml", "The path to a buildbuddy config file")

	staticDirectory = flag.String("static_directory", "/static", "the directory containing static files to host")
	appDirectory    = flag.String("app_directory", "/app", "the directory containing app binary files to host")
)

func redirectHTTPS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		protocol := r.Header.Get("X-Forwarded-Proto") // Set by load balancer
		if protocol == "http" {
			http.Redirect(w, r, "https://"+r.Host+r.URL.String(), http.StatusMovedPermanently)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	// Parse all flags, once and for all.
	flag.Parse()

	configurator, err := config.NewConfigurator(*configFile)
	if err != nil {
		log.Fatalf("Error loading config from file: %s", err)
	}

	staticFileServer, err := static.NewStaticFileServer(*staticDirectory, false, []string{"/invocation/"})
	if err != nil {
		log.Fatalf("Error initializing static file server: %s", err)
	}

	bs, err := blobstore.GetConfiguredBlobstore(configurator)
	if err != nil {
		log.Fatalf("Error configuring blobstore: %s", err)
	}
	db, err := database.GetConfiguredDatabase(configurator)
	if err != nil {
		log.Fatalf("Error configuring database: %s", err)
	}
	eventHandler := build_event_handler.NewBuildEventHandler(bs, db)

	afs, err := static.NewStaticFileServer(*appDirectory, true, []string{})
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
	buildEventServer, err := build_event_server.NewBuildEventProtocolServer(eventHandler)
	if err != nil {
		log.Fatalf("Error initializing BuildEventProtocolServer: %s", err)
	}
	bpb.RegisterPublishBuildEventServer(grpcServer, buildEventServer)
	buildBuddyServer, err := buildbuddy_server.NewBuildBuddyServer(eventHandler)
	if err != nil {
		log.Fatalf("Error initializing BuildBuddyServer: %s", err)
	}
	bbspb.RegisterBuildBuddyServiceServer(grpcServer, buildBuddyServer)

	http.Handle("/", redirectHTTPS(staticFileServer))
	http.Handle("/app/", redirectHTTPS(afs))
	http.Handle("/rpc/BuildBuddyService/GetInvocation", redirectHTTPS(buildBuddyServer.GetInvocationHandlerFunc()))

	hostAndPort := fmt.Sprintf("%s:%d", *listen, *port)
	log.Printf("HTTP listening on http://%s\n", hostAndPort)

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
