package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/tryflame/buildbuddy/server/build_event_server"
	"github.com/tryflame/buildbuddy/server/buildbuddy_server"
	"github.com/tryflame/buildbuddy/server/static"
	"google.golang.org/grpc"

	bpb "proto"
	bbspb "proto/buildbuddy_service"
)

var (
	listen   = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port     = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	gRPCPort = flag.Int("grpc_port", 1985, "The port to listen for gRPC traffic on")

	staticDirectory = flag.String("static_directory", "/static-content", "the directory containing static files to host")
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

	sfs, err := static.NewStaticFileServer(*staticDirectory, false)
	if err != nil {
		log.Fatalf("Error initializing static file server: %v", err)
	}

	afs, err := static.NewStaticFileServer(*appDirectory, false)
	if err != nil {
		log.Fatalf("Error initializing app server: %v", err)
	}

	http.Handle("/static", redirectHTTPS(sfs))
	http.Handle("/", redirectHTTPS(afs))

	hostAndPort := fmt.Sprintf("%s:%d", *listen, *port)
	log.Printf("HTTP listening on http://%s\n", hostAndPort)

	// Initialize our gRPC server (and fail early if not).
	gRPCHostAndPort := fmt.Sprintf("%s:%d", *listen, *gRPCPort)
	log.Printf("gRPC listening on http://%s\n", gRPCHostAndPort)

	lis, err := net.Listen("tcp", gRPCHostAndPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	buildEventServer, err := build_event_server.NewBuildEventProtocolServer()
	if err != nil {
		log.Fatalf("Error initializing BuildEventProtocolServer: %v", err)
	}
	bpb.RegisterPublishBuildEventServer(grpcServer, buildEventServer)
	buildBuddyServer, err := buildbuddy_server.NewBuildBuddyServer()
	if err != nil {
		log.Fatalf("Error initializing BuildBuddyServer: %s", err)
	}
	bbspb.RegisterBuildBuddyServiceServer(grpcServer, buildBuddyServer)

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
