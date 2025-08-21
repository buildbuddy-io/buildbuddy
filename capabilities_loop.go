package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	remoteexecution "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func main() {
	// Create gRPC connection
	conn, err := grpc.NewClient("localhost:1985",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create Capabilities client
	client := remoteexecution.NewCapabilitiesClient(conn)

	log.Println("Starting GetCapabilities loop...")

	req := &remoteexecution.GetCapabilitiesRequest{}
	for i := 1; ; i++ {
		// Create context with trace header
		ctx := context.Background()
		md := metadata.New(map[string]string{
			"x-buildbuddy-trace": "force",
		})
		ctx = metadata.NewOutgoingContext(ctx, md)

		// Variable to capture response headers and trailers
		var responseHeaders metadata.MD
		var responseTrailers metadata.MD
		var p peer.Peer

		// Make the call
		start := time.Now()
		resp, err := client.GetCapabilities(ctx, req,
			grpc.Header(&responseHeaders),
			grpc.Trailer(&responseTrailers),
			grpc.Peer(&p))
		duration := time.Since(start)

		if err != nil {
			grpcStatus := status.Convert(err)
			log.Printf("Call %d FAILED after %v: code=%v, message=%s",
				i, duration, grpcStatus.Code(), grpcStatus.Message())
		} else {
			log.Printf("Call %d SUCCESS in %v - API: %d.%d, Peer: %v",
				i, duration, resp.HighApiVersion.Major, resp.HighApiVersion.Minor, p.Addr)
		}

		time.Sleep(100 * time.Millisecond)
	}
}
