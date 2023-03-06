package main

import (
	"context"
	"log"
	"time"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"go.opentelemetry.io/otel/trace"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

type daemon struct {
	lastFinishedAt   time.Time
	sleepDuration    time.Duration
	c                *cache
	tracer           trace.Tracer
	serviceClient    bbspb.BuildBuddyServiceClient
	byteStreamClient bspb.ByteStreamClient
}

// NewDaemon produce daemon struct that can be executed as a long-lived process
func NewDaemon(
	buildbuddyURL string,
	c *cache,
	startingPoint time.Time,
	tracer trace.Tracer,
	sleepDuration int,
) *daemon {
	// Default to HoneycombMaxRetention on initial run
	// should be updated on subsequent runs
	lastFinishedAt := time.Now().Add(-1 * honeycombMaxRetention)

	conn, err := grpc_client.DialTarget(buildbuddyURL)
	if err != nil {
		log.Fatalf("error dialing bes backend: %v", err)
	}
	serviceClient := bbspb.NewBuildBuddyServiceClient(conn)
	byteStreamClient := bspb.NewByteStreamClient(conn)

	return &daemon{
		lastFinishedAt:   lastFinishedAt,
		c:                c,
		sleepDuration:    time.Duration(sleepDuration) * time.Minute,
		tracer:           tracer,
		serviceClient:    serviceClient,
		byteStreamClient: byteStreamClient,
	}
}

// Exec execute the daemon as a long-lived process
func (d *daemon) Exec(ctx context.Context) {
	// TODO: implement graceful shutdown when SIGTERM/SIGKILL
	for {
		d.exportInvocations(ctx)

		log.Printf("sleeping for %s", d.sleepDuration)
		time.Sleep(d.sleepDuration)
	}
}
