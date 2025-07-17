package main

import (
	"context"
	"flag"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"
	"time"
)

var (
	apiKey = flag.String("apiKey", "", "")
)

func main() {
	for range 15 {
		if err := dial(); err != nil {
			log.Fatalf("%s", err)
		}
	}
}

func dial() error {
	ctx := context.Background()
	start := time.Now()
	flag.Parse()
	conn, err := grpc_client.DialSimple("grpcs://proxy.metal.buildbuddy.dev")
	if err != nil {
		log.Fatalf("%s", status.WrapError(err, "error dialing bes_backend"))
	}
	log.Warningf("Mag: Dial took %s", time.Since(start))
	defer conn.Close()
	start = time.Now()
	besClient := pepb.NewPublishBuildEventClient(conn)
	log.Warningf("Mag: Create build event client took %s", time.Since(start))
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, *apiKey)
	}
	start = time.Now()
	_, err = besClient.PublishBuildToolEventStream(ctx)
	if err != nil {
		return status.WrapError(err, "error creating stream")
	}
	log.Warningf("Mag: Create stream took %s", time.Since(start))
	return nil
}
