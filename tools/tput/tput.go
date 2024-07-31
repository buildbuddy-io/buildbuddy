package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/docker/go-units"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	cacheTarget    = flag.String("cache_target", "grpc://localhost:1985", "Cache target to connect to.")
	apiKey         = flag.String("api_key", "", "An optional API key to use when reading / writing data.")
	timeout        = flag.Duration("timeout", 60*time.Second, "Use this timeout as the context timeout for rpc calls")
	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	monitoringPort = flag.Int("monitoring_port", 0, "The port to listen for monitoring traffic on")
	size           = flag.Int("size", 256_000_000, "Blob size.")
)

func main() {
	flag.Parse()
	if err := log.Configure(); err != nil {
		log.Fatalf("Failed to configure logging: %s", err)
	}

	ctx := context.Background()

	env := real_environment.NewBatchEnv()

	if *monitoringPort > 0 {
		monitoring.StartMonitoringHandler(env, fmt.Sprintf("%s:%d", *listen, *monitoringPort))
	}

	conn, err := grpc_client.DialSimpleWithoutPooling(*cacheTarget, grpc.WithBlock(), grpc.WithTimeout(*timeout))
	if err != nil {
		log.Fatalf("Unable to connect to target '%s': %s", *cacheTarget, err)
	}
	log.Printf("Connected to target: %q", *cacheTarget)

	bsClient := bspb.NewByteStreamClient(conn)
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	} else {
		log.Fatalf("--api_key is required")
	}

	rng := digest.RandomGenerator(time.Now().Unix())

	var rns []*digest.ResourceName
	for i := 0; i < 5; i++ {
		d, r, err := rng.RandomDigestReader(int64(*size))
		if err != nil {
			log.Fatalf("could not generate random file: %s", err)
		}

		rn := digest.NewResourceName(d, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256)
		rns = append(rns, rn)

		start := time.Now()
		_, err = cachetools.UploadFromReader(ctx, bsClient, rn, r)
		if err != nil {
			log.Fatalf("Could not upload data: %s", err)
		}
		secs := time.Since(start).Seconds()
		tput := float64(*size) / secs
		log.Infof("Write throughput: %s/s", units.HumanSize(tput))
	}

	for _, rn := range rns {
		start := time.Now()
		err := cachetools.GetBlob(ctx, bsClient, rn, io.Discard)
		if err != nil {
			log.Fatalf("Could not download data: %s", err)
		}
		secs := time.Since(start).Seconds()
		tput := float64(*size) / secs
		log.Infof("Read throughput: %s/s", units.HumanSize(tput))
	}
}
