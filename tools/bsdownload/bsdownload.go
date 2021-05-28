package main

import (
	"context"
	"flag"
	"io"
	"time"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	server     = flag.String("server", "", "gRPC server")
	digestHash = flag.String("digest_hash", "", "digest hash")
	digestSize = flag.Int64("digest_size", 0, "Number of bytes to download")
)

func main() {
	flag.Parse()

	ctx := context.Background()

	clientConn, err := grpc_client.DialTargetWithOptions(*server, true, grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
	if err != nil {
		log.Fatalf("Could not connect to server %q: %s", *server, err)
	}

	byteStreamClient := bspb.NewByteStreamClient(clientConn)

	for i := 0; i < 3; i++ {
		start := time.Now()
		log.Infof("Starting download")
		nd := digest.NewInstanceNameDigest(&repb.Digest{
			Hash:      *digestHash, // not used by server
			SizeBytes: *digestSize,
		}, "")
		err = cachetools.GetBlob(ctx, byteStreamClient, nd, io.Discard)
		log.Infof("Download done in %s: %v", time.Now().Sub(start), err)
	}
}
