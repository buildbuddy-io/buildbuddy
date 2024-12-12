// Fetch executions for an invocation, writing the results to stdout as a
// JSON-serialized GetExecutionResponse proto.
//
// Example usage: to replay all executions for an invocation, run the following
// command (replace ARGS with the appropriate args for replay_action):
//
// bazel run -- enterprise/tools/get_executions --api_key=$API_KEY --invocation_id=$IID \
//   | jq -r '"--execution_id=" + .execution[].executionId' \
//   | xargs bazel run -- enterprise/tools/replay_action ARGS

package main

import (
	"context"
	"fmt"
	"strings"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/metadata"
)

var (
	target             = flag.String("target", "remote.buildbuddy.io", "BuildBuddy gRPC target")
	apiKey             = flag.String("api_key", "", "BuildBuddy API key for the org that owns the invocation")
	invocationID       = flag.String("invocation_id", "", "Invocation ID to fetch executions for")
	remoteInstanceName = flag.String("remote_instance_name", "", "")
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	ctx := context.Background()
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}
	conn, err := grpc_client.DialSimpleWithoutPooling(*target)
	if err != nil {
		return fmt.Errorf("dial %s: %w", *target, err)
	}
	defer conn.Close()
	client := bbspb.NewBuildBuddyServiceClient(conn)
	rsp, err := client.GetExecution(ctx, &espb.GetExecutionRequest{
		ExecutionLookup: &espb.ExecutionLookup{
			InvocationId: *invocationID,
		},
	})
	if err != nil {
		return err
	}
	bsClient := bspb.NewByteStreamClient(conn)

	for _, e := range rsp.GetExecution() {
		ad := e.GetActionDigest()
		rn := digest.NewResourceName(ad, *remoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_BLAKE3)
		action := &repb.Action{}
		err := cachetools.GetBlobAsProto(ctx, bsClient, rn, action)
		if err != nil {
			log.Fatalf("could not fetch action for execution: %s", err)
		}

		cmd := &repb.Command{}
		crn := digest.NewResourceName(action.GetCommandDigest(), *remoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_BLAKE3)
		err = cachetools.GetBlobAsProto(ctx, bsClient, crn, cmd)
		if err != nil {
			log.Fatalf("could not fetch command for execution: %s", err)
		}

		match := false
		for _, op := range cmd.GetOutputPaths() {
			if strings.Contains(op, "bazel-out/platform_linux_x86_64-fastbuild-ST-9e98d01b8f6c/bin/external/com_github_klauspost_compress/zstd/zstd.a") {
				match = true
			}
		}
		if match {
			log.Warningf("execution id: %s", e.GetExecutionId())
			for _, op := range cmd.GetOutputPaths() {
				log.Infof("output path: %s", op)
			}
		}
	}

	//b, err := protojson.Marshal(rsp)
	//if err != nil {
	//	return fmt.Errorf("marshal response: %w", err)
	//}
	//if _, err := os.Stdout.Write(append(b, '\n')); err != nil {
	//	return err
	//}
	return nil
}
