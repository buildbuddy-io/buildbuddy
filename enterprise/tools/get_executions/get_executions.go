// Fetch executions for an invocation, writing the results to stdout as a
// JSON-serialized GetExecutionResponse proto.
//
// Example usage: to replay all executions for an invocation, run:
//
// bazel run -- enterprise/tools/get_executions --api_key=$API_KEY --invocation_id=$IID \
//   | jq -r '"--execution_id=" + .execution[].executionId' \
//   | xargs bazel run -- enterprise/tools/replay_action ...

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/mattn/go-isatty"
	"google.golang.org/protobuf/encoding/protojson"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
)

var (
	target       = flag.String("target", "remote.buildbuddy.io", "BuildBuddy gRPC target")
	apiKey       = flag.String("api_key", "", "BuildBuddy API key")
	invocationID = flag.String("invocation_id", "", "Invocation ID to fetch executions for")
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	ctx := context.Background()
	conn, err := grpc_client.DialSimpleWithoutPooling("remote.buildbuddy.io")
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
	b, err := protojson.Marshal(rsp)
	if err != nil {
		return fmt.Errorf("marshal response: %w", err)
	}
	if _, err := os.Stdout.Write(b); err != nil {
		return err
	}
	if isatty.IsTerminal(os.Stdout.Fd()) {
		if _, err := os.Stdout.Write([]byte{'\n'}); err != nil {
			return err
		}
	}
	return nil
}
