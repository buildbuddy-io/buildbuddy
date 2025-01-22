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
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

var (
	target       = flag.String("target", "remote.buildbuddy.io", "BuildBuddy gRPC target")
	apiKey       = flag.String("api_key", "", "BuildBuddy API key for the org that owns the invocation")
	invocationID = flag.String("invocation_id", "", "Invocation ID to fetch executions for")

	inlineResponse = flag.Bool("inline_response", false, "Inline ExecuteResponse for each execution")

	executorHostnames = flag.Bool("executor_hostnames", false, "Replace executor host ID (worker field) with hostname")
	executorsGroupID  = flag.String("executors_group_id", "", "Group ID for fetching executor metadata")
	executorsAPIKey   = flag.String("executors_api_key", "", "BuildBuddy API key for fetching executor metadata. Must be an org admin key. Defaults to the value of -api_key.")
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	ctx := context.Background()

	executionsCtx := ctx
	if *apiKey != "" {
		executionsCtx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}
	conn, err := grpc_client.DialSimpleWithoutPooling(*target)
	if err != nil {
		return fmt.Errorf("dial %s: %w", *target, err)
	}
	defer conn.Close()
	client := bbspb.NewBuildBuddyServiceClient(conn)
	rsp, err := client.GetExecution(executionsCtx, &espb.GetExecutionRequest{
		ExecutionLookup: &espb.ExecutionLookup{
			InvocationId: *invocationID,
		},
		InlineExecuteResponse: *inlineResponse,
	})
	if err != nil {
		return err
	}

	if *executorHostnames {
		key := *executorsAPIKey
		if key == "" {
			key = *apiKey
		}
		if key == "" {
			return fmt.Errorf("either -executors_api_key or -api_key is required to fetch executor info")
		}
		executorsCtx := metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", key)
		if err := resolveWorkerHostnames(executorsCtx, client, rsp); err != nil {
			return fmt.Errorf("resolve worker hostnames: %w", err)
		}
	}

	b, err := protojson.Marshal(rsp)
	if err != nil {
		return fmt.Errorf("marshal response: %w", err)
	}
	if _, err := os.Stdout.Write(append(b, '\n')); err != nil {
		return err
	}
	return nil
}

// Looks up executor info with GetExecution and substitutes the resulting
// hostnames for any executor host IDs in the given GetExecutionResponse.
func resolveWorkerHostnames(ctx context.Context, client bbspb.BuildBuddyServiceClient, rsp *espb.GetExecutionResponse) error {
	executorsResponse, err := client.GetExecutionNodes(ctx, &scpb.GetExecutionNodesRequest{
		RequestContext: &ctxpb.RequestContext{
			GroupId: *executorsGroupID,
		},
	})
	if err != nil {
		return fmt.Errorf("GetExecutionNodes: %w", err)
	}
	hostnames := map[string]string{}
	for _, node := range executorsResponse.GetExecutor() {
		hostnames[node.GetNode().GetExecutorHostId()] = node.GetNode().GetHost()
	}
	for _, execution := range rsp.GetExecution() {
		hostname, ok := hostnames[execution.GetExecutedActionMetadata().GetWorker()]
		if !ok {
			continue
		}
		// Update metadata from Execution row
		execution.ExecutedActionMetadata.Worker = hostname
		// Update metadata in inlined response, if applicable
		if md := execution.GetExecuteResponse().GetResult().GetExecutionMetadata(); md != nil {
			md.Worker = hostname
		}
	}
	return nil
}
