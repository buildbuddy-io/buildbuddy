// workflow is a prober that runs a very basic workflow in order to test remote
// runner health (used for remote bazel as well)
package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc/metadata"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
)

var (
	target = flag.String("target", "grpcs://remote.buildbuddy.io", "Buildbuddy app grpc target")
	apiKey = flag.String("api_key", "", "The API key used to authenticate the workflow run.")
)

const (
	proberRepoUrl           = "https://github.com/buildbuddy-io/probers"
	proberRepoDefaultBranch = "main"
	proberActionName        = "Prober test"

	pollInterval = 15 * time.Second
	pollTimeout  = 5 * time.Minute
)

func main() {
	flag.Parse()
	if *apiKey == "" {
		log.Fatalf("API key required to authenticate workflow run")
	}

	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		log.Fatalf("Error dialing BB target: %s", err)
	}
	bbClient := apipb.NewApiServiceClient(conn)

	executeRes, err := bbClient.ExecuteWorkflow(ctx, &apipb.ExecuteWorkflowRequest{
		RepoUrl:     proberRepoUrl,
		Branch:      proberRepoDefaultBranch,
		ActionNames: []string{proberActionName},
	})
	if err != nil {
		log.Fatalf("Error executing workflow: %s", err)
	}
	if len(executeRes.ActionStatuses) != 1 {
		log.Fatalf("Unexpected number of action statuses: %d", len(executeRes.ActionStatuses))
	}
	invocationID := executeRes.ActionStatuses[0].InvocationId

	// Poll until invocation is finished
	startTime := time.Now()
	for {
		if time.Since(startTime) > pollTimeout {
			break
		}

		invocationResp, err := bbClient.GetInvocation(ctx, &apipb.GetInvocationRequest{
			Selector: &apipb.InvocationSelector{
				InvocationId: invocationID,
			},
		})
		if err != nil {
			log.Fatalf("Error getting invocation: %s", err)
		}
		if len(invocationResp.Invocation) != 1 {
			log.Fatalf("Unexpected number of invocations: %d", len(invocationResp.Invocation))
		}
		inv := invocationResp.Invocation[0]

		invComplete := inv.BazelExitCode != ""
		if invComplete {
			if inv.Success && inv.BazelExitCode == "OK" {
				os.Exit(0)
			} else {
				log.Fatalf("Workflow failed: %v", invocationResp)
			}
		}
		// If the invocation hasn't completed yet, sleep and retry
		time.Sleep(pollInterval)
	}
	log.Fatalf("Workflow %s did not complete before timeout", invocationID)
}
