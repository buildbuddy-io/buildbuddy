// remote_runner is a prober that runs a very basic remote bazel run in order to test remote
// runner health (used for workflows as well)
package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	gitpb "github.com/buildbuddy-io/buildbuddy/proto/git"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
)

var (
	target = flag.String("target", "grpcs://remote.buildbuddy.io", "Buildbuddy app grpc target")
	apiKey = flag.String("api_key", "", "The API key used to authenticate the remote run.")
)

const (
	proberRepoUrl           = "https://github.com/buildbuddy-io/probers"
	proberRepoDefaultBranch = "main"

	pollInterval = 15 * time.Second
	pollTimeout  = 5 * time.Minute
)

func main() {
	flag.Parse()
	if *apiKey == "" {
		log.Fatalf("API key required to authenticate remote run")
	}

	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		log.Fatalf("Error dialing BB target: %s", err)
	}
	bbClient := bbspb.NewBuildBuddyServiceClient(conn)

	runRes, err := bbClient.Run(ctx, &rnpb.RunRequest{
		GitRepo: &gitpb.GitRepo{RepoUrl: proberRepoUrl},
		RepoState: &gitpb.RepoState{
			Branch: proberRepoDefaultBranch,
		},
		Steps: []*rnpb.Step{
			{
				Run: "bazel version",
			},
		},
		Async: false,
	})

	if err != nil {
		log.Fatalf("Error executing remote run: %s", err)
	}
	invocationID := runRes.InvocationId

	// Poll until invocation is finished
	startTime := time.Now()
	for {
		if time.Since(startTime) > pollTimeout {
			break
		}

		invocationResp, err := bbClient.GetInvocation(ctx, &inpb.GetInvocationRequest{
			Lookup: &inpb.InvocationLookup{
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
				log.Fatalf("Remote run failed: %v", invocationResp)
			}
		}
		// If the invocation hasn't completed yet, sleep and retry
		time.Sleep(pollInterval)
	}
	log.Fatalf("Remote run %s did not complete before timeout", invocationID)
}
