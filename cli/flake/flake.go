package flake

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	"github.com/buildbuddy-io/buildbuddy/proto/target"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"google.golang.org/grpc/metadata"
)

var (
	flags      = flag.NewFlagSet("flake", flag.ContinueOnError)
	targetFlag = flags.String("target", login.DefaultApiTarget, "Cache gRPC target")

	usage = `
usage: bb ` + flags.Name() + `

Lists flakes for the last 7 days.
`
)

func HandleFlake(args []string) (int, error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return -1, err
	}

	if len(flags.Args()) > 0 {
		log.Print(usage)
		return 1, nil
	}

	if *targetFlag == "" {
		log.Printf("A non-empty --target must be specified")
		return 1, nil
	}

	apiKey, err := storage.ReadRepoConfig("api-key")
	if err != nil {
		return 1, fmt.Errorf("could not read api key: %s", err)
	}
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-buildbuddy-api-key", apiKey)
	// Dial BuildBuddy API
	conn, err := grpc_client.DialSimple(*targetFlag)
	if err != nil {
		return -1, fmt.Errorf("failed to dial target: %w", err)
	}
	defer conn.Close()

	client := buildbuddy_service.NewBuildBuddyServiceClient(conn)

	req := &target.GetTargetStatsRequest{}
	resp, err := client.GetTargetStats(ctx, req)
	if err != nil {
		return -1, fmt.Errorf("GetTargetStats RPC failed: %w", err)
	}

	if len(resp.Stats) == 0 {
		fmt.Fprintln(os.Stdout, "No flaky targets found in the last 7 days.")
		return 0, nil
	}

	// Sort by flaky runs descending
	sort.Slice(resp.Stats, func(i, j int) bool {
		return resp.Stats[i].Data.FlakyRuns > resp.Stats[j].Data.FlakyRuns
	})

	fmt.Fprintln(os.Stdout, "Top flaky targets in the last 7 days:")
	for _, stat := range resp.Stats {
		if stat.Data.FlakyRuns > 0 {
			fmt.Fprintf(os.Stdout, "%6d flakes  %s\n", stat.Data.FlakyRuns, stat.Label)
		}
	}
	return 0, nil
}
