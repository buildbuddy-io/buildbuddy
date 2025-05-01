package flake

import (
	"context"
	"flag"
	"fmt"
	"os"

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

	ctx := context.Background()
	if apiKey, err := storage.ReadRepoConfig("api-key"); err == nil && apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	}
	// Dial BuildBuddy API
	conn, err := grpc_client.DialSimple(*targetFlag)
	if err != nil {
		return -1, fmt.Errorf("failed to dial target: %w", err)
	}
	defer conn.Close()

	client := buildbuddy_service.NewBuildBuddyServiceClient(conn)

	// Create and send the request
	req := &target.GetDailyTargetStatsRequest{}
	resp, err := client.GetDailyTargetStats(ctx, req)
	if err != nil {
		return -1, fmt.Errorf("GetDailyTargetStats RPC failed: %w", err)
	}

	// Print the flaky targets
	fmt.Fprintln(os.Stdout, "Flaky target stats over the last 7 days:")
	for _, stat := range resp.Stats {
		if stat.Data.GetFlakyRuns() > 0 {
			fmt.Fprintf(os.Stdout, "Date: %s, Flaky runs: %d\n", stat.Date, stat.Data.GetFlakyRuns())
		}
	}

	return 0, nil
}
