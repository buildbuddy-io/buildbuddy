package view

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"regexp"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
)

var (
	flags = flag.NewFlagSet("view", flag.ContinueOnError)

	apiTarget = flags.String("target", "remote.buildbuddy.io", "BuildBuddy gRPC target")
	lines     = flags.Int("lines", 100_000, "Minimum number of lines to fetch")

	usage = `
bb ` + flags.Name() + ` <invocation-id-or-url> [--lines=100000] [--target=remote.buildbuddy.io]

Views build logs from BuildBuddy using the GetEventLogChunk API.

Examples:
  bb view 12345678-1234-1234-1234-123456789012
  bb view https://app.buildbuddy.io/invocation/12345678-1234-1234-1234-123456789012
`
)

func extractInvocationID(input string) (string, error) {
	uuidPattern := regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	if uuidPattern.MatchString(input) {
		return input, nil
	}

	parsedURL, err := url.Parse(input)
	if err == nil {
		pathPattern := regexp.MustCompile(`/invocation/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})`)
		if matches := pathPattern.FindStringSubmatch(parsedURL.Path); len(matches) > 1 {
			return matches[1], nil
		}
	}

	return "", fmt.Errorf("could not extract invocation ID from: %s", input)
}

func HandleView(args []string) (exitCode int, err error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return -1, err
	}

	if flags.NArg() < 1 {
		log.Print("Error: invocation ID or URL required")
		log.Print(usage)
		return 1, nil
	}

	invocationID, err := extractInvocationID(flags.Arg(0))
	if err != nil {
		return -1, err
	}

	apiKey, err := login.GetAPIKey()
	if err != nil || apiKey == "" {
		return -1, fmt.Errorf("not logged in: %w", err)
	}

	conn, err := grpc_client.DialSimple(*apiTarget)
	if err != nil {
		return -1, fmt.Errorf("failed to connect to BuildBuddy: %w", err)
	}
	defer conn.Close()
	bbClient := bbspb.NewBuildBuddyServiceClient(conn)

	ctx := context.Background()
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	}

	resp, err := bbClient.GetEventLogChunk(ctx, &elpb.GetEventLogChunkRequest{
		InvocationId: invocationID,
		MinLines:     int32(*lines),
	})
	if err != nil {
		return -1, fmt.Errorf("failed to get log chunk: %w", err)
	}

	// Print the log chunk
	if len(resp.Buffer) > 0 {
		fmt.Print(string(resp.Buffer))
	}

	return 0, nil
}
