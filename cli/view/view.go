package view

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"regexp"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/util/download"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	flags = flag.NewFlagSet("view", flag.ContinueOnError)
	Flags = flags

	apiTarget  = flags.String("target", "remote.buildbuddy.io", "BuildBuddy gRPC target")
	lines      = flags.Int("lines", 100_000, "Minimum number of lines to fetch")
	testFilter = flags.String("test_filter", "", "If set, print only the output of matching failed test cases instead of the full build logs. The value is a test-name pattern (regular expression), matching bazel's --test_filter.")

	pathPattern = regexp.MustCompile(`/invocation/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})`)
	uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

	usage = `
bb ` + flags.Name() + ` <invocation-id-or-url> [target...] [--lines=100000] [--test_filter=TestName] [--target=remote.buildbuddy.io]

Views build logs from BuildBuddy using the GetEventLogChunk API.

If one or more target labels are given (and/or --test_filter is set), prints the
output of failed test cases instead of the full build logs. --test_filter is a
regular expression (matching bazel's --test_filter semantics). With no
target, every failing test target in the invocation is searched.

Examples:
  bb view 12345678-1234-1234-1234-123456789012
  bb view https://app.buildbuddy.io/invocation/12345678-1234-1234-1234-123456789012
  bb view 12345678-1234-1234-1234-123456789012 //server/foo:foo_test
  bb view 12345678-1234-1234-1234-123456789012 //server/foo:foo_test --test_filter=TestName
`
)

func extractInvocationID(input string) (string, error) {
	if uuidPattern.MatchString(input) {
		return input, nil
	}

	parsedURL, err := url.Parse(input)
	if err == nil {
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

	// When targets and/or --test_filter are given, print only the matching
	// failed test cases instead of the full build logs.
	targets := flags.Args()[1:]
	if len(targets) > 0 || *testFilter != "" {
		bsClient := bspb.NewByteStreamClient(conn)
		return ViewFilteredTestOutput(ctx, bbClient, download.NewByteStreamDownloader(bsClient), os.Stdout, invocationID, targets, *testFilter)
	}

	// If the invocation has run logs, only print the run logs and not the build logs.
	req := &elpb.GetEventLogChunkRequest{
		InvocationId: invocationID,
		MinLines:     int32(*lines),
		Type:         elpb.LogType_RUN_LOG,
	}
	header := "RUN LOGS"
	resp, err := bbClient.GetEventLogChunk(ctx, req)
	if err != nil && !status.IsNotFoundError(err) {
		log.Debugf("failed to get run logs: %s", err)
	}

	if err != nil || len(resp.Buffer) == 0 {
		req.Type = elpb.LogType_BUILD_LOG
		header = "BUILD LOGS"
		resp, err = bbClient.GetEventLogChunk(ctx, req)
		if err != nil {
			return -1, fmt.Errorf("failed to get build logs: %w", err)
		}
	}

	// Print the log chunk
	if len(resp.Buffer) > 0 {
		fmt.Printf("===== %s =====\n", header)
		fmt.Print(string(resp.Buffer))
	}

	return 0, nil
}

func hasRunLogs(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, invocationID string) (bool, error) {
	resp, err := bbClient.GetInvocation(ctx, &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{
			InvocationId: invocationID,
		},
	})
	if err != nil {
		return false, err
	}
	if len(resp.GetInvocation()) == 0 {
		return false, fmt.Errorf("invocation %s not found", invocationID)
	}
	inv := resp.GetInvocation()[0]
	return inv.GetRunStatus() != inspb.OverallStatus_UNKNOWN_OVERALL_STATUS, nil
}
