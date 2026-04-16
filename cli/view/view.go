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
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
)

var (
	flags = flag.NewFlagSet("view", flag.ContinueOnError)
	Flags = flags

	apiTarget = flags.String("target", "remote.buildbuddy.io", "BuildBuddy gRPC target")
	lines     = flags.Int("lines", 100_000, "Minimum number of lines to fetch")
	runOnly   = flags.Bool("run", false, "If true, fetch only run logs")

	pathPattern = regexp.MustCompile(`/invocation/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})`)
	uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

	usage = `
bb ` + flags.Name() + ` <invocation-id-or-url> [--lines=100000] [--run] [--target=remote.buildbuddy.io]

Views build logs from BuildBuddy and, if available, run logs using the
GetEventLogChunk API.

Examples:
  bb view 12345678-1234-1234-1234-123456789012
  bb view --run 12345678-1234-1234-1234-123456789012
  bb view https://app.buildbuddy.io/invocation/12345678-1234-1234-1234-123456789012
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

type viewClient interface {
	GetEventLogChunk(context.Context, *elpb.GetEventLogChunkRequest, ...grpc.CallOption) (*elpb.GetEventLogChunkResponse, error)
	GetInvocation(context.Context, *inpb.GetInvocationRequest, ...grpc.CallOption) (*inpb.GetInvocationResponse, error)
}

func newGetEventLogChunkRequest(invocationID string, minLines int, logType elpb.LogType) *elpb.GetEventLogChunkRequest {
	return &elpb.GetEventLogChunkRequest{
		InvocationId: invocationID,
		MinLines:     int32(minLines),
		Type:         logType,
	}
}

func fetchLogChunk(ctx context.Context, bbClient viewClient, invocationID string, minLines int, logType elpb.LogType) (*elpb.GetEventLogChunkResponse, error) {
	return bbClient.GetEventLogChunk(ctx, newGetEventLogChunkRequest(invocationID, minLines, logType))
}

func fetchInvocation(ctx context.Context, bbClient viewClient, invocationID string) (*inpb.Invocation, error) {
	resp, err := bbClient.GetInvocation(ctx, &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{
			InvocationId: invocationID,
		},
	})
	if err != nil {
		return nil, err
	}
	if len(resp.GetInvocation()) == 0 {
		return nil, fmt.Errorf("invocation %s not found", invocationID)
	}
	return resp.GetInvocation()[0], nil
}

func shouldFetchRunLogs(inv *inpb.Invocation) bool {
	return inv.GetRunStatus() != inspb.OverallStatus_UNKNOWN_OVERALL_STATUS
}

const ansiReset = "\x1b[0m"

func appendLogSection(dst []byte, title string, src []byte) []byte {
	headerPrefix := ""
	if len(dst) > 0 {
		if dst[len(dst)-1] != '\n' {
			dst = append(dst, '\n')
		}
		dst = append(dst, '\n')
		headerPrefix = ansiReset
	}
	dst = append(dst, []byte(fmt.Sprintf("%s===== %s =====\n", headerPrefix, title))...)
	dst = append(dst, src...)
	if len(src) > 0 && src[len(src)-1] != '\n' {
		dst = append(dst, '\n')
	}
	return dst
}

func getLogs(ctx context.Context, bbClient viewClient, invocationID string, minLines int, runOnly bool) ([]byte, error) {
	if runOnly {
		resp, err := fetchLogChunk(ctx, bbClient, invocationID, minLines, elpb.LogType_RUN_LOG)
		if err != nil {
			return nil, fmt.Errorf("failed to get run log chunk: %w", err)
		}
		return appendLogSection(nil, "RUN LOG", resp.GetBuffer()), nil
	}

	buildResp, err := fetchLogChunk(ctx, bbClient, invocationID, minLines, elpb.LogType_BUILD_LOG)
	if err != nil {
		return nil, fmt.Errorf("failed to get build log chunk: %w", err)
	}
	output := appendLogSection(nil, "BUILD LOG", buildResp.GetBuffer())

	inv, err := fetchInvocation(ctx, bbClient, invocationID)
	if err != nil {
		return nil, fmt.Errorf("failed to get invocation: %w", err)
	}
	if !shouldFetchRunLogs(inv) {
		return output, nil
	}

	runResp, err := fetchLogChunk(ctx, bbClient, invocationID, minLines, elpb.LogType_RUN_LOG)
	if err != nil {
		return nil, fmt.Errorf("failed to get run log chunk: %w", err)
	}
	return appendLogSection(output, "RUN LOG", runResp.GetBuffer()), nil
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

	logs, err := getLogs(ctx, bbClient, invocationID, *lines, *runOnly)
	if err != nil {
		return -1, err
	}

	if len(logs) > 0 {
		fmt.Print(string(logs))
	}

	return 0, nil
}
