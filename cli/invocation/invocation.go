package invocation

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"regexp"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"google.golang.org/grpc/metadata"

	cmnpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	invpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	tpb "github.com/buildbuddy-io/buildbuddy/proto/target"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// maxLogBytes is the maximum bytes to include per target's stdout or stderr,
	// taking the tail so the most recent output (usually where errors appear) is preserved.
	maxLogBytes = 50 * 1024 // 50KB

	cmdUsage = `
usage: bb invocation <subcommand> [flags]

Subcommands:
  get <invocation-id-or-url>    Fetch invocation metadata and failing target logs

Examples:
  bb invocation get 12345678-1234-1234-1234-123456789012
  bb invocation get https://app.buildbuddy.io/invocation/12345678-...
`

	getUsage = `
usage: bb invocation get <invocation-id-or-url> [flags]

Fetches invocation metadata and resolved logs for failing targets as JSON.
For each failing target, stdout/stderr are fetched from the build cache and
included directly (not as digests).

Flags:
  --target    BuildBuddy gRPC target (default: grpcs://remote.buildbuddy.io)
  --api_key   BuildBuddy API key (overrides stored key from bb login)

Examples:
  bb invocation get 12345678-1234-1234-1234-123456789012
  bb invocation get https://app.buildbuddy.io/invocation/12345678-...
`
)

var (
	pathPattern = regexp.MustCompile(`/invocation/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})`)
	uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

// Result is the JSON output of `bb invocation get`.
type Result struct {
	InvocationID    string          `json:"invocation_id"`
	Success         bool            `json:"success"`
	Command         string          `json:"command"`
	Patterns        []string        `json:"patterns"`
	RepoURL         string          `json:"repo_url,omitempty"`
	CommitSHA       string          `json:"commit_sha,omitempty"`
	BranchName      string          `json:"branch_name,omitempty"`
	BazelExitCode   string          `json:"bazel_exit_code,omitempty"`
	DurationSeconds float64         `json:"duration_seconds"`
	// ConsoleBuffer contains the truncated build output captured by BuildBuddy.
	// Useful when there are no failing targets (e.g. build failed before any
	// target ran) or as additional context alongside failing target logs.
	ConsoleBuffer  string          `json:"console_buffer,omitempty"`
	FailingTargets []FailingTarget `json:"failing_targets"`
}

// FailingTarget contains the label, status, and resolved log output for a
// target that failed to build or whose tests failed.
type FailingTarget struct {
	Label  string `json:"label"`
	Status string `json:"status"`
	// Stdout and Stderr are fetched from the build cache and included directly.
	// They are truncated to the last 50KB if larger.
	Stdout string `json:"stdout,omitempty"`
	Stderr string `json:"stderr,omitempty"`
}

// HandleInvocation handles the `bb invocation` command tree.
func HandleInvocation(args []string) (int, error) {
	if len(args) == 0 {
		log.Print(cmdUsage)
		return 1, nil
	}
	switch args[0] {
	case "get":
		return handleGet(args[1:])
	default:
		log.Printf("Unknown subcommand %q", args[0])
		log.Print(cmdUsage)
		return 1, nil
	}
}

func handleGet(args []string) (int, error) {
	f := flag.NewFlagSet("invocation get", flag.ContinueOnError)
	target := f.String("target", login.DefaultApiTarget, "BuildBuddy gRPC target")
	apiKey := f.String("api_key", "", "BuildBuddy API key (overrides stored key)")

	if err := arg.ParseFlagSet(f, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(getUsage)
			return 1, nil
		}
		return -1, err
	}
	if f.NArg() != 1 {
		log.Print(getUsage)
		return 1, nil
	}

	invocationID, err := extractInvocationID(f.Arg(0))
	if err != nil {
		return -1, err
	}

	ctx := context.Background()
	key := *apiKey
	if key == "" {
		if k, err := login.GetAPIKey(); err == nil {
			key = k
		}
	}
	if key == "" {
		return -1, fmt.Errorf("not logged in: run 'bb login' or pass --api_key")
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", key)

	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		return -1, fmt.Errorf("connect to %s: %w", *target, err)
	}
	defer conn.Close()

	result, err := getInvocation(
		ctx,
		bbspb.NewBuildBuddyServiceClient(conn),
		bspb.NewByteStreamClient(conn),
		invocationID,
	)
	if err != nil {
		return -1, err
	}

	b, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return -1, fmt.Errorf("marshal output: %w", err)
	}
	_, err = os.Stdout.Write(append(b, '\n'))
	return 0, err
}

func getInvocation(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, bsClient bspb.ByteStreamClient, invocationID string) (*Result, error) {
	invResp, err := bbClient.GetInvocation(ctx, &invpb.GetInvocationRequest{
		Lookup: &invpb.InvocationLookup{InvocationId: invocationID},
	})
	if err != nil {
		return nil, fmt.Errorf("get invocation: %w", err)
	}
	if len(invResp.GetInvocation()) == 0 {
		return nil, fmt.Errorf("invocation %q not found", invocationID)
	}
	inv := invResp.GetInvocation()[0]

	result := &Result{
		InvocationID:    inv.GetInvocationId(),
		Success:         inv.GetSuccess(),
		Command:         inv.GetCommand(),
		Patterns:        inv.GetPattern(),
		RepoURL:         inv.GetRepoUrl(),
		CommitSHA:       inv.GetCommitSha(),
		BranchName:      inv.GetBranchName(),
		BazelExitCode:   inv.GetBazelExitCode(),
		DurationSeconds: float64(inv.GetDurationUsec()) / 1e6,
		ConsoleBuffer:   tail([]byte(inv.GetConsoleBuffer()), maxLogBytes),
	}

	if inv.GetSuccess() {
		return result, nil
	}

	failingTargets, err := fetchFailingTargets(ctx, bbClient, bsClient, invocationID)
	if err != nil {
		log.Printf("Warning: could not fetch failing targets: %v", err)
	}
	result.FailingTargets = failingTargets
	return result, nil
}

func fetchFailingTargets(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, bsClient bspb.ByteStreamClient, invocationID string) ([]FailingTarget, error) {
	resp, err := bbClient.GetTarget(ctx, &tpb.GetTargetRequest{
		InvocationId: invocationID,
	})
	if err != nil {
		return nil, fmt.Errorf("get targets: %w", err)
	}

	var failingTargets []FailingTarget
	for _, group := range resp.GetTargetGroups() {
		if !isFailingStatus(group.GetStatus()) {
			continue
		}
		for _, t := range group.GetTargets() {
			ft := FailingTarget{
				Label:  t.GetMetadata().GetLabel(),
				Status: t.GetStatus().String(),
			}
			stdout, stderr, err := fetchTargetLogs(ctx, bbClient, bsClient, invocationID, ft.Label)
			if err != nil {
				log.Printf("Warning: could not fetch logs for %s: %v", ft.Label, err)
			} else {
				ft.Stdout = stdout
				ft.Stderr = stderr
			}
			failingTargets = append(failingTargets, ft)
		}
	}
	return failingTargets, nil
}

func fetchTargetLogs(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, bsClient bspb.ByteStreamClient, invocationID, targetLabel string) (stdout, stderr string, err error) {
	execResp, err := bbClient.GetExecution(ctx, &espb.GetExecutionRequest{
		ExecutionLookup: &espb.ExecutionLookup{
			InvocationId: invocationID,
			TargetLabel:  targetLabel,
		},
		InlineExecuteResponse: true,
	})
	if err != nil {
		return "", "", fmt.Errorf("get executions: %w", err)
	}

	// Find the first execution with a non-zero exit code and an inlined response.
	for _, ex := range execResp.GetExecution() {
		if ex.GetExitCode() == 0 || ex.GetExecuteResponse() == nil {
			continue
		}
		logs, err := rexec.GetExecutionLogs(
			ctx,
			bsClient,
			"", // instance name — empty for the default BB instance
			repb.DigestFunction_SHA256,
			ex.GetExecuteResponse(),
		)
		if err != nil {
			return "", "", fmt.Errorf("fetch logs: %w", err)
		}
		return tail(logs.Stdout, maxLogBytes), tail(logs.Stderr, maxLogBytes), nil
	}
	return "", "", nil
}

// isFailingStatus reports whether s represents a build or test failure.
func isFailingStatus(s cmnpb.Status) bool {
	switch s {
	case cmnpb.Status_FAILED_TO_BUILD,
		cmnpb.Status_FAILED,
		cmnpb.Status_TIMED_OUT,
		cmnpb.Status_TOOL_FAILED:
		return true
	default:
		return false
	}
}

// tail returns the last n bytes of data as a string, or the full string if
// data is shorter than n.
func tail(data []byte, n int) string {
	if len(data) == 0 {
		return ""
	}
	if len(data) > n {
		data = data[len(data)-n:]
	}
	return string(data)
}

func extractInvocationID(input string) (string, error) {
	if uuidPattern.MatchString(input) {
		return input, nil
	}
	if u, err := url.Parse(input); err == nil {
		if m := pathPattern.FindStringSubmatch(u.Path); len(m) > 1 {
			return m[1], nil
		}
	}
	return "", fmt.Errorf("could not extract invocation ID from %q", input)
}
