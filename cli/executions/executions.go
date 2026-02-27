package executions

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/terminal"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const usage = `
usage: bb executions get <execution_id> [--output=markdown|md|json] [--target=grpcs://remote.buildbuddy.io]

Fetches the cached execution response for an execution ID.

By default (--output=markdown), returns a summary including:
  - RPC status and message
  - Exit code and output counts (files, directories, symlinks)
  - Stdout/stderr details (inline size or digest)
  - Server logs (name and digest)
  - Execution metadata (worker/executor, timestamps, durations)

Use --output=json to print the full ExecuteResponse as JSON.

Examples:
  # Basic usage
  bb executions get uploads/0f8fad5b-d9cb-469f-a165-70867728950e/blobs/f33f7f8f85f0c4f68f68422e6159c252f2b0f73cc75ad1df69c46733465ff7f7/142

  # Output as JSON
  bb executions get uploads/.../blobs/... --output=json
`

type getFlags struct {
	target string
	apiKey string
	output string
}

func HandleExecutions(args []string) (int, error) {
	if len(args) == 0 {
		log.Print(usage)
		return 1, nil
	}

	switch args[0] {
	case "get":
		return handleGet(args[1:])
	default:
		log.Printf("Unknown subcommand %q", args[0])
		log.Print(usage)
		return 1, nil
	}
}

func handleGet(args []string) (int, error) {
	f := flag.NewFlagSet("executions get", flag.ContinueOnError)
	target := f.String("target", login.DefaultApiTarget, "BuildBuddy gRPC target")
	apiKey := f.String("api_key", "", "Optionally override BuildBuddy API key")
	output := f.String("output", "markdown", "Output format: markdown (or md), json")

	if err := arg.ParseFlagSet(f, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return -1, err
	}
	if f.NArg() != 1 {
		log.Print(usage)
		return 1, nil
	}
	flags := &getFlags{target: *target, apiKey: *apiKey, output: strings.ToLower(*output)}
	if flags.output == "md" {
		flags.output = "markdown"
	}
	if flags.output != "markdown" && flags.output != "json" {
		return -1, fmt.Errorf("invalid --output %q (allowed values: markdown, md, json)", *output)
	}

	ctx := context.Background()
	if flags.apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", flags.apiKey)
	} else if apiKey, err := login.GetAPIKey(); err == nil && apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	}

	executionID := f.Arg(0)
	executeResponse, err := fetchExecuteResponse(ctx, flags.target, executionID)
	if err != nil {
		return -1, err
	}

	if flags.output == "json" {
		b, err := protojson.MarshalOptions{Multiline: true}.Marshal(executeResponse)
		if err != nil {
			return -1, fmt.Errorf("marshal ExecuteResponse as json: %w", err)
		}
		_, err = os.Stdout.Write(append(b, '\n'))
		return 0, err
	}

	_, err = fmt.Fprint(os.Stdout, RenderMarkdown(executionID, executeResponse))
	return 0, err
}

func fetchExecuteResponse(ctx context.Context, target, executionID string) (*repb.ExecuteResponse, error) {
	conn, err := grpc_client.DialSimple(target)
	if err != nil {
		return nil, fmt.Errorf("dial %q: %w", target, err)
	}
	defer conn.Close()

	acClient := repb.NewActionCacheClient(conn)
	return rexec.GetCachedExecuteResponse(ctx, acClient, executionID)
}

// RenderMarkdown renders a human-friendly markdown summary of an ExecuteResponse.
func RenderMarkdown(executionID string, executeResponse *repb.ExecuteResponse) string {
	return RenderMarkdownWithDetails(executionID, executeResponse, nil)
}

type jsonOutput struct {
	ExecutionID     string          `json:"executionId"`
	ExecuteResponse json.RawMessage `json:"executeResponse"`
	Stdout          string          `json:"stdout"`
	Stderr          string          `json:"stderr"`
	ServerLogs      []serverLogJSON `json:"serverLogs,omitempty"`
}

type serverLogJSON struct {
	Name string `json:"name"`
	Text string `json:"text"`
}

// WriteJSONOutput writes JSON execution output including fetched log details.
func WriteJSONOutput(w io.Writer, executionID string, executeResponse *repb.ExecuteResponse, logs *rexec.ExecutionLogs) error {
	executeResponseJSON, err := protojson.Marshal(executeResponse)
	if err != nil {
		return fmt.Errorf("marshal execute response: %w", err)
	}
	if logs == nil {
		logs = &rexec.ExecutionLogs{}
	}

	out := jsonOutput{
		ExecutionID:     executionID,
		ExecuteResponse: executeResponseJSON,
		Stdout:          string(logs.Stdout),
		Stderr:          string(logs.Stderr),
	}

	if len(logs.ServerLogs) > 0 {
		logNames := make([]string, 0, len(logs.ServerLogs))
		for name := range logs.ServerLogs {
			logNames = append(logNames, name)
		}
		slices.Sort(logNames)
		out.ServerLogs = make([]serverLogJSON, 0, len(logNames))
		for _, name := range logNames {
			out.ServerLogs = append(out.ServerLogs, serverLogJSON{
				Name: name,
				Text: string(logs.ServerLogs[name]),
			})
		}
	}

	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal output json: %w", err)
	}
	if _, err := w.Write(append(b, '\n')); err != nil {
		return err
	}
	return nil
}

// RenderMarkdownWithDetails renders a human-friendly markdown summary of an
// ExecuteResponse, optionally including fetched stdout/stderr details.
func RenderMarkdownWithDetails(executionID string, executeResponse *repb.ExecuteResponse, details *rexec.ExecutionLogs) string {
	var b strings.Builder
	statusCode := codes.Code(executeResponse.GetStatus().GetCode())
	statusColor := colorForStatus(statusCode)
	var stdout, stderr []byte
	if details != nil {
		stdout = details.Stdout
		stderr = details.Stderr
	}

	b.WriteString(colorHeading("# Execution details\n"))
	b.WriteString(fmt.Sprintf("- Execution ID: `%s`\n", executionID))
	b.WriteString("- Stage: Completed\n")
	b.WriteString(fmt.Sprintf("- RPC status: %s%s (%d)%s", statusColor, statusCode.String(), statusCode, terminal.Esc()))
	if msg := executeResponse.GetStatus().GetMessage(); msg != "" {
		b.WriteString(fmt.Sprintf(": %s", msg))
	}
	b.WriteString("\n")
	b.WriteString(fmt.Sprintf("- Served from cache: %s\n", yesNo(executeResponse.GetCachedResult())))
	if msg := executeResponse.GetMessage(); msg != "" {
		b.WriteString(fmt.Sprintf("- Message: %s\n", msg))
	}

	result := executeResponse.GetResult()
	if result != nil {
		if len(stdout) == 0 {
			stdout = result.GetStdoutRaw()
		}
		if len(stderr) == 0 {
			stderr = result.GetStderrRaw()
		}

		b.WriteString("\n")
		b.WriteString(colorHeading("# Result details\n"))
		b.WriteString(fmt.Sprintf("- Exit code: %d\n", result.GetExitCode()))
		b.WriteString(fmt.Sprintf("- Outputs: %d files, %d directories, %d symlinks\n", len(result.GetOutputFiles()), len(result.GetOutputDirectories()), len(result.GetOutputSymlinks())))
		b.WriteString(fmt.Sprintf("- Stdout: %s\n", outputSummary(len(result.GetStdoutRaw()), result.GetStdoutDigest())))
		b.WriteString(fmt.Sprintf("- Stderr: %s\n", outputSummary(len(result.GetStderrRaw()), result.GetStderrDigest())))

		if len(executeResponse.GetServerLogs()) > 0 {
			b.WriteString("- Server logs:\n")
			logNames := make([]string, 0, len(executeResponse.GetServerLogs()))
			for name := range executeResponse.GetServerLogs() {
				logNames = append(logNames, name)
			}
			slices.Sort(logNames)
			for _, name := range logNames {
				logFile := executeResponse.GetServerLogs()[name]
				b.WriteString(fmt.Sprintf("  - %s: %s\n", name, digestString(logFile.GetDigest())))
			}
		}

		md := result.GetExecutionMetadata()
		if md != nil {
			workerQueuedTS := "Unknown"
			aux := &espb.ExecutionAuxiliaryMetadata{}
			if ok, err := rexec.FindFirstAuxiliaryMetadata(md, aux); err == nil && ok {
				workerQueuedTS = formatTimestamp(aux.GetWorkerQueuedTimestamp())
			}

			b.WriteString("\n")
			b.WriteString(colorHeading("# Execution metadata\n"))
			b.WriteString(fmt.Sprintf("- Worker: %s\n", emptyIfUnset(md.GetWorker())))
			b.WriteString(fmt.Sprintf("- Executor ID: %s\n", emptyIfUnset(md.GetExecutorId())))
			b.WriteString(fmt.Sprintf("- Queued: %s\n", formatTimestamp(md.GetQueuedTimestamp())))
			b.WriteString(fmt.Sprintf("- Worker queued: %s\n", workerQueuedTS))
			b.WriteString(fmt.Sprintf("- Worker started: %s\n", formatTimestamp(md.GetWorkerStartTimestamp())))
			b.WriteString(fmt.Sprintf("- Worker completed: %s\n", formatTimestamp(md.GetWorkerCompletedTimestamp())))
			b.WriteString(fmt.Sprintf("- Input fetch duration: %s\n", formatDuration(md.GetInputFetchStartTimestamp(), md.GetInputFetchCompletedTimestamp())))
			b.WriteString(fmt.Sprintf("- Execution duration: %s\n", formatDuration(md.GetExecutionStartTimestamp(), md.GetExecutionCompletedTimestamp())))
			b.WriteString(fmt.Sprintf("- Output upload duration: %s\n", formatDuration(md.GetOutputUploadStartTimestamp(), md.GetOutputUploadCompletedTimestamp())))
			b.WriteString(fmt.Sprintf("- Worker total duration: %s\n", formatDuration(md.GetWorkerStartTimestamp(), md.GetWorkerCompletedTimestamp())))
		}
	}

	b.WriteString("\n")
	b.WriteString(colorHeading("# Execution stdout\n"))
	b.WriteString(renderTextSection(stdout))
	b.WriteString("\n")
	b.WriteString(colorHeading("# Execution stderr\n"))
	b.WriteString(renderTextSection(stderr))
	b.WriteString("\n")
	b.WriteString(colorHeading("# Execution server logs\n"))
	b.WriteString(renderServerLogsSection(executeResponse, details))
	return b.String()
}

func colorHeading(s string) string {
	return terminal.Esc(1, 36) + s + terminal.Esc()
}

func colorForStatus(code codes.Code) string {
	if code == codes.OK {
		return terminal.Esc(32)
	}
	return terminal.Esc(31)
}

func yesNo(v bool) string {
	if v {
		return "Yes"
	}
	return "No"
}

func emptyIfUnset(v string) string {
	if v == "" {
		return "Unknown"
	}
	return v
}

func outputSummary(rawLen int, d *repb.Digest) string {
	if rawLen > 0 {
		return fmt.Sprintf("inline (%d bytes)", rawLen)
	}
	if d == nil {
		return "None"
	}
	return digestString(d)
}

func digestString(d *repb.Digest) string {
	if d == nil {
		return "Unknown"
	}
	return fmt.Sprintf("%s/%d", d.GetHash(), d.GetSizeBytes())
}

func formatTimestamp(ts *timestamppb.Timestamp) string {
	if ts == nil || ts.AsTime().IsZero() {
		return "Unknown"
	}
	return ts.AsTime().UTC().Format(time.RFC3339Nano)
}

func formatDuration(start, end *timestamppb.Timestamp) string {
	if start == nil || end == nil || start.AsTime().IsZero() || end.AsTime().IsZero() {
		return "Unknown"
	}
	d := end.AsTime().Sub(start.AsTime())
	if d < 0 {
		d = -d
	}
	return d.String()
}

func renderTextSection(data []byte) string {
	if len(data) == 0 {
		return "(Empty)\n"
	}
	if !utf8.Valid(data) {
		return fmt.Sprintf("<binary: %d bytes>\n", len(data))
	}
	out := "```\n" + string(data)
	if data[len(data)-1] != '\n' {
		out += "\n"
	}
	out += "```\n"
	return out
}

func renderServerLogsSection(executeResponse *repb.ExecuteResponse, details *rexec.ExecutionLogs) string {
	if details != nil && len(details.ServerLogs) > 0 {
		logNames := make([]string, 0, len(details.ServerLogs))
		for name := range details.ServerLogs {
			logNames = append(logNames, name)
		}
		slices.Sort(logNames)

		var b strings.Builder
		for i, name := range logNames {
			if i > 0 {
				b.WriteString("\n")
			}
			b.WriteString(colorHeading(fmt.Sprintf("## %s\n", name)))
			b.WriteString(renderTextSection(details.ServerLogs[name]))
		}
		return b.String()
	}

	if len(executeResponse.GetServerLogs()) == 0 {
		return "(Empty)\n"
	}

	// Server logs exist but were not fetched in this code path.
	logNames := make([]string, 0, len(executeResponse.GetServerLogs()))
	for name := range executeResponse.GetServerLogs() {
		logNames = append(logNames, name)
	}
	slices.Sort(logNames)
	var b strings.Builder
	for _, name := range logNames {
		logFile := executeResponse.GetServerLogs()[name]
		b.WriteString(fmt.Sprintf("- %s: %s\n", name, digestString(logFile.GetDigest())))
	}
	return b.String()
}
