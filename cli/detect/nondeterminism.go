package detect

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/explain"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/bazelrc"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	npb "github.com/buildbuddy-io/buildbuddy/proto/notification"
	sdpb "github.com/buildbuddy-io/buildbuddy/proto/spawn_diff"
)

const (
	defaultBazelCommand            = "build //..."
	defaultBESBackend              = "grpcs://remote.buildbuddy.io"
	defaultBESResultsURL           = "https://app.buildbuddy.io/invocation/"
	exitCodeNondeterminismDetected = 10

	// Keep this in sync with ci_runner_env.BuildBuddyInvocationIDEnvVarName.
	WorkflowInvocationIDEnvVar = "BUILDBUDDY_INVOCATION_ID"
)

const nondeterminismUsage = `
usage: bb detect nondeterminism [options]

Runs a Bazel command twice without accepting remote cache hits, then compares
the compact execution logs with bb explain.

If no Bazel command is provided, "build //..." is used.

Examples:
  bb detect nondeterminism
  bb detect nondeterminism --bazel_command='build //foo:bar --config=linux'

Exit codes:
  0: no nondeterminism detected
  10: nondeterminism detected
  other nonzero: command, usage, or infrastructure error
`

var (
	nondeterminismFlags = flag.NewFlagSet("detect nondeterminism", flag.ContinueOnError)

	bazelCommand  = nondeterminismFlags.String("bazel_command", defaultBazelCommand, "Shell-tokenized Bazel command to run twice.")
	besBackend    = nondeterminismFlags.String("bes_backend", defaultBESBackend, "BuildBuddy BES backend target to stream Bazel results to.")
	besResultsURL = nondeterminismFlags.String(
		"bes_results_url",
		defaultBESResultsURL,
		"BuildBuddy invocation URL prefix for Bazel BES results.",
	)
	notifyEmail = nondeterminismFlags.Bool("notify_email", false, "Send an email to workspace admins when nondeterminism is detected.")
	notifySlack = nondeterminismFlags.String("notify_slack", "", "Name of the BuildBuddy secret with the Slack webhook URL to notify when nondeterminism is detected.")
)

var errNondeterminismDetected = errors.New("nondeterminism detected")

type commandRunner interface {
	Run(ctx context.Context, name string, args ...string) error
}

type explainer interface {
	Diff(oldLog, newLog string, nondeterministicOnly bool) (*sdpb.DiffResult, error)
	WriteText(w io.Writer, diff *sdpb.DiffResult, verbose bool)
}

type osCommandRunner struct{}

func (r osCommandRunner) Run(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	return cmd.Run()
}

type options struct {
	bazelArgs     *arg.BazelArgs
	besBackend    string
	besResultsURL string
}

type checker struct {
	opts      options
	runner    commandRunner
	explainer explainer
}

type buildMetadata struct {
	tempDir         string
	oldLog          string
	newLog          string
	oldInvocationID string
	newInvocationID string
}

type explainRunner struct{}

func (explainRunner) Diff(oldLog, newLog string, nondeterministicOnly bool) (*sdpb.DiffResult, error) {
	return explain.Diff(oldLog, newLog, nondeterministicOnly)
}

func (explainRunner) WriteText(w io.Writer, diff *sdpb.DiffResult, verbose bool) {
	explain.WriteText(w, diff, verbose)
}

func handleNondeterminism(args []string) (int, error) {
	if err := arg.ParseFlagSet(nondeterminismFlags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(nondeterminismUsage)
			return 1, nil
		}
		return -1, err
	}
	if len(nondeterminismFlags.Args()) > 0 {
		log.Print(nondeterminismUsage)
		return -1, fmt.Errorf("unexpected positional args %q; pass the Bazel command with --bazel_command or omit it to use \"build //...\"", nondeterminismFlags.Args())
	}

	if *notifyEmail || *notifySlack != "" {
		if os.Getenv("BB_NOTIFY_API_KEY") == "" {
			return -1, status.PermissionDeniedError("To send notifications, set the BB_NOTIFY_API_KEY environment variable with an API key with the notification capability.")
		}
	}

	bazelArgs, err := parseBazelCommand(*bazelCommand)
	if err != nil {
		log.Print(nondeterminismUsage)
		return -1, err
	}
	opts := options{
		bazelArgs:     bazelArgs,
		besBackend:    *besBackend,
		besResultsURL: *besResultsURL,
	}

	c := &checker{
		opts:      opts,
		runner:    osCommandRunner{},
		explainer: explainRunner{},
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := c.Run(ctx); err != nil {
		if errors.Is(err, errNondeterminismDetected) {
			return exitCodeNondeterminismDetected, nil
		}
		return -1, err
	}
	return 0, nil
}

func parseBazelCommand(command string) (*arg.BazelArgs, error) {
	command = strings.TrimSpace(command)
	args, err := shlex.Split(command)
	if err != nil {
		return nil, fmt.Errorf("parse --bazel_command: %w", err)
	}
	bazelArgs, err := arg.NewBazelArgsNoResolve(args)
	if err != nil {
		return nil, fmt.Errorf("parse Bazel command: %w", err)
	}
	bazelCommand := bazelArgs.GetCommand()
	if !bazelrc.IsBazelCommand(bazelCommand) {
		return nil, fmt.Errorf("expected a Bazel command like build or test, got %q", bazelCommand)
	}
	return bazelArgs, nil
}

func (c *checker) Run(ctx context.Context) error {
	m, err := newBuildMetadata()
	if err != nil {
		return err
	}
	defer os.RemoveAll(m.tempDir)
	log.Printf("Writing nondeterminism check files under %s", m.tempDir)

	buildErr := c.runBuilds(ctx, m)
	if buildErr != nil {
		if errors.Is(buildErr, context.Canceled) {
			return buildErr
		}
		log.Warnf("One or both uncached builds failed; continuing with bb explain: %s", buildErr)
	}

	diff, explainErr := c.runExplainDiff(ctx, m)
	if explainErr != nil {
		return errors.Join(buildErr, explainErr)
	}
	if len(diff.GetSpawnDiffs()) == 0 {
		log.Print("\n\n\033[32mNo nondeterminism detected.\033[0m\n\n")
		return buildErr
	}

	log.Print("\n\n\033[31mNondeterminism detected.\033[0m\n\n")
	if err := c.printExplainText(ctx, diff); err != nil {
		log.Warnf("Failed to print text bb explain output: %s", err)
	}

	if *notifyEmail || *notifySlack != "" {
		if err := c.notifyNondeterminism(ctx, m); err != nil {
			log.Warnf("Failed to send nondeterminism notification: %s", err)
		}
	}
	return errors.Join(buildErr, errNondeterminismDetected)
}

func (c *checker) notifyNondeterminism(ctx context.Context, m *buildMetadata) error {
	// Sending notifications requires an API key with notification capabilities.
	apiKey := os.Getenv("BB_NOTIFY_API_KEY")
	conn, err := grpc_client.DialSimple(c.opts.besBackend)
	if err != nil {
		return status.UnavailableErrorf("failed to connect to %s: %s", c.opts.besBackend, err)
	}
	defer conn.Close()

	event := &npb.NondeterminismDetected{
		BuildInvocationIds: []string{
			m.oldInvocationID,
			m.newInvocationID,
		},
	}
	// When running inside a BuildBuddy workflow, also link to the workflow
	// invocation, which contains both builds nested under it.
	if workflowInvocationID := os.Getenv(WorkflowInvocationIDEnvVar); workflowInvocationID != "" {
		event.ParentInvocationId = workflowInvocationID
	}

	channels := make([]*npb.NotificationChannel, 0, 2)
	if *notifyEmail {
		channels = append(channels, &npb.NotificationChannel{
			Channel: &npb.NotificationChannel_Email{
				Email: &npb.EmailChannel{},
			},
		})
	}
	if *notifySlack != "" {
		channels = append(channels, &npb.NotificationChannel{
			Channel: &npb.NotificationChannel_Slack{
				Slack: &npb.SlackChannel{
					WebhookUrlSecretName: *notifySlack,
				},
			},
		})
	}

	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	client := bbspb.NewBuildBuddyServiceClient(conn)
	if _, err := client.SendNotification(ctx, &npb.SendNotificationRequest{
		Event: &npb.SendNotificationRequest_NondeterminismDetected{
			NondeterminismDetected: event,
		},
		Channels: channels,
	}); err != nil {
		return status.UnavailableErrorf("failed to send nondeterminism notification: %s", err)
	}
	log.Print("Notification sent.")
	return nil
}

func newBuildMetadata() (*buildMetadata, error) {
	dir, err := os.MkdirTemp("", "nondeterminism-check-*")
	if err != nil {
		return nil, err
	}
	return &buildMetadata{
		tempDir:         dir,
		oldLog:          filepath.Join(dir, "build_1_compact_exec_log.pb.zst"),
		newLog:          filepath.Join(dir, "build_2_compact_exec_log.pb.zst"),
		oldInvocationID: uuid.New(),
		newInvocationID: uuid.New(),
	}, nil
}

func (c *checker) runBuilds(ctx context.Context, m *buildMetadata) error {
	var buildErr error
	for i, build := range []struct {
		outputBase   string
		logPath      string
		invocationID string
	}{
		{outputBase: filepath.Join(m.tempDir, "output_base_1"), logPath: m.oldLog, invocationID: m.oldInvocationID},
		{outputBase: filepath.Join(m.tempDir, "output_base_2"), logPath: m.newLog, invocationID: m.newInvocationID},
	} {
		if err := c.runBuild(ctx, i+1, build.outputBase, build.logPath, build.invocationID); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			buildErr = errors.Join(buildErr, err)
		}
	}
	return buildErr
}

func (c *checker) runBuild(ctx context.Context, buildNumber int, outputBase, compactLogPath, invocationID string) error {
	args, err := addBazelFlags(c.opts.bazelArgs, outputBase, compactLogPath, invocationID, c.opts.besBackend, c.opts.besResultsURL)
	if err != nil {
		return err
	}

	defer c.cleanupBazel(outputBase)
	return c.runner.Run(ctx, "bazel", args...)
}

func (c *checker) cleanupBazel(outputBase string) {
	// Use a non-cancelled context to ensure the cleanup happens.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	log.Printf("Cleaning up output base %s", outputBase)
	if err := c.runner.Run(shutdownCtx, "bazel", "--output_base="+outputBase, "shutdown"); err != nil {
		log.Warnf("Failed to shut down Bazel server for output base %s: %s", outputBase, err)
	}
	if err := c.runner.Run(shutdownCtx, "bazel", "--output_base="+outputBase, "clean", "--expunge"); err != nil {
		log.Warnf("Failed to clean up Bazel output base %s: %s", outputBase, err)
	}
}

func addBazelFlags(baseArgs *arg.BazelArgs, outputBase, compactLogPath, invocationID, besBackend, besResultsURL string) ([]string, error) {
	// Clone the base args to avoid mutating the original, because each build has different flags (output base, compact log path, etc.).
	clonedArgs, err := arg.NewBazelArgsNoResolve(baseArgs.Forwarded())
	if err != nil {
		return nil, err
	}
	// Use a separate output base for each build to avoid local cache hits which might paper over nondeterminism.
	if err := clonedArgs.AppendStartupOption("output_base", outputBase); err != nil {
		return nil, err
	}
	for _, flag := range []string{
		"--bes_backend=" + besBackend,
		"--bes_results_url=" + besResultsURL,
		// Don't accept remote cache hits, which might paper over nondeterminism.
		"--noremote_accept_cached",
		// Disable local caches too.
		// We don't disable the repository cache because it validates digests on hits, so it can't get poisoned with non-determinism.
		"--repo_contents_cache=",
		"--disk_cache=",
		"--noexperimental_convenience_symlinks",
		"--execution_log_compact_file=" + compactLogPath,
		"--invocation_id=" + invocationID,
	} {
		if err := clonedArgs.Append(flag); err != nil {
			return nil, err
		}
	}
	if err := login.ConfigureAPIKey(clonedArgs); err != nil {
		return nil, err
	}
	return clonedArgs.Forwarded(), nil
}

func (c *checker) runExplainDiff(ctx context.Context, m *buildMetadata) (*sdpb.DiffResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Both builds run the same command on the same sources, so restrict the diff
	// to spawns that represent genuine non-determinism.
	diff, err := c.explainer.Diff(m.oldLog, m.newLog, true /* nondeterministicOnly */)
	if err != nil {
		return nil, fmt.Errorf("run bb explain: %w", err)
	}
	return diff, nil
}

func (c *checker) printExplainText(ctx context.Context, diff *sdpb.DiffResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	log.Print("bb explain output:\n")
	c.explainer.WriteText(os.Stdout, diff, false /* verbose */)
	return nil
}
