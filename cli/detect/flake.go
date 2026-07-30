package detect

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/remotebazel"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	gitpb "github.com/buildbuddy-io/buildbuddy/proto/git"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	defaultFlakeRuns      = 100
	exitCodeFlakeDetected = 10

	bazelTestFailureExitCode = 3
	bazelNoTestsExitCode     = 4
)

const flakeUsage = `
usage: bb detect flake <invocation-id-or-url> --target=<test-target> --test_filter=<pattern> [--n=100]

Attempts to reproduce a flaky test using the effective Bazel flags from an
existing BuildBuddy invocation. It progressively tries:

  1. The specified target and test filter with --runs_per_test=n.
  2. The specified target without a test filter with --runs_per_test=n.
  3. The full original command with --runs_per_test=n.

Each strategy uses one Bazel invocation and disables test-result caching.

Examples:
  bb detect flake 12345678-1234-1234-1234-123456789012 \
    --target=//server/foo:foo_test --test_filter=TestName --n=100

Exit codes:
  0: the flake was not reproduced
  10: the flake was reproduced
  other nonzero: command, usage, or infrastructure error
`

var (
	flakeFlags = flag.NewFlagSet("detect flake", flag.ContinueOnError)

	flakeTarget     = flakeFlags.String("target", "", "Bazel test target containing the flaky test.")
	flakeTestFilter = flakeFlags.String("test_filter", "", "Bazel test filter identifying the flaky test.")
	flakeRuns       = flakeFlags.Int("n", defaultFlakeRuns, "Maximum number of test runs for each reproduction strategy.")
	flakeAPITarget  = flakeFlags.String("buildbuddy_target", login.DefaultApiTarget, "BuildBuddy gRPC target used to fetch the invocation.")
)

// Options that identify an invocation, emit auxiliary files, or contain
// credentials cannot safely be copied from an invocation to a local replay.
var nonReplayableOptions = map[string]struct{}{
	"bes_header":                      {},
	"build_metadata":                  {},
	"build_event_binary_file":         {},
	"build_event_json_file":           {},
	"build_event_text_file":           {},
	"build_request_id":                {},
	"client_cwd":                      {},
	"execution_log_binary_file":       {},
	"execution_log_compact_file":      {},
	"execution_log_json_file":         {},
	"experimental_execution_log_file": {},
	"flaky_test_attempts":             {},
	"invocation_id":                   {},
	"isatty":                          {},
	"memory_profile":                  {},
	"profile":                         {},
	"remote_cache_header":             {},
	"remote_header":                   {},
	"runs_per_test":                   {},
	"test_filter":                     {},
	"terminal_columns":                {},
	"tool_invocation_id":              {},
}

var nonReplayableStartupOptions = map[string]struct{}{
	"install_base":      {},
	"install_md5":       {},
	"lock_install_base": {},
	"output_base":       {},
	"output_user_root":  {},
}

type flakeCommandRunner interface {
	Run(ctx context.Context, name string, args ...string) (int, error)
}

type osFlakeCommandRunner struct{}

func (osFlakeCommandRunner) Run(ctx context.Context, name string, args ...string) (int, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	cmd.Stdin = os.Stdin
	err := cmd.Run()
	if err == nil {
		return 0, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		exitCode := exitErr.ExitCode()
		// Keep passing and no-tests intermediary builds quiet. A reproduced
		// test failure or unexpected Bazel failure is terminal, so preserve its
		// output for diagnosis.
		if exitCode != bazelNoTestsExitCode {
			_, _ = os.Stdout.Write(output.Bytes())
		}
		return exitCode, nil
	}
	return -1, err
}

type replayCommand struct {
	startupOptions []string
	command        string
	commandOptions []string
	targets        []string
}

type flakeChecker struct {
	runner flakeCommandRunner
}

type flakeDetection struct {
	strategy string
	attempt  int
}

type flakeStrategy struct {
	name        string
	targets     []string
	testFilter  string
	runsPerTest int
}

func handleFlake(args []string) (int, error) {
	if err := arg.ParseFlagSet(flakeFlags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(flakeUsage)
			return 1, nil
		}
		return -1, err
	}
	if flakeFlags.NArg() != 1 {
		log.Print(flakeUsage)
		return -1, fmt.Errorf("expected exactly one invocation ID or URL")
	}
	if *flakeTarget == "" {
		return -1, fmt.Errorf("--target is required")
	}
	if *flakeTestFilter == "" {
		return -1, fmt.Errorf("--test_filter is required")
	}
	if *flakeRuns < 1 {
		return -1, fmt.Errorf("--n must be at least 1")
	}

	invocationID, err := parseFlakeInvocationID(flakeFlags.Arg(0))
	if err != nil {
		return -1, err
	}
	apiKey, err := login.GetAPIKey()
	if err != nil {
		return -1, fmt.Errorf("read BuildBuddy API key: %w", err)
	}
	if apiKey == "" {
		return -1, fmt.Errorf("not logged in; run `bb login` first")
	}

	conn, err := grpc_client.DialSimple(*flakeAPITarget)
	if err != nil {
		return -1, fmt.Errorf("connect to BuildBuddy: %w", err)
	}
	defer conn.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)

	bbClient := bbspb.NewBuildBuddyServiceClient(conn)
	response, err := bbClient.GetInvocation(ctx, &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{InvocationId: invocationID},
	})
	if err != nil {
		return -1, fmt.Errorf("fetch invocation %s: %w", invocationID, err)
	}
	if len(response.GetInvocation()) == 0 {
		return -1, fmt.Errorf("invocation %s not found", invocationID)
	}
	invocation := response.GetInvocation()[0]

	replay, skipped, err := replayCommandFromInvocation(invocation)
	if err != nil {
		return -1, err
	}
	if skipped > 0 {
		log.Warnf("Skipped %d redacted option(s) that cannot be replayed.", skipped)
	}

	bsClient := bspb.NewByteStreamClient(conn)
	return runFlakeDetectionOnOriginalRunner(
		ctx,
		bbClient,
		bsClient,
		invocation,
		replay,
		*flakeTarget,
		*flakeTestFilter,
		*flakeRuns,
		*flakeAPITarget,
	)
}

func runFlakeDetectionOnOriginalRunner(
	ctx context.Context,
	bbClient bbspb.BuildBuddyServiceClient,
	bsClient bspb.ByteStreamClient,
	invocation *inpb.Invocation,
	replay replayCommand,
	target, testFilter string,
	runs int,
	apiTarget string,
) (int, error) {
	parentInvocationID := runnerInvocationID(invocation)
	if parentInvocationID == "" {
		return -1, fmt.Errorf("invocation %s is not linked to a CI runner invocation", invocation.GetInvocationId())
	}
	parentResponse, err := bbClient.GetInvocation(ctx, &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{InvocationId: parentInvocationID},
	})
	if err != nil {
		return -1, fmt.Errorf("fetch runner invocation %s: %w", parentInvocationID, err)
	}
	if len(parentResponse.GetInvocation()) != 1 {
		return -1, fmt.Errorf("runner invocation %s not found", parentInvocationID)
	}
	instanceName := invocationOptionValue(parentResponse.GetInvocation()[0], "remote_instance_name")
	if instanceName == "" {
		instanceName = invocationOptionValue(invocation, "remote_instance_name")
	}
	if instanceName == "" {
		return -1, fmt.Errorf("invocation %s does not specify a remote instance name", invocation.GetInvocationId())
	}

	executionResponse, err := bbClient.GetExecution(ctx, &espb.GetExecutionRequest{
		ExecutionLookup: &espb.ExecutionLookup{InvocationId: parentInvocationID},
	})
	if err != nil {
		return -1, fmt.Errorf("fetch runner execution for invocation %s: %w", parentInvocationID, err)
	}
	if len(executionResponse.GetExecution()) != 1 {
		return -1, fmt.Errorf(
			"expected one runner execution for invocation %s, got %d",
			parentInvocationID,
			len(executionResponse.GetExecution()),
		)
	}
	actionDigest := executionResponse.GetExecution()[0].GetActionDigest()
	if actionDigest == nil {
		return -1, fmt.Errorf("runner execution for invocation %s has no action digest", parentInvocationID)
	}

	action := &repb.Action{}
	actionResource := digest.NewCASResourceName(actionDigest, instanceName, repb.DigestFunction_BLAKE3)
	if err := cachetools.GetBlobAsProto(ctx, bsClient, actionResource, action); err != nil {
		return -1, fmt.Errorf("fetch runner action for invocation %s: %w", parentInvocationID, err)
	}
	if action.GetCommandDigest() == nil {
		return -1, fmt.Errorf("runner action for invocation %s has no command digest", parentInvocationID)
	}

	command := &repb.Command{}
	commandResource := digest.NewCASResourceName(action.GetCommandDigest(), instanceName, repb.DigestFunction_BLAKE3)
	if err := cachetools.GetBlobAsProto(ctx, bsClient, commandResource, command); err != nil {
		return -1, fmt.Errorf("fetch runner command for invocation %s: %w", parentInvocationID, err)
	}
	if command.GetPlatform() == nil {
		return -1, fmt.Errorf("runner command for invocation %s has no platform properties", parentInvocationID)
	}

	commitSHA := commandArgumentValue(command.GetArguments(), "commit_sha")
	if commitSHA == "" {
		commitSHA = invocation.GetCommitSha()
	}
	if commitSHA == "" {
		return -1, fmt.Errorf("invocation %s does not identify the original commit", invocation.GetInvocationId())
	}
	repoURL := commandArgumentValue(command.GetArguments(), "pushed_repo_url")
	if repoURL == "" {
		repoURL = commandArgumentValue(command.GetArguments(), "target_repo_url")
	}
	if repoURL == "" {
		repoURL = invocation.GetRepoUrl()
	}
	if repoURL == "" {
		return -1, fmt.Errorf("invocation %s does not identify the original repository", invocation.GetInvocationId())
	}
	branch := commandArgumentValue(command.GetArguments(), "pushed_branch")
	if branch == "" {
		branch = invocation.GetBranchName()
	}

	strategies := flakeStrategies(replay, target, testFilter, runs)
	request := &rnpb.RunRequest{
		Name: "Detect flake in " + target,
		GitRepo: &gitpb.GitRepo{
			RepoUrl: repoURL,
		},
		RepoState: &gitpb.RepoState{
			CommitSha: commitSHA,
			Branch:    branch,
		},
		Steps:          remoteFlakeSteps(replay, strategies),
		ExecProperties: clonePlatformProperties(command.GetPlatform().GetProperties()),
		RemoteHeaders: []string{
			"x-buildbuddy-platform.recycle-runner=false",
		},
		WaitUntil: rnpb.WaitCondition_STARTED,
	}

	log.Printf(
		"Recreating runner type from invocation %s at commit %s with recycling disabled.",
		parentInvocationID,
		commitSHA,
	)
	runResponse, err := bbClient.Run(ctx, request)
	if err != nil {
		return -1, fmt.Errorf("run flake detector on original runner type: %w", err)
	}
	log.Printf("Remote detector invocation: https://app.buildbuddy.io/invocation/%s", runResponse.GetInvocationId())
	log.Printf("\nTesting remotely with %s (--runs_per_test=%d)...", strategies[0].name, runs)

	streamConn, err := grpc_client.DialSimple(apiTarget)
	if err != nil {
		return -1, fmt.Errorf("connect to stream remote detector logs: %w", err)
	}
	if err := remotebazel.StreamLogs(
		ctx,
		bbspb.NewBuildBuddyServiceClient(streamConn),
		runResponse.GetInvocationId(),
	); err != nil {
		streamConn.Close()
		return -1, fmt.Errorf("stream remote detector logs: %w", err)
	}
	streamConn.Close()

	resultConn, err := grpc_client.DialSimple(apiTarget)
	if err != nil {
		return -1, fmt.Errorf("connect to fetch remote detector result: %w", err)
	}
	defer resultConn.Close()
	result, err := waitForInvocationCompletion(
		ctx,
		bbspb.NewBuildBuddyServiceClient(resultConn),
		runResponse.GetInvocationId(),
	)
	if err != nil {
		return -1, fmt.Errorf("fetch remote detector result: %w", err)
	}
	exitCode, ok := remoteRunnerExitCode(result)
	if !ok {
		return -1, fmt.Errorf("remote detector invocation %s has no completed step", runResponse.GetInvocationId())
	}
	switch exitCode {
	case 0:
		log.Printf("\n\033[32mFlake was not reproduced after all strategies.\033[0m")
	case exitCodeFlakeDetected:
		completedSteps := remoteRunnerExitCodes(result)
		if len(completedSteps) <= len(strategies) {
			log.Printf("\n\033[31mFlake reproduced using %s.\033[0m", strategies[len(completedSteps)-1].name)
		} else {
			log.Printf("\n\033[31mFlake reproduced.\033[0m")
		}
	}
	return exitCode, nil
}

func waitForInvocationCompletion(
	ctx context.Context,
	bbClient bbspb.BuildBuddyServiceClient,
	invocationID string,
) (*inpb.Invocation, error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		response, err := bbClient.GetInvocation(ctx, &inpb.GetInvocationRequest{
			Lookup: &inpb.InvocationLookup{InvocationId: invocationID},
		})
		if err != nil {
			if !status.IsUnavailableError(err) {
				return nil, err
			}
		} else if len(response.GetInvocation()) == 1 {
			invocation := response.GetInvocation()[0]
			if invocation.GetInvocationStatus() == inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS ||
				invocation.GetInvocationStatus() == inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS {
				return invocation, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func runnerInvocationID(invocation *inpb.Invocation) string {
	if invocation.GetParentInvocationId() != "" {
		return invocation.GetParentInvocationId()
	}
	const key = "PARENT_INVOCATION_ID="
	for _, commandLine := range invocation.GetStructuredCommandLine() {
		for _, section := range commandLine.GetSections() {
			for _, option := range section.GetOptionList().GetOption() {
				if option.GetOptionName() != "build_metadata" {
					continue
				}
				if value, ok := strings.CutPrefix(option.GetOptionValue(), key); ok {
					return value
				}
			}
		}
	}
	return ""
}

func invocationOptionValue(invocation *inpb.Invocation, optionName string) string {
	commandLine := findCommandLine(invocation.GetStructuredCommandLine(), "canonical")
	if commandLine == nil {
		commandLine = findCommandLine(invocation.GetStructuredCommandLine(), "original")
	}
	if commandLine == nil {
		return ""
	}
	value := ""
	for _, section := range commandLine.GetSections() {
		for _, option := range section.GetOptionList().GetOption() {
			if option.GetOptionName() == optionName {
				value = option.GetOptionValue()
			}
		}
	}
	return value
}

func clonePlatformProperties(properties []*repb.Platform_Property) []*repb.Platform_Property {
	cloned := make([]*repb.Platform_Property, 0, len(properties))
	for _, property := range properties {
		cloned = append(cloned, &repb.Platform_Property{
			Name:  property.GetName(),
			Value: property.GetValue(),
		})
	}
	return cloned
}

func commandArgumentValue(arguments []string, name string) string {
	prefix := "--" + name + "="
	value := ""
	for i, argument := range arguments {
		if v, ok := strings.CutPrefix(argument, prefix); ok {
			value = v
			continue
		}
		if argument == "--"+name && i+1 < len(arguments) {
			value = arguments[i+1]
		}
	}
	return value
}

func remoteRunnerExitCode(invocation *inpb.Invocation) (int, bool) {
	exitCodes := remoteRunnerExitCodes(invocation)
	if len(exitCodes) == 0 {
		return 0, false
	}
	return exitCodes[len(exitCodes)-1], true
}

func remoteRunnerExitCodes(invocation *inpb.Invocation) []int {
	var exitCodes []int
	for _, event := range invocation.GetEvent() {
		completed := event.GetBuildEvent().GetRemoteRunnerStepCompleted()
		if completed != nil {
			exitCodes = append(exitCodes, int(completed.GetExitCode()))
		}
	}
	return exitCodes
}

func remoteFlakeSteps(replay replayCommand, strategies []flakeStrategy) []*rnpb.Step {
	steps := make([]*rnpb.Step, 0, len(strategies))
	for i, strategy := range strategies {
		successMessage := "Flake was not reproduced after all strategies."
		if i+1 < len(strategies) {
			successMessage = fmt.Sprintf(
				"Flake not reproduced with %s; escalating to %s (--runs_per_test=%d).",
				strategy.name,
				strategies[i+1].name,
				strategies[i+1].runsPerTest,
			)
		}
		failureMessage := fmt.Sprintf("Flake reproduced using %s.", strategy.name)
		command := remoteBazelCommand(replay.args(strategy.targets, strategy.testFilter, strategy.runsPerTest))
		steps = append(steps, &rnpb.Step{
			Run: remoteFlakeStep(command, successMessage, failureMessage),
		})
	}
	return steps
}

func remoteBazelCommand(args []string) string {
	separatorIndex := slices.Index(args, "--")
	if separatorIndex < 0 {
		separatorIndex = len(args)
	}
	beforeTargets := append([]string{"bazel"}, args[:separatorIndex]...)
	command := shlex.Quote(beforeTargets...)
	// Credential flags are redacted from stored structured command lines. The
	// hosted runner injects this environment variable; expand it remotely so
	// the API key never appears in the RunRequest or its printed command.
	command += ` --remote_header="x-buildbuddy-api-key=${BUILDBUDDY_API_KEY}"`
	command += ` --bes_header="x-buildbuddy-api-key=${BUILDBUDDY_API_KEY}"`
	if separatorIndex < len(args) {
		command += " " + shlex.Quote(args[separatorIndex:]...)
	}
	return command
}

func remoteFlakeStep(command, successMessage, failureMessage string) string {
	return fmt.Sprintf(`set +e
%s
flake_exit_code="$?"
set -e
case "$flake_exit_code" in
  0|%d)
    printf '%%s\n' %s
    ;;
  %d)
    printf '%%s\n' %s
    exit %d
    ;;
  *)
    exit "$flake_exit_code"
    ;;
esac`,
		command,
		bazelNoTestsExitCode,
		shlex.Quote(successMessage),
		bazelTestFailureExitCode,
		shlex.Quote(failureMessage),
		exitCodeFlakeDetected,
	)
}

func parseFlakeInvocationID(value string) (string, error) {
	matches := uuid.Pattern.FindStringSubmatch(strings.TrimSuffix(value, "/"))
	if len(matches) != 2 {
		return "", fmt.Errorf("invalid invocation ID or URL %q", value)
	}
	return matches[1], nil
}

func replayCommandFromInvocation(invocation *inpb.Invocation) (replayCommand, int, error) {
	canonical := findCommandLine(invocation.GetStructuredCommandLine(), "canonical")
	original := findCommandLine(invocation.GetStructuredCommandLine(), "original")
	commandLine := canonical
	if commandLine == nil {
		commandLine = original
	}
	if commandLine == nil {
		return replayCommand{}, 0, fmt.Errorf("invocation %s has no structured command line", invocation.GetInvocationId())
	}

	replay := replayCommand{command: invocation.GetCommand()}
	skippedRedacted := 0
	// Canonical startup sections include Bazel implementation details such as
	// --install_md5 and --lock_install_base, which Bazel reports but does not
	// accept as command-line flags. The original startup section contains only
	// user-accepted syntax.
	if original != nil {
		for _, section := range original.GetSections() {
			if !strings.Contains(strings.ToLower(section.GetSectionLabel()), "startup option") {
				continue
			}
			var rcOptions []string
			if canonical != nil {
				rcOptions = []string{"bazelrc", "home_rc", "ignore_all_rc_files", "master_bazelrc", "system_rc", "workspace_rc"}
			}
			options, skipped := replayableOptions(section.GetOptionList().GetOption(), nonReplayableStartupOptions, rcOptions...)
			replay.startupOptions = append(replay.startupOptions, options...)
			skippedRedacted += skipped
		}
	}
	// Canonical command options already contain the effective rc/config
	// expansions. Disable rc loading so they are not applied a second time.
	if canonical != nil {
		replay.startupOptions = append(replay.startupOptions, "--ignore_all_rc_files")
	}

	for _, section := range commandLine.GetSections() {
		label := strings.ToLower(section.GetSectionLabel())
		switch {
		case strings.Contains(label, "command option"):
			var expansionOptions []string
			if canonical != nil {
				expansionOptions = []string{"config"}
			}
			options, skipped := replayableOptions(section.GetOptionList().GetOption(), nonReplayableOptions, expansionOptions...)
			replay.commandOptions = append(replay.commandOptions, options...)
			skippedRedacted += skipped
		case label == "command":
			if chunks := section.GetChunkList().GetChunk(); len(chunks) > 0 {
				replay.command = chunks[0]
			}
		case label == "residual" || label == "arguments":
			replay.targets = append(replay.targets, section.GetChunkList().GetChunk()...)
		}
	}

	if replay.command != "test" && replay.command != "coverage" {
		return replayCommand{}, skippedRedacted, fmt.Errorf("invocation %s ran Bazel command %q, expected test or coverage", invocation.GetInvocationId(), replay.command)
	}
	if len(replay.targets) == 0 {
		replay.targets = append(replay.targets, invocation.GetPattern()...)
	}
	replay.targets = trimTargetSeparator(replay.targets)
	if len(replay.targets) == 0 {
		return replayCommand{}, skippedRedacted, fmt.Errorf("invocation %s has no test targets", invocation.GetInvocationId())
	}
	return replay, skippedRedacted, nil
}

func trimTargetSeparator(targets []string) []string {
	if len(targets) > 0 && targets[0] == "--" {
		return targets[1:]
	}
	return targets
}

func findCommandLine(commandLines []*clpb.CommandLine, label string) *clpb.CommandLine {
	for _, commandLine := range commandLines {
		if commandLine.GetCommandLineLabel() == label {
			return commandLine
		}
	}
	return nil
}

func replayableOptions(options []*clpb.Option, excluded map[string]struct{}, additionallyExcluded ...string) ([]string, int) {
	var args []string
	skippedRedacted := 0
	for _, option := range options {
		combined := option.GetCombinedForm()
		if strings.Contains(combined, "<REDACTED>") {
			skippedRedacted++
			continue
		}
		if _, ok := excluded[option.GetOptionName()]; ok {
			continue
		}
		if slices.Contains(additionallyExcluded, option.GetOptionName()) {
			continue
		}
		parts, err := shlex.Split(combined)
		if err != nil || len(parts) == 0 {
			// Canonical structured command lines normally use one combined
			// token. Fall back to an unambiguous --name=value representation.
			if option.GetOptionName() == "" {
				continue
			}
			parts = []string{"--" + option.GetOptionName() + "=" + option.GetOptionValue()}
		}
		args = append(args, parts...)
	}
	return args, skippedRedacted
}

func (c *flakeChecker) Run(ctx context.Context, replay replayCommand, target, testFilter string, runs int) (*flakeDetection, error) {
	strategies := flakeStrategies(replay, target, testFilter, runs)

	for i, strategy := range strategies {
		if i == 0 {
			log.Printf("\nTesting with %s (--runs_per_test=%d)...", strategy.name, runs)
		} else {
			log.Printf(
				"\nFlake not reproduced with %s; escalating to %s (--runs_per_test=%d)...",
				strategies[i-1].name,
				strategy.name,
				runs,
			)
		}
		args := replay.args(strategy.targets, strategy.testFilter, strategy.runsPerTest)
		exitCode, err := c.runner.Run(ctx, "bazel", args...)
		if err != nil {
			return nil, fmt.Errorf("run Bazel using %s: %w", strategy.name, err)
		}
		switch exitCode {
		case 0, bazelNoTestsExitCode:
			continue
		case bazelTestFailureExitCode:
			return &flakeDetection{strategy: strategy.name, attempt: 1}, nil
		default:
			return nil, fmt.Errorf("Bazel exited with code %d while trying %s", exitCode, strategy.name)
		}
	}
	return nil, nil
}

func flakeStrategies(replay replayCommand, target, testFilter string, runs int) []flakeStrategy {
	return []flakeStrategy{
		{
			name:        "the target and test filter",
			targets:     []string{target},
			testFilter:  testFilter,
			runsPerTest: runs,
		},
		{
			name:        "the target without a test filter",
			targets:     []string{target},
			runsPerTest: runs,
		},
		{
			name:        "the full original command",
			targets:     replay.targets,
			runsPerTest: runs,
		},
	}
}

func (r replayCommand) args(targets []string, testFilter string, runsPerTest int) []string {
	args := append([]string(nil), r.startupOptions...)
	args = append(args, r.command)
	args = append(args, r.commandOptions...)
	// A separate invocation must execute the test again instead of accepting a
	// cached pass from an earlier attempt. Disable Bazel's own flaky-test
	// retries so a failing attempt is observable by this detector.
	args = append(args, "--nocache_test_results", "--flaky_test_attempts=1")
	if testFilter != "" {
		args = append(args, "--test_filter="+testFilter)
	}
	if runsPerTest > 0 {
		args = append(args, "--runs_per_test="+strconv.Itoa(runsPerTest))
	}
	args = append(args, "--")
	args = append(args, targets...)
	return args
}
