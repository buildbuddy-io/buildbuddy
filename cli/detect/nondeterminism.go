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

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/explain"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/bazelrc"
	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"

	sdpb "github.com/buildbuddy-io/buildbuddy/proto/spawn_diff"
)

const (
	defaultBazelCommand            = "build //..."
	defaultBESBackend              = "remote.buildbuddy.io"
	defaultBESResultsURL           = "https://app.buildbuddy.io/invocation/"
	exitCodeNondeterminismDetected = 10
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
)

var errNondeterminismDetected = errors.New("nondeterminism detected")

type commandRunner interface {
	Run(ctx context.Context, name string, args ...string) error
}

type explainer interface {
	Diff(oldLog, newLog string) (*sdpb.DiffResult, error)
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

type artifacts struct {
	tempDir string
	oldLog  string
	newLog  string
}

type explainRunner struct{}

func (explainRunner) Diff(oldLog, newLog string) (*sdpb.DiffResult, error) {
	return explain.Diff(oldLog, newLog)
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
	a, err := newArtifacts()
	if err != nil {
		return err
	}
	defer os.RemoveAll(a.tempDir)
	log.Printf("Writing nondeterminism check files under %s", a.tempDir)

	buildErr := c.runBuilds(ctx, a)
	if buildErr != nil {
		if errors.Is(buildErr, context.Canceled) {
			return buildErr
		}
		log.Warnf("One or both uncached builds failed; continuing with bb explain: %s", buildErr)
	}

	diff, explainErr := c.runExplainDiff(ctx, a)
	if explainErr != nil {
		return errors.Join(buildErr, explainErr)
	}
	// Both builds run the same command on the same sources, so only spawns whose
	// outputs or exit code changed despite unchanged inputs are genuinely
	// non-deterministic. Other diffs (e.g. changed inputs) are downstream effects
	// whose root cause is itself reported as a non-deterministic spawn.
	diff.SpawnDiffs = explain.FilterNondeterministicSpawns(diff.GetSpawnDiffs())
	if len(diff.GetSpawnDiffs()) == 0 {
		log.Print("\n\n\033[32mNo nondeterminism detected.\033[0m\n\n")
		return buildErr
	}

	log.Print("\n\n\033[31mNondeterminism detected.\033[0m\n\n")
	if err := c.printExplainText(ctx, diff); err != nil {
		log.Warnf("Failed to print text bb explain output: %s", err)
	}
	return errors.Join(buildErr, errNondeterminismDetected)
}

func newArtifacts() (*artifacts, error) {
	dir, err := os.MkdirTemp("", "nondeterminism-check-*")
	if err != nil {
		return nil, err
	}
	return &artifacts{
		tempDir: dir,
		oldLog:  filepath.Join(dir, "build_1_compact_exec_log.pb.zst"),
		newLog:  filepath.Join(dir, "build_2_compact_exec_log.pb.zst"),
	}, nil
}

func (c *checker) runBuilds(ctx context.Context, a *artifacts) error {
	var buildErr error
	for i, build := range []struct {
		outputBase string
		logPath    string
	}{
		{outputBase: filepath.Join(a.tempDir, "output_base_1"), logPath: a.oldLog},
		{outputBase: filepath.Join(a.tempDir, "output_base_2"), logPath: a.newLog},
	} {
		if err := c.runBuild(ctx, i+1, build.outputBase, build.logPath); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			buildErr = errors.Join(buildErr, err)
		}
	}
	return buildErr
}

func (c *checker) runBuild(ctx context.Context, buildNumber int, outputBase, compactLogPath string) error {
	args, err := addBazelFlags(c.opts.bazelArgs, outputBase, compactLogPath, c.opts.besBackend, c.opts.besResultsURL)
	if err != nil {
		return err
	}
	runErr := c.runner.Run(ctx, "bazel", args...)
	cleanupErr := removeOutputBase(outputBase)
	if cleanupErr != nil {
		log.Warnf("Failed to remove Bazel output base %s: %s", outputBase, cleanupErr)
	}
	if runErr != nil {
		return fmt.Errorf("uncached build %d: %w", buildNumber, runErr)
	}
	return nil
}

func removeOutputBase(outputBase string) error {
	log.Printf("Removing Bazel output base %s", outputBase)
	if err := os.RemoveAll(outputBase); err != nil {
		return err
	}
	return nil
}

func addBazelFlags(baseArgs *arg.BazelArgs, outputBase, compactLogPath, besBackend, besResultsURL string) ([]string, error) {
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
		"--execution_log_compact_file=" + compactLogPath,
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

func (c *checker) runExplainDiff(ctx context.Context, a *artifacts) (*sdpb.DiffResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	diff, err := c.explainer.Diff(a.oldLog, a.newLog)
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
