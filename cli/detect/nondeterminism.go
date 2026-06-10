package detect

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/bazelrc"
	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"
	"google.golang.org/protobuf/proto"

	spawn_diff "github.com/buildbuddy-io/buildbuddy/proto/spawn_diff"
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
	Output(ctx context.Context, name string, args ...string) ([]byte, error)
}

type osCommandRunner struct{}

func (r osCommandRunner) Run(ctx context.Context, name string, args ...string) error {
	return r.run(ctx, name, args, os.Stdout, os.Stderr)
}

func (r osCommandRunner) Output(ctx context.Context, name string, args ...string) ([]byte, error) {
	var stdout, stderr bytes.Buffer
	if err := r.run(ctx, name, args, &stdout, io.MultiWriter(os.Stderr, &stderr)); err != nil {
		return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

func (r osCommandRunner) run(ctx context.Context, name string, args []string, stdout, stderr io.Writer) error {
	log.Printf("Running %s", shlex.Quote(append([]string{name}, args...)...))
	if name == "bazel" {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Use the bazelisk binary embedded in the bb binary to avoid having to download it.
		exitCode, err := bazelisk.Run(args, &bazelisk.RunOpts{Stdout: stdout, Stderr: stderr})
		if err != nil {
			return err
		}
		if exitCode != 0 {
			return fmt.Errorf("bazel exited with code %d", exitCode)
		}
		return nil
	}
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Stdin = os.Stdin
	return cmd.Run()
}

type options struct {
	bazelArgs     *arg.BazelArgs
	besBackend    string
	besResultsURL string
}

type checker struct {
	opts   options
	runner commandRunner
}

type artifacts struct {
	tempDir      string
	oldLog       string
	newLog       string
	explainProto string
	explainText  string
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
		opts:   opts,
		runner: osCommandRunner{},
	}
	if err := c.Run(context.Background()); err != nil {
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

	buildErr := errors.Join(
		c.runBuild(ctx, 1, filepath.Join(a.tempDir, "output_base_1"), a.oldLog),
		c.runBuild(ctx, 2, filepath.Join(a.tempDir, "output_base_2"), a.newLog),
	)
	if buildErr != nil {
		log.Warnf("One or both uncached builds failed; continuing with bb explain: %s", buildErr)
	}

	diff, explainErr := c.runExplainProto(ctx, a)
	if explainErr != nil {
		return errors.Join(buildErr, explainErr)
	}
	if len(diff.GetSpawnDiffs()) == 0 {
		log.Print("\n\n\033[32mNo nondeterminism detected.\033[0m\n\n")
		return buildErr
	}

	log.Print("\n\n\033[31mNondeterminism detected.\033[0m\n\n")
	if err := c.writeExplainText(ctx, a); err != nil {
		log.Warnf("Failed to write text bb explain artifact: %s", err)
	}
	return errors.Join(buildErr, errNondeterminismDetected)
}

func newArtifacts() (*artifacts, error) {
	dir, err := os.MkdirTemp("", "nondeterminism-check-*")
	if err != nil {
		return nil, err
	}
	return &artifacts{
		tempDir:      dir,
		oldLog:       filepath.Join(dir, "build_1_compact_exec_log.pb.zst"),
		newLog:       filepath.Join(dir, "build_2_compact_exec_log.pb.zst"),
		explainProto: filepath.Join(dir, "bb_explain.pb"),
		explainText:  filepath.Join(dir, "bb_explain.txt"),
	}, nil
}

func (c *checker) runBuild(ctx context.Context, buildNumber int, outputBase, compactLogPath string) error {
	args, err := addBazelFlags(c.opts.bazelArgs, outputBase, compactLogPath, c.opts.besBackend, c.opts.besResultsURL)
	if err != nil {
		return err
	}
	if err := c.runner.Run(ctx, "bazel", args...); err != nil {
		return fmt.Errorf("uncached build %d: %w", buildNumber, err)
	}
	return nil
}

// addBazelFlags adds the necessary Bazel flags to the command line.
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

func (c *checker) runExplainProto(ctx context.Context, a *artifacts) (*spawn_diff.DiffResult, error) {
	args := []string{
		"explain",
		"--old=" + a.oldLog,
		"--new=" + a.newLog,
		"--output_format=proto",
	}
	out, err := c.runner.Output(ctx, "bb", args...)
	if err != nil {
		return nil, fmt.Errorf("run bb explain: %w", err)
	}
	if err := os.WriteFile(a.explainProto, out, 0644); err != nil {
		return nil, fmt.Errorf("write bb explain proto: %w", err)
	}
	diff := &spawn_diff.DiffResult{}
	if err := proto.Unmarshal(out, diff); err != nil {
		return nil, fmt.Errorf("parse bb explain proto: %w", err)
	}
	return diff, nil
}

func (c *checker) writeExplainText(ctx context.Context, a *artifacts) error {
	args := []string{
		"explain",
		"--old=" + a.oldLog,
		"--new=" + a.newLog,
		"--verbose",
	}
	out, err := c.runner.Output(ctx, "bb", args...)
	if err != nil {
		return fmt.Errorf("run text bb explain: %w", err)
	}
	fmt.Printf("bb explain output:\n%s", string(out))
	return os.WriteFile(a.explainText, out, 0644)
}
