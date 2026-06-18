package detect

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	spawn_diff "github.com/buildbuddy-io/buildbuddy/proto/spawn_diff"
)

func init() {
	parser.SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
}

func TestAddBazelFlags(t *testing.T) {
	args, err := addBazelFlags(
		bazelArgsForTest(t, "--bazelrc=/tmp/bazelrc", "test", "//foo:bar"),
		"/tmp/output-base",
		"/tmp/log.pb.zst",
		"grpcs://bes.example.com",
		"https://example.com/invocation/",
	)
	require.NoError(t, err)

	assert.Equal(t, []string{
		"--bazelrc=/tmp/bazelrc",
		"--output_base=/tmp/output-base",
		"test",
		"--bes_backend=grpcs://bes.example.com",
		"--bes_results_url=https://example.com/invocation/",
		"--noremote_accept_cached",
		"--repo_contents_cache=",
		"--disk_cache=",
		"--noexperimental_convenience_symlinks",
		"--execution_log_compact_file=/tmp/log.pb.zst",
		"//foo:bar",
	}, args)
}

func TestAddBazelFlags_DoesNotMutateBaseArgs(t *testing.T) {
	baseArgs := bazelArgsForTest(t, "build", "//foo:bar")

	_, err := addBazelFlags(baseArgs, "/tmp/output-base", "/tmp/log.pb.zst", defaultBESBackend, defaultBESResultsURL)
	require.NoError(t, err)

	assert.Equal(t, []string{"build", "//foo:bar"}, baseArgs.Forwarded())
}

func TestParseBazelCommand(t *testing.T) {
	bazelArgs, err := parseBazelCommand(`--bazelrc=/tmp/bazelrc test //foo:bar --test_output=errors`)
	require.NoError(t, err)
	assert.Equal(t, []string{"--bazelrc=/tmp/bazelrc", "test", "--test_output=errors", "//foo:bar"}, bazelArgs.Forwarded())

	_, err = parseBazelCommand("//foo:bar")
	require.Error(t, err)
}

func TestRunReturnsDetectionError(t *testing.T) {
	diff := &spawn_diff.DiffResult{SpawnDiffs: []*spawn_diff.SpawnDiff{nondeterministicSpawnDiff("//foo:bar")}}
	explainer := &fakeExplainer{diff: diff}
	runner := &fakeRunner{}
	c := &checker{
		opts: options{
			bazelArgs:     bazelArgsForTest(t, "build", "//foo:bar"),
			besBackend:    defaultBESBackend,
			besResultsURL: defaultBESResultsURL,
		},
		runner:    runner,
		explainer: explainer,
	}

	err := c.Run(context.Background())
	require.ErrorIs(t, err, errNondeterminismDetected)

	require.Equal(t, []string{"build", "shutdown", "clean", "build", "shutdown", "clean"}, bazelCommands(runner.runs))
	assert.Equal(t, 1, explainer.writeCalls)
	assert.Same(t, diff, explainer.wroteDiff)
}

// nondeterministicSpawnDiff returns a spawn diff that the detector should treat
// as non-determinism: an exit code change despite unchanged inputs.
func nondeterministicSpawnDiff(label string) *spawn_diff.SpawnDiff {
	return &spawn_diff.SpawnDiff{
		TargetLabel: label,
		Diff: &spawn_diff.SpawnDiff_Modified{Modified: &spawn_diff.Modified{
			Diffs: []*spawn_diff.Diff{{
				Diff: &spawn_diff.Diff_ExitCode{ExitCode: &spawn_diff.IntDiff{Old: 0, New: 1}},
			}},
		}},
	}
}

func TestRunIgnoresDeterministicDiffs(t *testing.T) {
	// A spawn whose inputs changed is a downstream effect, not non-determinism.
	diff := &spawn_diff.DiffResult{SpawnDiffs: []*spawn_diff.SpawnDiff{{
		TargetLabel: "//foo:bar",
		Diff: &spawn_diff.SpawnDiff_Modified{Modified: &spawn_diff.Modified{
			Diffs: []*spawn_diff.Diff{{
				Diff: &spawn_diff.Diff_InputContents{InputContents: &spawn_diff.FileSetDiff{}},
			}},
		}},
	}}}
	explainer := &fakeExplainer{diff: diff}
	runner := &fakeRunner{}
	c := &checker{
		opts: options{
			bazelArgs:     bazelArgsForTest(t, "build", "//foo:bar"),
			besBackend:    defaultBESBackend,
			besResultsURL: defaultBESResultsURL,
		},
		runner:    runner,
		explainer: explainer,
	}

	require.NoError(t, c.Run(context.Background()))

	require.Len(t, runner.runs, 2)
	assert.Equal(t, 0, explainer.writeCalls)
}

func TestRunReturnsNilWhenNoDiffs(t *testing.T) {
	explainer := &fakeExplainer{diff: &spawn_diff.DiffResult{}}
	runner := &fakeRunner{}
	c := &checker{
		opts: options{
			bazelArgs:     bazelArgsForTest(t, "build", "//foo:bar"),
			besBackend:    defaultBESBackend,
			besResultsURL: defaultBESResultsURL,
		},
		runner:    runner,
		explainer: explainer,
	}

	require.NoError(t, c.Run(context.Background()))

	require.Equal(t, []string{"build", "shutdown", "clean", "build", "shutdown", "clean"}, bazelCommands(runner.runs))
	assert.Equal(t, 0, explainer.writeCalls)
}

func TestRemovesOutputBaseAfterEachRun(t *testing.T) {
	a, err := newArtifacts()
	require.NoError(t, err)
	defer os.RemoveAll(a.tempDir)

	var runner fakeRunner
	var buildRuns int
	runner.onRun = func(ctx context.Context, call commandCall) error {
		command, _ := parser.GetBazelCommandAndIndex(call.args)
		if command == "shutdown" {
			return nil
		}
		outputBase := outputBaseFromArgs(t, call.args)
		if command == "clean" {
			return os.RemoveAll(outputBase)
		}

		buildRuns++
		require.NoError(t, os.MkdirAll(filepath.Join(outputBase, "execroot"), 0755))
		if buildRuns == 2 {
			require.NoDirExists(t, filepath.Join(a.tempDir, "output_base_1"))
		}
		return nil
	}
	c := &checker{
		opts: options{
			bazelArgs:     bazelArgsForTest(t, "build", "//foo:bar"),
			besBackend:    defaultBESBackend,
			besResultsURL: defaultBESResultsURL,
		},
		runner: &runner,
	}

	require.NoError(t, c.runBuilds(context.Background(), a))

	require.Equal(t, []string{"build", "shutdown", "clean", "build", "shutdown", "clean"}, bazelCommands(runner.runs))
	require.NoDirExists(t, filepath.Join(a.tempDir, "output_base_1"))
	require.NoDirExists(t, filepath.Join(a.tempDir, "output_base_2"))
}

func bazelArgsForTest(t *testing.T, args ...string) *arg.BazelArgs {
	t.Helper()
	parsedArgs, err := arg.NewBazelArgsNoResolve(args)
	require.NoError(t, err)
	return parsedArgs
}

func outputBaseFromArgs(t *testing.T, args []string) string {
	t.Helper()
	for _, arg := range args {
		if value, ok := strings.CutPrefix(arg, "--output_base="); ok {
			return value
		}
	}
	require.FailNow(t, "missing --output_base arg", "args: %v", args)
	return ""
}

func bazelCommands(calls []commandCall) []string {
	var commands []string
	for _, call := range calls {
		command, _ := parser.GetBazelCommandAndIndex(call.args)
		commands = append(commands, command)
	}
	return commands
}

type commandCall struct {
	name string
	args []string
}

type fakeRunner struct {
	runs   []commandCall
	runErr error
	onRun  func(context.Context, commandCall) error
}

func (r *fakeRunner) Run(ctx context.Context, name string, args ...string) error {
	call := commandCall{name: name, args: append([]string(nil), args...)}
	r.runs = append(r.runs, call)
	if r.onRun != nil {
		if err := r.onRun(ctx, call); err != nil {
			return err
		}
	}
	return r.runErr
}

type fakeExplainer struct {
	diff       *spawn_diff.DiffResult
	diffErr    error
	writeCalls int
	wroteDiff  *spawn_diff.DiffResult
}

func (e *fakeExplainer) Diff(oldLog, newLog string) (*spawn_diff.DiffResult, error) {
	return e.diff, e.diffErr
}

func (e *fakeExplainer) WriteText(w io.Writer, diff *spawn_diff.DiffResult, verbose bool) {
	e.writeCalls++
	e.wroteDiff = diff
}
