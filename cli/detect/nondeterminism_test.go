package detect

import (
	"context"
	"io"
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
	diff := &spawn_diff.DiffResult{SpawnDiffs: []*spawn_diff.SpawnDiff{{TargetLabel: "//foo:bar"}}}
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

	require.Len(t, runner.runs, 2)
	assert.Equal(t, 1, explainer.writeCalls)
	assert.Same(t, diff, explainer.wroteDiff)
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

	require.Len(t, runner.runs, 2)
	assert.Equal(t, 0, explainer.writeCalls)
}

func bazelArgsForTest(t *testing.T, args ...string) *arg.BazelArgs {
	t.Helper()
	parsedArgs, err := arg.NewBazelArgsNoResolve(args)
	require.NoError(t, err)
	return parsedArgs
}

type commandCall struct {
	name string
	args []string
}

type fakeRunner struct {
	runs   []commandCall
	runErr error
}

func (r *fakeRunner) Run(ctx context.Context, name string, args ...string) error {
	r.runs = append(r.runs, commandCall{name: name, args: append([]string(nil), args...)})
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
