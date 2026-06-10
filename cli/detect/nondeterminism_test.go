package detect

import (
	"context"
	"errors"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

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
	bazelArgs, err := parseBazelCommand("")
	require.NoError(t, err)
	assert.Equal(t, []string{"build", "//..."}, bazelArgs.Forwarded())

	bazelArgs, err = parseBazelCommand(`--bazelrc=/tmp/bazelrc test //foo:bar --test_output=errors`)
	require.NoError(t, err)
	assert.Equal(t, []string{"--bazelrc=/tmp/bazelrc", "test", "--test_output=errors", "//foo:bar"}, bazelArgs.Forwarded())

	_, err = parseBazelCommand("//foo:bar")
	require.Error(t, err)
}

func TestRunReturnsDetectionError(t *testing.T) {
	diff := &spawn_diff.DiffResult{SpawnDiffs: []*spawn_diff.SpawnDiff{{TargetLabel: "//foo:bar"}}}
	diffBytes, err := proto.Marshal(diff)
	require.NoError(t, err)
	runner := &fakeRunner{outputs: [][]byte{diffBytes, []byte("text diff")}}
	c := &checker{
		opts: options{
			bazelArgs:     bazelArgsForTest(t, "build", "//foo:bar"),
			besBackend:    defaultBESBackend,
			besResultsURL: defaultBESResultsURL,
		},
		runner: runner,
	}

	err = c.Run(context.Background())
	require.ErrorIs(t, err, errNondeterminismDetected)

	require.Len(t, runner.runs, 2)
	require.Len(t, runner.outputCalls, 2)
	assert.Contains(t, runner.outputCalls[0].args, "--output_format=proto")
	assert.Contains(t, runner.outputCalls[1].args, "--verbose")
}

func TestRunReturnsNilWhenNoDiffs(t *testing.T) {
	diffBytes, err := proto.Marshal(&spawn_diff.DiffResult{})
	require.NoError(t, err)
	runner := &fakeRunner{outputs: [][]byte{diffBytes}}
	c := &checker{
		opts: options{
			bazelArgs:     bazelArgsForTest(t, "build", "//foo:bar"),
			besBackend:    defaultBESBackend,
			besResultsURL: defaultBESResultsURL,
		},
		runner: runner,
	}

	require.NoError(t, c.Run(context.Background()))

	require.Len(t, runner.outputCalls, 1)
}

func bazelArgsForTest(t *testing.T, args ...string) *arg.BazelArgs {
	t.Helper()
	bazelArgs, err := arg.NewBazelArgsNoResolve(args)
	require.NoError(t, err)
	return bazelArgs
}

type commandCall struct {
	name string
	args []string
}

type fakeRunner struct {
	runs        []commandCall
	outputCalls []commandCall
	outputs     [][]byte
	runErr      error
	outputErr   error
}

func (r *fakeRunner) Run(ctx context.Context, name string, args ...string) error {
	r.runs = append(r.runs, commandCall{name: name, args: append([]string(nil), args...)})
	return r.runErr
}

func (r *fakeRunner) Output(ctx context.Context, name string, args ...string) ([]byte, error) {
	r.outputCalls = append(r.outputCalls, commandCall{name: name, args: append([]string(nil), args...)})
	if r.outputErr != nil {
		return nil, r.outputErr
	}
	if len(r.outputs) == 0 {
		return nil, errors.New("missing fake output")
	}
	out := r.outputs[0]
	r.outputs = r.outputs[1:]
	return out, nil
}
