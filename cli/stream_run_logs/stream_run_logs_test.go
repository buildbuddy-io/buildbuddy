package stream_run_logs

import (
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/creack/pty"
	"github.com/stretchr/testify/require"
)

func init() {
	log.Configure("--verbose=1")
	parser.SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
}

func TestConfigure_UsesEffectiveArgsForStreamingOpts(t *testing.T) {
	usePseudoTTY(t)

	pair := &arg.ArgPair{
		Raw: []string{"run", "//:target"},
		Effective: []string{
			"run",
			"//:target",
			"--stream_run_logs",
			"--remote_header=x-buildbuddy-api-key=abc123",
			"--invocation_id=from-config",
		},
	}

	opts, err := Configure(pair, "grpcs://example.com")

	require.NoError(t, err)
	// Raw args are unchanged when invocation_id is already present in effective.
	require.Equal(t, []string{"run", "//:target"}, pair.Raw)
	require.Equal(t, &Opts{
		BesBackend:   "grpcs://example.com",
		InvocationID: "from-config",
		ApiKey:       "abc123",
		OnFailure:    FailureModeFail,
	}, opts)
}

func TestConfigure_AppendsInvocationIDWhenMissingFromEffectiveArgs(t *testing.T) {
	usePseudoTTY(t)

	pair := &arg.ArgPair{
		Raw: []string{"run", "//:target"},
		Effective: []string{
			"run",
			"//:target",
			"--stream_run_logs",
			"--remote_header=x-buildbuddy-api-key=abc123",
		},
	}

	opts, err := Configure(pair, "grpcs://example.com")

	require.NoError(t, err)
	require.NotNil(t, opts)
	require.NotEmpty(t, opts.InvocationID)
	// Invocation ID should have been appended to both slices.
	require.Equal(t, opts.InvocationID, arg.Get(pair.Raw, "invocation_id"))
}

func usePseudoTTY(t *testing.T) {
	tty, ptmx, err := pty.Open()
	require.NoError(t, err)

	origStdout := os.Stdout
	origStderr := os.Stderr
	os.Stdout = tty
	os.Stderr = tty

	t.Cleanup(func() {
		os.Stdout = origStdout
		os.Stderr = origStderr
		require.NoError(t, tty.Close())
		require.NoError(t, ptmx.Close())
	})
}
