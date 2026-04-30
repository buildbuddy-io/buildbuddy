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

func TestConfigure_UsesResolvedArgsForStreamingOpts(t *testing.T) {
	usePseudoTTY(t)

	args := []string{"run", "//:target"}
	resolvedArgs := []string{
		"run",
		"//:target",
		"--stream_run_logs",
		"--remote_header=x-buildbuddy-api-key=abc123",
		"--invocation_id=from-config",
	}
	bazelArgs := &parser.BazelArgs{Forwarded: args, Resolved: resolvedArgs}

	bazelArgs, opts, err := Configure(bazelArgs, "grpcs://example.com")

	require.NoError(t, err)
	require.Equal(t, args, bazelArgs.Forwarded)
	require.Equal(t, &Opts{
		BesBackend:   "grpcs://example.com",
		InvocationID: "from-config",
		ApiKey:       "abc123",
		OnFailure:    FailureModeFail,
	}, opts)
}

func TestConfigure_AppendsInvocationIDWhenMissingFromResolvedArgs(t *testing.T) {
	usePseudoTTY(t)

	args := []string{"run", "//:target"}
	resolvedArgs := []string{
		"run",
		"//:target",
		"--stream_run_logs",
		"--remote_header=x-buildbuddy-api-key=abc123",
	}
	bazelArgs := &parser.BazelArgs{Forwarded: args, Resolved: resolvedArgs}

	bazelArgs, opts, err := Configure(bazelArgs, "grpcs://example.com")

	require.NoError(t, err)
	require.NotNil(t, opts)
	require.NotEmpty(t, opts.InvocationID)
	require.Equal(t, opts.InvocationID, arg.Get(bazelArgs.Forwarded, "invocation_id"))
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
