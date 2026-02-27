// Package bazelrbe_prober_test contains integration tests for the bazelrbe prober
// binary against multiple Bazel versions.
package bazelrbe_prober_test

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

var (
	// Injected via x_defs in BUILD file
	bazelrbeProberRunfilePath string

	// Bazel binary paths for each version - injected via x_defs
	bazel6Runfilepath string
	bazel7Runfilepath string
	bazel8Runfilepath string
)

func TestBazelRBEProber_Bazel6(t *testing.T) {
	runProberTest(t, bazel6Runfilepath, "bazel6", "")
}

func TestBazelRBEProber_Bazel7(t *testing.T) {
	// Bazel 7+ has bzlmod enabled by default, which requires network access to
	// fetch from the Bazel Central Registry. Disable it for network-isolated tests.
	runProberTest(t, bazel7Runfilepath, "bazel7", "--enable_bzlmod=false")
}

func TestBazelRBEProber_Bazel8(t *testing.T) {
	// Skip Bazel 8 test because Bazel 8's embedded WORKSPACE suffix has implicit
	// dependencies on rules_cc that require network access to download. The prober
	// only creates a WORKSPACE file and genrule targets, but Bazel 8 still tries
	// to fetch rules_cc from github.com even with --enable_bzlmod=false and
	// --enable_workspace. This test environment blocks network access.
	//
	// In production, the prober runs with network access and can download these
	// dependencies. To properly test Bazel 8 in CI, we would need to either:
	// 1. Allow network access in the test (tag with "requires-network")
	// 2. Modify the prober to include a pre-warmed repository cache for Bazel 8+
	//
	// For now, we test Bazel 6 and 7 which don't have these implicit dependencies.
	t.Skip("Bazel 8 requires network access to download implicit rules_cc dependency")
}

func runProberTest(t *testing.T, bazelRunfilepath, proberName, extraBazelArgs string) {
	// Set up RBE test environment with a BuildBuddy server and executor
	env := rbetest.NewRBETestEnv(t)
	env.AddBuildBuddyServer()
	flags.Set(t, "executor.api_key", env.APIKey1)
	env.AddExecutor(t)

	// Resolve runfile paths
	proberBinary, err := runfiles.Rlocation(bazelrbeProberRunfilePath)
	require.NoError(t, err, "failed to locate prober binary")

	bazelBinary, err := runfiles.Rlocation(bazelRunfilepath)
	require.NoError(t, err, "failed to locate bazel binary for %s", proberName)

	// Run the prober
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	bazelArgs := "--remote_executor=" + env.GetRemoteExecutionTarget() +
		" --remote_instance_name=buildbuddy-io/buildbuddy/ci" +
		" --remote_default_exec_properties=OSFamily=" + runtime.GOOS +
		" --remote_default_exec_properties=Arch=" + runtime.GOARCH
	if extraBazelArgs != "" {
		bazelArgs += " " + extraBazelArgs
	}

	args := []string{
		"--bazel_binary=" + bazelBinary,
		"--prober_name=" + proberName,
		"--num_targets=2",
		"--num_inputs_per_target=2",
		"--input_size_bytes=1000",
		"--bazel_args=" + bazelArgs,
	}

	cmd := exec.CommandContext(ctx, proberBinary, args...)
	cmd.Env = append(os.Environ(), "HOME="+t.TempDir())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	require.NoError(t, err, "prober failed for %s", proberName)

	// Ensure server processes complete
	env.ShutdownBuildBuddyServers()
}
