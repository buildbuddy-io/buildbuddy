package bazelrbe_test

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

var proberBinaryRunfilePath string

func TestProber_Success(t *testing.T) {
	// Set up RBE environment with BuildBuddy server and executor
	env := rbetest.NewRBETestEnv(t)
	env.AddBuildBuddyServer()
	flags.Set(t, "executor.api_key", env.APIKey1)
	env.AddExecutor(t)

	// Get the prober binary path from runfiles
	proberPath, err := runfiles.Rlocation(proberBinaryRunfilePath)
	require.NoError(t, err, "failed to locate prober binary")

	// Get the test bazel binary and lockfile paths
	bazelPath := testbazel.BinaryPath(t)
	lockfilePath := testbazel.LockfilePath(t)

	// Build bazel_args with remote executor and platform properties
	bazelArgs := fmt.Sprintf(
		"--remote_executor=%s --remote_default_exec_properties=OSFamily=%s --remote_default_exec_properties=Arch=%s",
		env.GetRemoteExecutionTarget(), runtime.GOOS, runtime.GOARCH,
	)

	// Run the prober with small values for faster test execution
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, proberPath,
		"--bazel_binary="+bazelPath,
		"--bazel_args="+bazelArgs,
		"--module_lock_file="+lockfilePath,
		"--prober_name=testprober",
		"--num_targets=2",
		"--num_inputs_per_target=2",
		"--input_size_bytes=1000",
	)
	output, err := cmd.CombinedOutput()

	require.NoError(t, err, "prober should succeed. Output:\n%s", string(output))
}
