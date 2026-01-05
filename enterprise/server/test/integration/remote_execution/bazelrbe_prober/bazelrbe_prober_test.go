package bazelrbe_prober_test

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

// Injected via x_defs in BUILD
var (
	proberBinaryRunfilePath string
	bazelBinaryRunfilePath  string
	outdirRunfilePath       string // contains repository_cache and MODULE.bazel.lock
)

func TestProber_Success(t *testing.T) {
	// Set up RBE environment with BuildBuddy server and executor
	env := rbetest.NewRBETestEnv(t)
	env.AddBuildBuddyServer()
	flags.Set(t, "executor.api_key", env.APIKey1)
	env.AddExecutor(t)

	// Get the prober binary path from runfiles
	proberPath, err := runfiles.Rlocation(proberBinaryRunfilePath)
	require.NoError(t, err, "failed to locate prober binary")

	// Get the bazel binary path from runfiles
	bazelPath, err := runfiles.Rlocation(bazelBinaryRunfilePath)
	require.NoError(t, err, "failed to locate bazel binary")

	// Get the lockfile path from the outdir
	outdirPath, err := runfiles.Rlocation(outdirRunfilePath)
	require.NoError(t, err, "failed to locate bazel outdir")
	lockfilePath := filepath.Join(outdirPath, "MODULE.bazel.lock")

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
