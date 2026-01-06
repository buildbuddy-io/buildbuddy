package bazelrbe_prober_test

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/stretchr/testify/require"
)

// Injected via x_defs per Bazel version
var (
	bazelRlocationpath  string
	proberRlocationpath string
	bazelMajorVersion   string // "6", "7", or "8"
)

func runProber(t testing.TB, ctx context.Context, env *rbetest.Env, extraBazelArgs ...string) error {
	bazelPath, err := runfiles.Rlocation(bazelRlocationpath)
	require.NoError(t, err, "failed to locate bazel binary")

	proberPath, err := runfiles.Rlocation(proberRlocationpath)
	require.NoError(t, err, "failed to locate prober binary")

	bazelArgs := []string{
		"--remote_executor=" + env.GetRemoteExecutionTarget(),
	}
	// Bazel 6 needs --enable_bzlmod for MODULE.bazel support
	if bazelMajorVersion == "6" {
		bazelArgs = append(bazelArgs, "--enable_bzlmod")
	}
	bazelArgs = append(bazelArgs, extraBazelArgs...)

	args := []string{
		"--bazel_binary=" + bazelPath,
		"--bazel_args=" + strings.Join(bazelArgs, " "),
		"--prober_name=test",
		"--num_targets=2",
		"--num_inputs_per_target=2",
		"--input_size_bytes=1000",
	}

	cmd := exec.CommandContext(ctx, proberPath, args...)
	output, err := cmd.CombinedOutput()
	t.Logf("Prober output:\n%s", output)
	return err
}

func TestProberBasic(t *testing.T) {
	env := rbetest.NewRBETestEnv(t)
	env.AddBuildBuddyServer()
	env.AddExecutor(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	err := runProber(t, ctx, env)
	require.NoError(t, err, "prober should complete successfully")
}
