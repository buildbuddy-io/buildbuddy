package bazelrbe_prober_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/stretchr/testify/require"
)

// Injected via x_defs for each Bazel version
var (
	bazel6Rlocationpath  string
	outdir6Rlocationpath string
	bazel7Rlocationpath  string
	outdir7Rlocationpath string
	bazel8Rlocationpath  string
	outdir8Rlocationpath string
	proberRlocationpath  string
)

type bazelVersion struct {
	name                string
	majorVersion        string
	bazelRlocationpath  string
	outdirRlocationpath string
}

func getBazelVersions() []bazelVersion {
	return []bazelVersion{
		{"Bazel6", "6", bazel6Rlocationpath, outdir6Rlocationpath},
		{"Bazel7", "7", bazel7Rlocationpath, outdir7Rlocationpath},
		{"Bazel8", "8", bazel8Rlocationpath, outdir8Rlocationpath},
	}
}

func runProber(t testing.TB, ctx context.Context, env *rbetest.Env, v bazelVersion, extraBazelArgs ...string) error {
	bazelPath, err := runfiles.Rlocation(v.bazelRlocationpath)
	require.NoError(t, err, "failed to locate bazel binary")

	proberPath, err := runfiles.Rlocation(proberRlocationpath)
	require.NoError(t, err, "failed to locate prober binary")

	outdirPath, err := runfiles.Rlocation(v.outdirRlocationpath)
	require.NoError(t, err, "failed to locate pre-extracted bazel installation")
	lockfilePath := filepath.Join(outdirPath, "MODULE.bazel.lock")
	repoCachePath := filepath.Join(outdirPath, "repository_cache")

	bazelArgs := []string{
		"--remote_executor=" + env.GetRemoteExecutionTarget(),
		"--repository_cache=" + repoCachePath,
		"--lockfile_mode=error",
	}
	// Bazel 6 and 7 need --enable_bzlmod for MODULE.bazel support.
	// Bazel 8+ has bzlmod enabled by default.
	if v.majorVersion == "6" || v.majorVersion == "7" {
		bazelArgs = append(bazelArgs, "--enable_bzlmod")
	}
	bazelArgs = append(bazelArgs, extraBazelArgs...)

	args := []string{
		"--bazel_binary=" + bazelPath,
		"--bazel_args=" + strings.Join(bazelArgs, " "),
		"--lockfile_path=" + lockfilePath,
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
	for _, v := range getBazelVersions() {
		t.Run(v.name, func(t *testing.T) {
			env := rbetest.NewRBETestEnv(t)
			env.AddBuildBuddyServer()
			env.AddExecutor(t)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			err := runProber(t, ctx, env, v)
			require.NoError(t, err, "prober should complete successfully")
		})
	}
}
