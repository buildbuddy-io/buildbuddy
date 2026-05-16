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
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
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

type staticExperimentFlagDetails struct{}

func (staticExperimentFlagDetails) Variant() string {
	return ""
}

type staticExperimentFlagProvider struct {
	bools  map[string]bool
	int64s map[string]int64
}

func (p *staticExperimentFlagProvider) Boolean(ctx context.Context, flagName string, defaultValue bool, opts ...any) bool {
	if v, ok := p.bools[flagName]; ok {
		return v
	}
	return defaultValue
}

func (p *staticExperimentFlagProvider) String(ctx context.Context, flagName string, defaultValue string, opts ...any) string {
	return defaultValue
}

func (p *staticExperimentFlagProvider) Float64(ctx context.Context, flagName string, defaultValue float64, opts ...any) float64 {
	return defaultValue
}

func (p *staticExperimentFlagProvider) Int64(ctx context.Context, flagName string, defaultValue int64, opts ...any) int64 {
	if v, ok := p.int64s[flagName]; ok {
		return v
	}
	return defaultValue
}

func (p *staticExperimentFlagProvider) Object(ctx context.Context, flagName string, defaultValue map[string]any, opts ...any) map[string]any {
	return defaultValue
}

func (p *staticExperimentFlagProvider) BooleanDetails(ctx context.Context, flagName string, defaultValue bool, opts ...any) (bool, interfaces.ExperimentFlagDetails) {
	return p.Boolean(ctx, flagName, defaultValue, opts...), staticExperimentFlagDetails{}
}

func (p *staticExperimentFlagProvider) StringDetails(ctx context.Context, flagName string, defaultValue string, opts ...any) (string, interfaces.ExperimentFlagDetails) {
	return p.String(ctx, flagName, defaultValue, opts...), staticExperimentFlagDetails{}
}

func (p *staticExperimentFlagProvider) Float64Details(ctx context.Context, flagName string, defaultValue float64, opts ...any) (float64, interfaces.ExperimentFlagDetails) {
	return p.Float64(ctx, flagName, defaultValue, opts...), staticExperimentFlagDetails{}
}

func (p *staticExperimentFlagProvider) Int64Details(ctx context.Context, flagName string, defaultValue int64, opts ...any) (int64, interfaces.ExperimentFlagDetails) {
	return p.Int64(ctx, flagName, defaultValue, opts...), staticExperimentFlagDetails{}
}

func (p *staticExperimentFlagProvider) ObjectDetails(ctx context.Context, flagName string, defaultValue map[string]any, opts ...any) (map[string]any, interfaces.ExperimentFlagDetails) {
	return p.Object(ctx, flagName, defaultValue, opts...), staticExperimentFlagDetails{}
}

func (p *staticExperimentFlagProvider) Subscribe(ch chan<- struct{}) (stop func()) {
	return func() {}
}

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
	env.AddBuildBuddyServerWithOptions(&rbetest.BuildBuddyServerOptions{
		EnvModifier: func(env *testenv.TestEnv) {
			env.SetExperimentFlagProvider(&staticExperimentFlagProvider{
				bools: map[string]bool{
					"cache.chunking_enabled":          true,
					"executor.upload_outputs_chunked": true,
				},
			})
		},
	})
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
		"--large_output_size_bytes=2097152",
		"--large_output_size_bytes=3145728",
		"--large_output_size_bytes=4194304",
		"--large_output_size_bytes=5242880",
		"--large_output_size_bytes=10485760",
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
