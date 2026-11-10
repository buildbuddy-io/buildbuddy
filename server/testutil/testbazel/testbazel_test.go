package testbazel_test

import (
	"context"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/stretchr/testify/require"
)

// TestOfflineBuildAndTest checks that the embedded test bazel works without
// network access: it builds a genrule and runs an sh_test loaded from
// @rules_shell, using only the prewarmed install_base, lockfile and vendor
// dir.
func TestOfflineBuildAndTest(t *testing.T) {
	ctx := context.Background()
	ws := testbazel.MakeTempModule(t, map[string]string{
		"BUILD": `
load("@rules_shell//shell:sh_test.bzl", "sh_test")

genrule(name = "gen", outs = ["gen.out"], cmd = "echo hello > $@")

sh_test(name = "sh_test", srcs = ["test.sh"], data = [":gen"])
`,
		"test.sh": "#!/usr/bin/env sh\ngrep -q hello gen.out\n",
	})

	result := testbazel.Invoke(ctx, t, ws, "info", "release")
	require.NoError(t, result.Error, "stderr: %s", result.Stderr)
	require.Equal(t, "release "+testbazel.Version, strings.TrimSpace(result.Stdout))

	result = testbazel.Invoke(ctx, t, ws, "build", "//:gen")
	require.NoError(t, result.Error, "stderr: %s", result.Stderr)

	result = testbazel.Invoke(ctx, t, ws, "test", "//:sh_test", "--test_output=errors")
	require.NoError(t, result.Error, "stderr: %s", result.Stderr)
}
