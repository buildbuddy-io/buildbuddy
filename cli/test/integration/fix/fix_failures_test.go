package fix_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Tests that `bb fix` reports failures, and that --diff reports pending
// changes, through its exit code.

func TestFix_FailsOnUnparseableBuildFile(t *testing.T) {
	ws := fixWorkspace(t, map[string]string{
		"MODULE.bazel":    "module(name = \"x\")\n",
		"bad/BUILD.bazel": "this is ( not starlark\n",
		"BUILD.bazel":     poorlyFormatted,
	})

	out, err := runFix(t, ws)
	require.Error(t, err, "output: %s", out)
	require.Contains(t, out, "bad/BUILD.bazel")

	// Other files are still fixed.
	b, err := os.ReadFile(filepath.Join(ws, "BUILD.bazel"))
	require.NoError(t, err)
	require.NotEqual(t, poorlyFormatted, string(b), "BUILD.bazel should still have been reformatted")
}

func TestFix_DiffExitCode(t *testing.T) {
	ws := fixWorkspace(t, map[string]string{
		"MODULE.bazel": "module(name = \"x\")\n",
		"BUILD.bazel":  poorlyFormatted,
	})

	out, err := runFix(t, ws, "--diff")
	require.Error(t, err, "--diff should fail when there are changes to apply; output: %s", out)

	out, err = runFix(t, ws)
	require.NoError(t, err, "output: %s", out)

	out, err = runFix(t, ws, "--diff")
	require.NoError(t, err, "--diff should pass once there's nothing to change; output: %s", out)
}

func TestFix_FailsWhenGazelleFails(t *testing.T) {
	// Gazelle refuses to process a BUILD file with two rules with the same
	// name (as happened in buildbuddy-internal, unnoticed, because `bb fix`
	// ignored Gazelle's exit code).
	ws := fixWorkspace(t, map[string]string{
		"MODULE.bazel": "module(name = \"x\")\n",
		"BUILD.bazel": `filegroup(
    name = "a",
    srcs = [],
)

filegroup(
    name = "a",
    srcs = [],
)
`,
	})

	out, err := runFix(t, ws)
	require.Error(t, err, "output: %s", out)
	require.Contains(t, out, `multiple rules have the name "a"`)
}
