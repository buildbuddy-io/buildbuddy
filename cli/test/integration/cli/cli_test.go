package cli_test

import (
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/testutil/testcli"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"
)

func TestBazelVersion(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	cmd := testcli.Command(t, ws, "version")

	b, err := testcli.CombinedOutput(cmd)
	require.NoError(t, err, "output: %s", string(b))

	require.Contains(t, string(b), "Build label: "+testbazel.Version)
}

func TestBazelBuildWithLocalPlugin(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"plugins/test/pre_bazel.sh": `
			echo 'Hello from pre_bazel.sh!'
			if grep '\--build_metadata=FOO=bar' "$1" >/dev/null ; then
				echo "--build_metadata FOO=bar was canonicalized as expected!"
			fi
		`,
		"plugins/test/post_bazel.sh": `echo 'Hello from post_bazel.sh!'`,
		"plugins/test/handle_bazel_output.sh": `
			if grep 'Build completed successfully'; then
				echo 'Hello from handle_bazel_output.sh! Build was successful.'
			fi
		`,
	})

	// Install the workspace-local plugin
	cmd := testcli.Command(t, ws, "install", "--path=plugins/test")

	err := cmd.Run()
	require.NoError(t, err)

	testfs.WriteAllFileContents(t, ws, map[string]string{"BUILD": ``})

	cmd = testcli.Command(t, ws, "build", "//...", "--build_metadata", "FOO=bar")

	b, err := testcli.CombinedOutput(cmd)

	require.NoError(t, err)
	output := strings.ReplaceAll(string(b), "\r\n", "\n")

	require.Contains(t, output, "Hello from pre_bazel.sh!")
	require.Contains(t, output, "--build_metadata FOO=bar was canonicalized as expected!")
	require.Contains(t, output, "Hello from handle_bazel_output.sh! Build was successful.")
	require.Contains(t, output, "Hello from post_bazel.sh!")
}
