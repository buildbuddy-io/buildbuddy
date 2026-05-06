package metadata

import (
	"encoding/json"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/stretchr/testify/require"
)

func TestAppendBuildMetadata(t *testing.T) {
	ws, commitSHA := testgit.MakeTempRepo(t, map[string]string{"WORKSPACE": ""})
	testgit.ConfigureRemoteOrigin(t, ws, "https://user:secret@example.com/org/repo.git")
	workspace.SetForTest(t, ws)

	args := []string{
		"build",
		// Even though AppendBuildMetadata sets branch metadata, the user value should take precedence.
		"--build_metadata=BRANCH_NAME=user-provided-branch",
		// Any user-provided metadata should be preserved.
		"--build_metadata=USER=1",
		"//foo",
	}
	updatedArgs, err := AppendBuildMetadata(args, append([]string{"/usr/local/bin/bb"}, args...))
	require.NoError(t, err)

	expectedOriginalArgsJSON, err := json.Marshal(append([]string{"bb"}, args...))
	require.NoError(t, err)

	// All metadata should be appended after the bazel command.
	require.Equal(t, "build", updatedArgs[0])

	// Check all the expected metadata flags are present.
	require.ElementsMatch(t, []string{
		"EXPLICIT_COMMAND_LINE=" + string(expectedOriginalArgsJSON),
		"REPO_URL=https://example.com/org/repo.git",
		"COMMIT_SHA=" + commitSHA,
		"BRANCH_NAME=master",
		"BRANCH_NAME=user-provided-branch",
		"USER=1",
	}, arg.GetMulti(updatedArgs, "build_metadata"))

	// The user-provided branch name should take precedence.
	require.Equal(t, "user-provided-branch", arg.Get(updatedArgs, "build_metadata=BRANCH_NAME"))
}
