package integration_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/remotebazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/stretchr/testify/require"
)

func TestWithPublicRepo(t *testing.T) {
	apiKey := os.Getenv("BB_PROD_PROBER_ORG_API_KEY")
	fmt.Printf("Api key is %s", apiKey)
	// Root dir is persisted on recycled runners
	rootDir := "/root/workspace/remote-bazel-integration-test"
	err := os.Setenv("HOME", rootDir)
	require.NoError(t, err)

	err = os.MkdirAll(rootDir, 0755)
	require.NoError(t, err)
	err = os.Chdir(rootDir)
	require.NoError(t, err)

	if _, err := os.Stat(fmt.Sprintf("%s/bazel-gazelle", rootDir)); os.IsNotExist(err) {
		output := testshell.Run(t, rootDir, "git clone https://github.com/bazelbuild/bazel-gazelle --filter=blob:none --depth=1")
		require.NotContains(t, output, "fatal")
	}

	err = os.Chdir(fmt.Sprintf("%s/bazel-gazelle", rootDir))
	require.NoError(t, err)

	// TODO: Run a server and executor locally to run this against
	exitCode, err := remotebazel.HandleRemoteBazel([]string{"help", fmt.Sprintf("--remote_header=x-buildbuddy-api-key=%s", apiKey)})
	require.NoError(t, err)
	require.Equal(t, 0, exitCode)
}
