package integration_test

import (
	"fmt"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testexecutor"
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/remotebazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/stretchr/testify/require"
)

func TestWithPublicRepo(t *testing.T) {
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

	// Spinning up firecracker need iptables which is in /usr/sbin.
	err = os.Setenv("PATH", os.Getenv("PATH")+":/usr/sbin")
	require.NoError(t, err)

	// Run a server and executor locally to run this against
	app := buildbuddy_enterprise.Run(t)
	_ = testexecutor.Run(
		t,
		testexecutor.ExecutorRunfilePath,
		[]string{
			"--executor.app_target=" + app.GRPCAddress(),
			"--executor.default_isolation_type=firecracker",
			"--executor.enable_firecracker=true",
			"--debug_stream_command_outputs",
		},
	)
	webClient := buildbuddy_enterprise.LoginAsDefaultSelfAuthUser(t, app)

	rsp := &akpb.CreateApiKeyResponse{}
	err = webClient.RPC("CreateApiKey", &akpb.CreateApiKeyRequest{
		RequestContext: webClient.RequestContext,
		GroupId:        webClient.RequestContext.GroupId,
		Capability: []akpb.ApiKey_Capability{
			akpb.ApiKey_CAS_WRITE_CAPABILITY,
			akpb.ApiKey_CACHE_WRITE_CAPABILITY,
			akpb.ApiKey_ORG_ADMIN_CAPABILITY,
		},
	}, rsp)
	require.NoError(t, err)
	apiKey := rsp.ApiKey.Value

	exitCode, err := remotebazel.HandleRemoteBazel([]string{
		fmt.Sprintf("--remote_runner=%s", app.GRPCAddress()),
		"help",
		fmt.Sprintf("--remote_header=x-buildbuddy-api-key=%s", apiKey)})
	require.NoError(t, err)
	require.Equal(t, 0, exitCode)
}
