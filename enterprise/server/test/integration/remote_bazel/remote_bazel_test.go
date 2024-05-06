package remote_bazel_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/remotebazel"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testexecutor"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/stretchr/testify/require"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

func TestWithPublicRepo(t *testing.T) {
	// Use a dir that is persisted on recycled runners
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

	// Run a server and executor locally to run remote bazel against
	grpcPort := testport.FindFree(t)
	appConfig := &app.App{
		HttpPort:       testport.FindFree(t),
		GRPCPort:       grpcPort,
		MonitoringPort: testport.FindFree(t),
	}
	grpcAddress := fmt.Sprintf("grpc://localhost:%v", grpcPort)
	app := buildbuddy_enterprise.RunWithConfig(t,
		appConfig,
		buildbuddy_enterprise.DefaultConfig,
		// Use the server that was just spun up as the BES handler for the remote run
		// (these URLs are propagated to the ci_runner)
		fmt.Sprintf("--app.events_api_url=%s", grpcAddress),
		fmt.Sprintf("--app.cache_api_url=%s", grpcAddress))
	_ = testexecutor.Run(
		t,
		testexecutor.ExecutorRunfilePath,
		[]string{
			"--executor.app_target=" + grpcAddress,
			"--executor.enable_bare_runner=true",
			"--debug_stream_command_outputs",
		},
	)
	webClient := buildbuddy_enterprise.LoginAsDefaultSelfAuthUser(t, app)

	// Get an API key to authenticate the remote bazel request
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
		// Have the ci runner use the "none" isolation type because it's simpler
		// to setup than a firecracker runner
		"--runner_exec_properties=workload-isolation-type=none",
		"--runner_exec_properties=container-image=",
		// Give more memory to the CI runner
		"--runner_exec_properties=EstimatedComputeUnits=3",
		"help",
		fmt.Sprintf("--remote_header=x-buildbuddy-api-key=%s", apiKey)})
	require.NoError(t, err)
	require.Equal(t, 0, exitCode)

	// Check the invocation logs to ensure the "bazel help" command successfully run
	searchRsp := &inpb.SearchInvocationResponse{}
	err = webClient.RPC("SearchInvocation", &inpb.SearchInvocationRequest{
		RequestContext: webClient.RequestContext,
		Query:          &inpb.InvocationQuery{GroupId: webClient.RequestContext.GetGroupId()},
	}, searchRsp)
	require.NoError(t, err)
	require.Equal(t, 1, len(searchRsp.GetInvocation()))
	invocationID := searchRsp.Invocation[0].InvocationId

	invRsp := &inpb.GetInvocationResponse{}
	err = webClient.RPC("GetInvocation", &inpb.GetInvocationRequest{
		RequestContext: webClient.RequestContext,
		Lookup: &inpb.InvocationLookup{
			InvocationId: invocationID,
		},
	}, invRsp)
	require.NoError(t, err)
	require.Equal(t, 1, len(invRsp.Invocation))
	require.Contains(t, invRsp.Invocation[0].ConsoleBuffer, "Usage: bazel <command> <options>")
}
