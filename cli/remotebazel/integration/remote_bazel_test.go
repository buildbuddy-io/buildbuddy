package integration_test

import (
	"fmt"
	"github.com/buildbuddy-io/buildbuddy/cli/remotebazel"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testexecutor"
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/stretchr/testify/require"
	"net"
	"os"
	"testing"
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

	// Spinning up firecracker needs iptables which is in /usr/sbin.
	err = os.Setenv("PATH", os.Getenv("PATH")+":/usr/sbin")
	require.NoError(t, err)

	// Run a server and executor locally to run this against

	grpcPort := testport.FindFree(t)
	appConfig := &app.App{
		HttpPort:       testport.FindFree(t),
		GRPCPort:       grpcPort,
		MonitoringPort: testport.FindFree(t),
	}
	// The firecracker runner has its own networking stack, and cannot reach
	// the locally running server using `localhost` so use the server's IP addr
	ip := GetOutboundIP(t)
	grpcAddress := fmt.Sprintf("grpc://%s:%v", ip, grpcPort)
	app := buildbuddy_enterprise.RunWithConfig(t,
		appConfig,
		buildbuddy_enterprise.DefaultConfig,
		fmt.Sprintf("--app.events_api_url=%s", grpcAddress),
		fmt.Sprintf("--app.cache_api_url=%s", grpcAddress))
	_ = testexecutor.Run(
		t,
		testexecutor.ExecutorRunfilePath,
		[]string{
			"--executor.app_target=" + grpcAddress,
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

	// TODO: Get the invocation ID with SearchInvocation
	invocationID := "2c7325e5-b0fc-4c3b-920e-e1fe289043d0"
	rsp2 := &inpb.GetInvocationResponse{}
	err = webClient.RPC("GetInvocation", &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{
			InvocationId: invocationID,
		},
	}, rsp2)
	require.NoError(t, err)
	require.Contains(t, rsp2.Invocation[0].ConsoleBuffer, "Usage: bazel <command> <options>")
}

// https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
func GetOutboundIP(t *testing.T) net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	require.NoError(t, err)
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}
