package main

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestInternalActionCache(t *testing.T) {
	env := testenv.GetTestEnv(t)
	enterprise_testenv.AddClientIdentity(t, env, interfaces.ClientIdentityCacheProxy)
	flags.Set(t, "internal_grpc_port", testport.FindFree(t))
	env.SetListenAddr("localhost")

	server, err := grpc_server.New(env, grpc_server.InternalGRPCPort(), false, grpc_server.GRPCServerConfig{})
	require.NoError(t, err)
	t.Cleanup(server.GetServer().Stop)
	env.SetInternalGRPCServer(server.GetServer())
	require.NoError(t, registerInternalServices(env))
	require.NoError(t, server.Start())

	// The local AC must be reachable over gRPC using the proxy's own identity,
	// including the restricted instance namespace used for OCI metadata.
	// No remote cache or OCIFetcher clients are configured.
	client := env.GetLocalActionCacheClient()
	require.NotNil(t, client)
	d, _ := testdigest.NewReader(t, 100)
	_, err = client.UpdateActionResult(t.Context(), &repb.UpdateActionResultRequest{
		InstanceName:   interfaces.OCIImageInstanceNamePrefix,
		DigestFunction: repb.DigestFunction_SHA256,
		ActionDigest:   d,
		ActionResult:   &repb.ActionResult{ExitCode: 42},
	})
	require.NoError(t, err)
	result, err := client.GetActionResult(t.Context(), &repb.GetActionResultRequest{
		InstanceName:   interfaces.OCIImageInstanceNamePrefix,
		DigestFunction: repb.DigestFunction_SHA256,
		ActionDigest:   d,
	})
	require.NoError(t, err)
	require.EqualValues(t, 42, result.GetExitCode())
}
