package action_cache_server_proxy

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func runRemoteActionCacheServer(ctx context.Context, env *testenv.TestEnv, t *testing.T) repb.ActionCacheClient {
	remoteActionCacheServer, err := action_cache_server.NewActionCacheServer(env)
	require.NoError(t, err)
	grpcServver, runFunc := testenv.RegisterLocalGRPCServer(env)
	repb.RegisterActionCacheServer(grpcServver, remoteActionCacheServer)
	go runFunc()
	conn, err := testenv.LocalGRPCConn(ctx, env,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4*1024*1024)))
	require.NoError(t, err)
	return repb.NewActionCacheClient(conn)
}

func runActionCacheServerProxy(ctx context.Context, remoteEnv *testenv.TestEnv, localEnv *testenv.TestEnv, t *testing.T) *grpc.ClientConn {
	acClient := runRemoteActionCacheServer(ctx, remoteEnv, t)
	localEnv.SetActionCacheClient(acClient)
	actionCacheServer, err := NewActionCacheServerProxy(localEnv)
	require.NoError(t, err)
	grpcServer, runFunc := testenv.RegisterLocalGRPCServer(localEnv)
	repb.RegisterActionCacheServer(grpcServer, actionCacheServer)
	go runFunc()

	clientConn, err := testenv.LocalGRPCConn(ctx, localEnv, grpc.WithDefaultCallOptions())
	if err != nil {
		t.Error(err)
	}

	return clientConn
}

func TestActionCacheProxy(t *testing.T) {
	ctx := context.Background()
	remoteEnv := testenv.GetTestEnv(t)
	localEnv := testenv.GetTestEnv(t)
	clientConn := runActionCacheServerProxy(ctx, remoteEnv, localEnv, t)
	acClient := repb.NewActionCacheClient(clientConn)

	digest := repb.Digest{
		Hash:      "0000000000000000000000000000000000000000000000000000000000000000",
		SizeBytes: 1024,
	}
	readReq := &repb.GetActionResultRequest{
		ActionDigest:   &digest,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	_, err := acClient.GetActionResult(ctx, readReq)
	require.True(t, status.IsNotFoundError(err))

	writeReq := repb.UpdateActionResultRequest{
		ActionDigest:   &digest,
		DigestFunction: repb.DigestFunction_SHA256,
		ActionResult:   &repb.ActionResult{ExitCode: 1},
	}
	_, err = acClient.UpdateActionResult(ctx, &writeReq)
	require.NoError(t, err)

	readResp, err := acClient.GetActionResult(ctx, readReq)
	require.NoError(t, err)
	require.Equal(t, int32(1), readResp.GetExitCode())

	writeReq = repb.UpdateActionResultRequest{
		ActionDigest:   &digest,
		DigestFunction: repb.DigestFunction_SHA256,
		ActionResult:   &repb.ActionResult{ExitCode: 2},
	}
	_, err = acClient.UpdateActionResult(ctx, &writeReq)
	require.NoError(t, err)

	readResp, err = acClient.GetActionResult(ctx, readReq)
	require.NoError(t, err)
	require.Equal(t, int32(2), readResp.GetExitCode())
}
