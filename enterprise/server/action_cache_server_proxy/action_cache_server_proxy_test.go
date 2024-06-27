package action_cache_server_proxy

import (
	"context"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func runACServer(ctx context.Context, t *testing.T) repb.ActionCacheClient {
	env := testenv.GetTestEnv(t)
	acServer, err := action_cache_server.NewActionCacheServer(env)
	require.NoError(t, err)
	grpcServer, runFunc := testenv.RegisterLocalGRPCServer(env)
	repb.RegisterActionCacheServer(grpcServer, acServer)
	go runFunc()
	conn, err := testenv.LocalGRPCConn(ctx, env)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return repb.NewActionCacheClient(conn)
}

func runACProxy(ctx context.Context, client repb.ActionCacheClient, t *testing.T) repb.ActionCacheClient {
	env := testenv.GetTestEnv(t)
	env.SetActionCacheClient(client)
	proxyServer, err := NewActionCacheServerProxy(env)
	require.NoError(t, err)
	grpcServer, runFunc := testenv.RegisterLocalGRPCServer(env)
	repb.RegisterActionCacheServer(grpcServer, proxyServer)
	go runFunc()
	conn, err := testenv.LocalGRPCConn(ctx, env)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return repb.NewActionCacheClient(conn)
}

func update(ctx context.Context, client repb.ActionCacheClient, digest *repb.Digest, code int32, t *testing.T) {
	req := repb.UpdateActionResultRequest{
		ActionDigest:   digest,
		DigestFunction: repb.DigestFunction_SHA256,
		ActionResult:   &repb.ActionResult{ExitCode: code},
	}
	_, err := client.UpdateActionResult(ctx, &req)
	require.NoError(t, err)
}

func get(ctx context.Context, client repb.ActionCacheClient, digest *repb.Digest, t *testing.T) *repb.ActionResult {
	req := &repb.GetActionResultRequest{
		ActionDigest:   digest,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	resp, err := client.GetActionResult(ctx, req)
	require.NoError(t, err)
	return resp
}

func TestActionCacheProxy(t *testing.T) {
	ctx := context.Background()
	ac := runACServer(ctx, t)
	proxy := runACProxy(ctx, ac, t)

	digestA := &repb.Digest{
		Hash:      strings.Repeat("a", 64),
		SizeBytes: 1024,
	}
	digestB := &repb.Digest{
		Hash:      strings.Repeat("b", 64),
		SizeBytes: 1024,
	}

	// DigestA shouldn't be present initially.
	readReqA := &repb.GetActionResultRequest{
		ActionDigest:   digestA,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	_, err := proxy.GetActionResult(ctx, readReqA)
	require.True(t, status.IsNotFoundError(err))

	// Write it through the proxy and confirm it's readable from the proxy and
	// backing cache.
	update(ctx, proxy, digestA, 1, t)
	require.Equal(t, int32(1), get(ctx, ac, digestA, t).GetExitCode())
	require.Equal(t, int32(1), get(ctx, proxy, digestA, t).GetExitCode())

	// Change the action result, write it, and confirm the new result is
	// readable from the proxy and backing cache.
	update(ctx, proxy, digestA, 2, t)
	require.Equal(t, int32(2), get(ctx, ac, digestA, t).GetExitCode())
	require.Equal(t, int32(2), get(ctx, proxy, digestA, t).GetExitCode())

	// DigestB shouldn't be present initially.
	readReqB := &repb.GetActionResultRequest{
		ActionDigest:   digestB,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	_, err = proxy.GetActionResult(ctx, readReqB)
	require.True(t, status.IsNotFoundError(err))

	// Write it to the backing cache and confirm it's readable from the proxy
	// and backing cache.
	update(ctx, ac, digestB, 999, t)
	require.Equal(t, int32(999), get(ctx, ac, digestB, t).GetExitCode())
	require.Equal(t, int32(999), get(ctx, proxy, digestB, t).GetExitCode())

	// Change the action result, write it, and confirm the new result is
	// readable from the proxy and backing cache.
	update(ctx, ac, digestB, 998, t)
	require.Equal(t, int32(998), get(ctx, ac, digestB, t).GetExitCode())
	require.Equal(t, int32(998), get(ctx, proxy, digestB, t).GetExitCode())
}
