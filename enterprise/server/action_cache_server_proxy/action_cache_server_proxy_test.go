package action_cache_server_proxy

import (
	"context"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func runACServer(ctx context.Context, t *testing.T, ta *testauth.TestAuthenticator) repb.ActionCacheClient {
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(ta)

	acServer, err := action_cache_server.NewActionCacheServer(env)
	require.NoError(t, err)
	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	repb.RegisterActionCacheServer(grpcServer, acServer)
	go runFunc()
	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return repb.NewActionCacheClient(conn)
}

func runACProxy(ctx context.Context, t *testing.T, ta *testauth.TestAuthenticator, client repb.ActionCacheClient) repb.ActionCacheClient {
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(ta)
	env.SetActionCacheClient(client)
	env.SetLocalActionCacheServer(runLocalActionCacheServerForProxy(ctx, env, t))

	proxyServer, err := NewActionCacheServerProxy(env)
	require.NoError(t, err)
	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	repb.RegisterActionCacheServer(grpcServer, proxyServer)
	go runFunc()
	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return repb.NewActionCacheClient(conn)
}

func runLocalActionCacheServerForProxy(ctx context.Context, env *testenv.TestEnv, t *testing.T) repb.ActionCacheServer {
	server, err := action_cache_server.NewActionCacheServer(env)
	require.NoError(t, err)
	return server
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

func getCached(ctx context.Context, client repb.ActionCacheClient, digest *repb.Digest, cachedValue *repb.Digest, t *testing.T) *repb.ActionResult {
	req := &repb.GetActionResultRequest{
		ActionDigest:             digest,
		DigestFunction:           repb.DigestFunction_SHA256,
		CachedActionResultDigest: cachedValue,
	}
	resp, err := client.GetActionResult(ctx, req)
	require.NoError(t, err)
	return resp
}

func TestActionCacheProxy(t *testing.T) {
	flags.Set(t, "cache_proxy.cache_action_results", false)
	ctx := context.Background()
	ta := testauth.NewTestAuthenticator(testauth.TestUsers())
	ac := runACServer(ctx, t, ta)
	proxy := runACProxy(ctx, t, ta, ac)

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

func TestActionCacheProxy_CachingAndEncryptionEnabled(t *testing.T) {
	flags.Set(t, "cache_proxy.cache_action_results", true)
	flags.Set(t, "cache.check_client_action_result_digests", true)

	env := testenv.GetTestEnv(t)
	userWithEncryption := testauth.User("user", "GR123")
	userWithEncryption.CacheEncryptionEnabled = true
	users := map[string]interfaces.UserInfo{
		"user": userWithEncryption,
	}
	ta := testauth.NewTestAuthenticator(users)
	env.SetAuthenticator(ta)
	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user")
	require.NoError(t, err)

	ac := runACServer(ctx, t, ta)
	countingClient := &countingActionCacheClient{
		realAC: ac,
	}
	proxy := runACProxy(ctx, t, ta, countingClient)

	digestA := &repb.Digest{
		Hash:      strings.Repeat("a", 64),
		SizeBytes: 1024,
	}

	// DigestA shouldn't be present initially.
	readReqA := &repb.GetActionResultRequest{
		ActionDigest:   digestA,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	_, err = proxy.GetActionResult(ctx, readReqA)
	require.True(t, status.IsNotFoundError(err))

	// Write it through the proxy and confirm it's readable from the proxy and
	// backing cache--this would theoretically stuff the cache if the user
	// wasn't using encryption.
	update(ctx, proxy, digestA, 1, t)
	require.Equal(t, int32(1), get(ctx, ac, digestA, t).GetExitCode())
	require.Equal(t, int32(1), get(ctx, proxy, digestA, t).GetExitCode())
	require.Equal(t, 0, countingClient.cacheHitCount)

	// Caching is eanbled, but the user is using encryption, so we shouldn't
	// be caching.
	require.Equal(t, int32(1), get(ctx, proxy, digestA, t).GetExitCode())
	require.Equal(t, 0, countingClient.cacheHitCount)
}

func TestActionCacheProxy_CachingEnabled(t *testing.T) {
	flags.Set(t, "cache_proxy.cache_action_results", true)
	flags.Set(t, "cache.check_client_action_result_digests", true)

	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(testauth.TestUsers("user", "GR123"))
	env.SetAuthenticator(ta)
	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user")
	require.NoError(t, err)

	ac := runACServer(ctx, t, ta)
	countingClient := &countingActionCacheClient{
		realAC: ac,
	}
	proxy := runACProxy(ctx, t, ta, countingClient)

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
	_, err = proxy.GetActionResult(ctx, readReqA)
	require.True(t, status.IsNotFoundError(err))

	// Write it through the proxy and confirm it's readable from the proxy and
	// backing cache.
	update(ctx, proxy, digestA, 1, t)
	require.Equal(t, int32(1), get(ctx, ac, digestA, t).GetExitCode())
	require.Equal(t, int32(1), get(ctx, proxy, digestA, t).GetExitCode())
	require.Equal(t, 0, countingClient.cacheHitCount)

	// This extra read should actually be a cached hit.
	require.Equal(t, int32(1), get(ctx, proxy, digestA, t).GetExitCode())
	require.Equal(t, 1, countingClient.cacheHitCount)

	// Change the action result, write it, and confirm the new result is
	// readable from the proxy and backing cache.
	update(ctx, proxy, digestA, 2, t)
	require.Equal(t, int32(2), get(ctx, ac, digestA, t).GetExitCode())
	require.Equal(t, int32(2), get(ctx, proxy, digestA, t).GetExitCode())
	// That should _not_ be a cache hit.
	require.Equal(t, 1, countingClient.cacheHitCount)
	// But reading from the proxy again _should_ be a cache hit.
	require.Equal(t, int32(2), get(ctx, proxy, digestA, t).GetExitCode())
	require.Equal(t, 2, countingClient.cacheHitCount)

	countingClient.cacheHitCount = 0
	// DigestB shouldn't be present initially,
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
	require.Equal(t, 0, countingClient.cacheHitCount)

	// Do a few extra reads..
	require.Equal(t, int32(999), get(ctx, proxy, digestB, t).GetExitCode())
	require.Equal(t, int32(999), get(ctx, proxy, digestB, t).GetExitCode())
	require.Equal(t, 2, countingClient.cacheHitCount)

	// Change the action result, write it, and confirm the new result is
	// readable from the proxy and backing cache.
	update(ctx, ac, digestB, 998, t)
	require.Equal(t, int32(998), get(ctx, ac, digestB, t).GetExitCode())
	require.Equal(t, int32(998), get(ctx, proxy, digestB, t).GetExitCode())
	require.Equal(t, 2, countingClient.cacheHitCount)
}

func TestSkipRemote(t *testing.T) {
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(testauth.TestUsers("user", "GR123"))
	env.SetAuthenticator(ta)
	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user")
	require.NoError(t, err)
	ctx = metadata.AppendToOutgoingContext(ctx, proxy_util.SkipRemoteKey, "true")

	ac := runACServer(ctx, t, ta)
	proxy := runACProxy(ctx, t, ta, ac)

	d := &repb.Digest{
		Hash:      strings.Repeat("a", 64),
		SizeBytes: 1024,
	}

	// Write to the proxy and check that no data was written to the remote cache.
	exitCode := int32(9)
	update(ctx, proxy, d, exitCode, t)
	req := &repb.GetActionResultRequest{
		ActionDigest:   d,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	_, err = ac.GetActionResult(ctx, req)
	require.True(t, status.IsNotFoundError(err))

	// Verify we can successfully read the data from the proxy.
	require.Equal(t, exitCode, get(ctx, proxy, d, t).GetExitCode())
}

type countingActionCacheClient struct {
	realAC        repb.ActionCacheClient
	cacheHitCount int
}

// GetActionResult implements remote_execution.ActionCacheClient.
func (c *countingActionCacheClient) GetActionResult(ctx context.Context, in *repb.GetActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	result, err := c.realAC.GetActionResult(ctx, in, opts...)
	if result.GetActionResultDigest() != nil {
		c.cacheHitCount++
	}
	return result, err
}

// UpdateActionResult implements remote_execution.ActionCacheClient.
func (c *countingActionCacheClient) UpdateActionResult(ctx context.Context, in *repb.UpdateActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	return c.realAC.UpdateActionResult(ctx, in, opts...)
}
