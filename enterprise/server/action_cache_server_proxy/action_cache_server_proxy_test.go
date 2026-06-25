package action_cache_server_proxy

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testusage"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	openfeatureTesting "github.com/open-feature/go-sdk/openfeature/testing"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
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

type localOnlyCache struct {
	interfaces.Cache
	mu     sync.Mutex
	values map[string][]byte
	mtimes map[string]int64
}

func cacheKey(r *rspb.ResourceName) string {
	return fmt.Sprintf("%d/%s/%d/%s/%d/%d", r.GetCacheType(), r.GetInstanceName(), r.GetCompressor(), r.GetDigest().GetHash(), r.GetDigest().GetSizeBytes(), r.GetDigestFunction())
}

func newLocalOnlyCache() *localOnlyCache {
	return &localOnlyCache{
		values: make(map[string][]byte),
		mtimes: make(map[string]int64),
	}
}

func (c *localOnlyCache) Get(ctx context.Context, r *rspb.ResourceName) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.values[cacheKey(r)]
	if !ok {
		return nil, status.NotFoundError("not found")
	}
	return append([]byte(nil), v...), nil
}

func (c *localOnlyCache) Metadata(ctx context.Context, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := cacheKey(r)
	if _, ok := c.values[key]; !ok {
		return nil, status.NotFoundError("not found")
	}
	return &interfaces.CacheMetadata{LastModifyTimeUsec: c.mtimes[key]}, nil
}

func (c *localOnlyCache) GetWithMetadata(ctx context.Context, r *rspb.ResourceName) ([]byte, *interfaces.CacheMetadata, error) {
	data, err := c.Get(ctx, r)
	if err != nil {
		return nil, nil, err
	}
	md, err := c.Metadata(ctx, r)
	if err != nil {
		return nil, nil, err
	}
	return data, md, nil
}

func (c *localOnlyCache) Set(ctx context.Context, r *rspb.ResourceName, data []byte) error {
	c.put(r, data, time.Now().UnixMicro())
	return nil
}

func (c *localOnlyCache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	w := &localWriteCloser{}
	w.commit = func() error {
		return c.Set(ctx, r, w.Bytes())
	}
	return w, nil
}

func (c *localOnlyCache) put(r *rspb.ResourceName, data []byte, mtime int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := cacheKey(r)
	c.values[key] = append([]byte(nil), data...)
	c.mtimes[key] = mtime
}

func seedLocalActionResult(t *testing.T, cache *localOnlyCache, req *repb.GetActionResultRequest, result *repb.ActionResult, mtime int64) *digest.ACResourceName {
	localKey, err := getACKeyForGetActionResultRequest(req)
	require.NoError(t, err)
	casDigest, err := digest.ComputeForMessage(result, req.GetDigestFunction())
	require.NoError(t, err)
	casKey := digest.NewCASResourceName(casDigest, req.GetInstanceName(), req.GetDigestFunction())
	resultBytes, err := proto.Marshal(result)
	require.NoError(t, err)
	casKeyBytes, err := proto.Marshal(casKey.ToProto())
	require.NoError(t, err)

	cache.put(localKey.ToProto(), casKeyBytes, mtime)
	cache.put(casKey.ToProto(), resultBytes, mtime)
	return localKey
}

type localWriteCloser struct {
	bytes.Buffer
	commit func() error
}

func (w *localWriteCloser) Close() error {
	return nil
}

func (w *localWriteCloser) Commit() error {
	return w.commit()
}

func setActionCacheTTL(t *testing.T, env *testenv.TestEnv, ttlSeconds int) {
	testProvider := openfeatureTesting.NewTestProvider()
	testProvider.UsingFlags(t, map[string]memprovider.InMemoryFlag{
		"cache_proxy.action_cache_ttl_seconds": {
			State:          memprovider.Enabled,
			DefaultVariant: "enabled",
			Variants: map[string]any{
				"enabled": ttlSeconds,
			},
		},
	})
	require.NoError(t, openfeature.SetProviderAndWait(testProvider))
	t.Cleanup(testProvider.Cleanup)
	fp, err := experiments.NewFlagProvider("test")
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)
}

type unexpectedActionCacheClient struct {
	repb.ActionCacheClient
	t testing.TB
}

func (c *unexpectedActionCacheClient) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	c.t.Fatal("unexpected remote GetActionResult")
	return nil, nil
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
	flags.Set(t, "cache_proxy.cache_action_results", false)
	ctx := context.Background()
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers())
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
	ta := testauth.NewTestAuthenticator(t, users)
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
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user", "GR123"))
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
	// backing cache. Writing through the proxy also populates the local cache,
	// so reading it back from the proxy is already a cache hit.
	update(ctx, proxy, digestA, 1, t)
	require.Equal(t, int32(1), get(ctx, ac, digestA, t).GetExitCode())
	require.Equal(t, int32(1), get(ctx, proxy, digestA, t).GetExitCode())
	require.Equal(t, 1, countingClient.cacheHitCount)

	// Another read is also a cache hit.
	require.Equal(t, int32(1), get(ctx, proxy, digestA, t).GetExitCode())
	require.Equal(t, 2, countingClient.cacheHitCount)

	// Change the action result through the proxy. This refreshes the local
	// entry, so the new result is readable and reads remain cache hits.
	update(ctx, proxy, digestA, 2, t)
	require.Equal(t, int32(2), get(ctx, ac, digestA, t).GetExitCode())
	require.Equal(t, int32(2), get(ctx, proxy, digestA, t).GetExitCode())
	require.Equal(t, 3, countingClient.cacheHitCount)
	// Reading from the proxy again is also a cache hit.
	require.Equal(t, int32(2), get(ctx, proxy, digestA, t).GetExitCode())
	require.Equal(t, 4, countingClient.cacheHitCount)

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

func TestActionCacheProxy_TTLServesFreshLocalResult(t *testing.T) {
	flags.Set(t, "cache_proxy.cache_action_results", true)

	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user", "GR123"))
	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user")
	require.NoError(t, err)

	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(ta)
	ut := testusage.NewTracker()
	env.SetUsageTracker(ut)
	setActionCacheTTL(t, env, 60)

	digestA := &repb.Digest{
		Hash:      strings.Repeat("a", 64),
		SizeBytes: 1024,
	}
	req := &repb.GetActionResultRequest{
		ActionDigest:   digestA,
		DigestFunction: repb.DigestFunction_SHA256,
	}

	localResult := &repb.ActionResult{
		ExitCode: 1,
		ExecutionMetadata: &repb.ExecutedActionMetadata{
			ExecutionStartTimestamp:     timestamppb.New(time.Unix(10, 0)),
			ExecutionCompletedTimestamp: timestamppb.New(time.Unix(11, 0)),
		},
	}
	cache := newLocalOnlyCache()
	seedLocalActionResult(t, cache, req, localResult, env.GetClock().Now().Add(-time.Second).UnixMicro())
	proxy := &ActionCacheServerProxy{
		supportsEncryption: func(context.Context) bool { return false },
		env:                env,
		authenticator:      env.GetAuthenticator(),
		localCache:         cache,
		remoteACClient:     &unexpectedActionCacheClient{t: t},
	}
	rsp, err := proxy.GetActionResult(ctx, req)
	require.NoError(t, err)
	require.Equal(t, int32(1), rsp.GetExitCode())

	totals := ut.Totals()
	require.Len(t, totals, 1)
	require.Equal(t, "GR123", totals[0].GroupID)
	require.Equal(t, int64(1), totals[0].Counts.ActionCacheHits)
	require.Equal(t, digestA.GetSizeBytes(), totals[0].Counts.TotalDownloadSizeBytes)
	require.Equal(t, int64(time.Second/time.Microsecond), totals[0].Counts.TotalCachedActionExecUsec)
}

func TestActionCacheProxy_TTLExpiredValidatesWithRemote(t *testing.T) {
	flags.Set(t, "cache_proxy.cache_action_results", true)
	flags.Set(t, "cache.check_client_action_result_digests", true)

	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user", "GR123"))
	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user")
	require.NoError(t, err)

	remoteAC := runACServer(ctx, t, ta)
	countingClient := &countingActionCacheClient{realAC: remoteAC}

	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(ta)
	setActionCacheTTL(t, env, 60)

	digestA := &repb.Digest{
		Hash:      strings.Repeat("a", 64),
		SizeBytes: 1024,
	}
	req := &repb.GetActionResultRequest{
		ActionDigest:   digestA,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	localResult, err := remoteAC.UpdateActionResult(ctx, &repb.UpdateActionResultRequest{
		ActionDigest:   digestA,
		DigestFunction: repb.DigestFunction_SHA256,
		ActionResult:   &repb.ActionResult{ExitCode: 1},
	})
	require.NoError(t, err)
	cache := newLocalOnlyCache()
	oldMTime := env.GetClock().Now().Add(-2 * time.Minute).UnixMicro()
	localKey := seedLocalActionResult(t, cache, req, localResult, oldMTime)
	proxy := &ActionCacheServerProxy{
		supportsEncryption: func(context.Context) bool { return false },
		env:                env,
		authenticator:      env.GetAuthenticator(),
		localCache:         cache,
		remoteACClient:     countingClient,
	}

	rsp, err := proxy.GetActionResult(ctx, req)
	require.NoError(t, err)
	require.Equal(t, int32(1), rsp.GetExitCode())
	require.Equal(t, 1, countingClient.getCount)
	require.Equal(t, 1, countingClient.cacheHitCount)

	require.Eventually(t, func() bool {
		md, err := cache.Metadata(ctx, localKey.ToProto())
		return err == nil && md.LastModifyTimeUsec > oldMTime
	}, time.Second, 10*time.Millisecond)

	rsp, err = proxy.GetActionResult(ctx, &repb.GetActionResultRequest{
		ActionDigest:   digestA,
		DigestFunction: repb.DigestFunction_SHA256,
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), rsp.GetExitCode())
	require.Equal(t, 1, countingClient.getCount)
}

func TestActionCacheProxy_DoesNotCacheHashOnlyRemoteResponse(t *testing.T) {
	flags.Set(t, "cache_proxy.cache_action_results", true)
	flags.Set(t, "cache.check_client_action_result_digests", true)

	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user", "GR123"))
	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user")
	require.NoError(t, err)

	remoteAC := runACServer(ctx, t, ta)
	countingClient := &countingActionCacheClient{realAC: remoteAC}

	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(ta)
	setActionCacheTTL(t, env, 60)

	digestA := &repb.Digest{
		Hash:      strings.Repeat("a", 64),
		SizeBytes: 1024,
	}
	remoteResult, err := remoteAC.UpdateActionResult(ctx, &repb.UpdateActionResultRequest{
		ActionDigest:   digestA,
		DigestFunction: repb.DigestFunction_SHA256,
		ActionResult:   &repb.ActionResult{ExitCode: 1},
	})
	require.NoError(t, err)
	remoteResultDigest, err := digest.ComputeForMessage(remoteResult, repb.DigestFunction_SHA256)
	require.NoError(t, err)

	proxy := &ActionCacheServerProxy{
		supportsEncryption: func(context.Context) bool { return false },
		env:                env,
		authenticator:      env.GetAuthenticator(),
		localCache:         newLocalOnlyCache(),
		remoteACClient:     countingClient,
	}

	hashOnlyResp, err := proxy.GetActionResult(ctx, &repb.GetActionResultRequest{
		ActionDigest:             digestA,
		DigestFunction:           repb.DigestFunction_SHA256,
		CachedActionResultDigest: remoteResultDigest,
	})
	require.NoError(t, err)
	require.Equal(t, remoteResultDigest.GetHash(), hashOnlyResp.GetActionResultDigest().GetHash())
	require.Equal(t, 1, countingClient.getCount)

	fullResp, err := proxy.GetActionResult(ctx, &repb.GetActionResultRequest{
		ActionDigest:   digestA,
		DigestFunction: repb.DigestFunction_SHA256,
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), fullResp.GetExitCode())
	require.Empty(t, fullResp.GetActionResultDigest().GetHash())
	require.Equal(t, 2, countingClient.getCount)
}

func TestActionCacheProxy_UpdateActionResultRefreshesLocalResultWithinTTL(t *testing.T) {
	flags.Set(t, "cache_proxy.cache_action_results", true)

	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user", "GR123"))
	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user")
	require.NoError(t, err)

	remoteAC := runACServer(ctx, t, ta)
	countingClient := &countingActionCacheClient{realAC: remoteAC}

	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(ta)
	setActionCacheTTL(t, env, 60)

	digestA := &repb.Digest{
		Hash:      strings.Repeat("a", 64),
		SizeBytes: 1024,
	}
	getReq := &repb.GetActionResultRequest{
		ActionDigest:   digestA,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	cache := newLocalOnlyCache()
	seedLocalActionResult(t, cache, getReq, &repb.ActionResult{ExitCode: 1}, env.GetClock().Now().Add(-time.Second).UnixMicro())
	proxy := &ActionCacheServerProxy{
		supportsEncryption: func(context.Context) bool { return false },
		env:                env,
		authenticator:      env.GetAuthenticator(),
		localCache:         cache,
		remoteACClient:     countingClient,
	}

	rsp, err := proxy.GetActionResult(ctx, getReq)
	require.NoError(t, err)
	require.Equal(t, int32(1), rsp.GetExitCode())
	require.Equal(t, 0, countingClient.getCount)
	require.Equal(t, 0, countingClient.updateCount)

	_, err = proxy.UpdateActionResult(ctx, &repb.UpdateActionResultRequest{
		ActionDigest:   digestA,
		DigestFunction: repb.DigestFunction_SHA256,
		ActionResult:   &repb.ActionResult{ExitCode: 2},
	})
	require.NoError(t, err)
	require.Equal(t, 0, countingClient.getCount)
	require.Equal(t, 1, countingClient.updateCount)

	rsp, err = proxy.GetActionResult(ctx, getReq)
	require.NoError(t, err)
	require.Equal(t, int32(2), rsp.GetExitCode())
	require.Equal(t, 0, countingClient.getCount)
	require.Equal(t, 1, countingClient.updateCount)
}

func TestActionCacheProxy_UpdateActionResultDoesNotLeaveFreshRequestVariantStale(t *testing.T) {
	flags.Set(t, "cache_proxy.cache_action_results", true)

	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user", "GR123"))
	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user")
	require.NoError(t, err)

	remoteAC := runACServer(ctx, t, ta)
	countingClient := &countingActionCacheClient{realAC: remoteAC}

	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(ta)
	setActionCacheTTL(t, env, 60)

	digestA := &repb.Digest{
		Hash:      strings.Repeat("a", 64),
		SizeBytes: 1024,
	}
	getReq := &repb.GetActionResultRequest{
		ActionDigest:        digestA,
		DigestFunction:      repb.DigestFunction_SHA256,
		IncludeTimelineData: true,
	}
	cache := newLocalOnlyCache()
	seedLocalActionResult(t, cache, getReq, &repb.ActionResult{ExitCode: 1}, env.GetClock().Now().Add(-time.Second).UnixMicro())
	proxy := &ActionCacheServerProxy{
		supportsEncryption: func(context.Context) bool { return false },
		env:                env,
		authenticator:      env.GetAuthenticator(),
		localCache:         cache,
		remoteACClient:     countingClient,
	}

	_, err = proxy.UpdateActionResult(ctx, &repb.UpdateActionResultRequest{
		ActionDigest:   digestA,
		DigestFunction: repb.DigestFunction_SHA256,
		ActionResult:   &repb.ActionResult{ExitCode: 2},
	})
	require.NoError(t, err)
	require.Equal(t, 0, countingClient.getCount)
	require.Equal(t, 1, countingClient.updateCount)

	rsp, err := proxy.GetActionResult(ctx, getReq)
	require.NoError(t, err)
	require.Equal(t, int32(2), rsp.GetExitCode())
	require.Equal(t, 1, countingClient.getCount)
	require.Equal(t, 1, countingClient.updateCount)
}

func TestSkipRemote(t *testing.T) {
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user", "GR123"))
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
	getCount      int
	updateCount   int
}

// GetActionResult implements remote_execution.ActionCacheClient.
func (c *countingActionCacheClient) GetActionResult(ctx context.Context, in *repb.GetActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	c.getCount++
	result, err := c.realAC.GetActionResult(ctx, in, opts...)
	if result.GetActionResultDigest() != nil {
		c.cacheHitCount++
	}
	return result, err
}

// UpdateActionResult implements remote_execution.ActionCacheClient.
func (c *countingActionCacheClient) UpdateActionResult(ctx context.Context, in *repb.UpdateActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	c.updateCount++
	return c.realAC.UpdateActionResult(ctx, in, opts...)
}

// TestRestrictedPrefixBypassViaProxy checks whether an untrusted cache client
// can access restricted AC instance name prefixes (e.g. _bb_ociregistry_) by
// sending requests through the cache proxy.
func TestRestrictedPrefixBypassViaProxy(t *testing.T) {
	ctx := context.Background()
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user", "GR123"))
	userCtx, err := ta.WithAuthenticatedUser(ctx, "user")
	require.NoError(t, err)

	// Shared signing key — both the auth server and the proxy use the same key
	// so the auth server can verify JWTs signed by the proxy.
	key, err := random.RandomString(16)
	require.NoError(t, err)
	flags.Set(t, "app.client_identity.key", string(key))
	flags.Set(t, "app.client_identity.client", interfaces.ClientIdentityCacheProxy)

	// Authoritative AC server with identity validation enabled.
	authEnv := testenv.GetTestEnv(t)
	authEnv.SetAuthenticator(ta)
	require.NoError(t, clientidentity.Register(authEnv))

	authACServer, err := action_cache_server.NewActionCacheServer(authEnv)
	require.NoError(t, err)
	authGRPCServer, authRunFunc, authLis := testenv.RegisterLocalGRPCServer(t, authEnv)
	repb.RegisterActionCacheServer(authGRPCServer, authACServer)
	go authRunFunc()

	proxyEnv := testenv.GetTestEnv(t)
	proxyEnv.SetAuthenticator(ta)
	require.NoError(t, clientidentity.Register(proxyEnv))

	authConn, err := testenv.LocalGRPCConn(ctx, authLis,
		interceptors.GetUnaryClientIdentityInterceptor(proxyEnv),
	)
	require.NoError(t, err)
	t.Cleanup(func() { authConn.Close() })

	proxyEnv.SetActionCacheClient(repb.NewActionCacheClient(authConn))
	proxyEnv.SetLocalActionCacheServer(runLocalActionCacheServerForProxy(ctx, proxyEnv, t))

	proxyACServer, err := NewActionCacheServerProxy(proxyEnv)
	require.NoError(t, err)
	proxyGRPCServer, proxyRunFunc, proxyLis := testenv.RegisterLocalGRPCServer(t, proxyEnv)
	repb.RegisterActionCacheServer(proxyGRPCServer, proxyACServer)
	go proxyRunFunc()

	// External client: connects to the proxy without any client-identity
	// interceptor, simulating a normal authenticated Bazel remote-cache client.
	extConn, err := testenv.LocalGRPCConn(ctx, proxyLis)
	require.NoError(t, err)
	t.Cleanup(func() { extConn.Close() })
	extACClient := repb.NewActionCacheClient(extConn)

	d := &repb.Digest{
		Hash:      strings.Repeat("a", 64),
		SizeBytes: 1024,
	}

	_, getErr := extACClient.GetActionResult(userCtx, &repb.GetActionResultRequest{
		InstanceName:   interfaces.OCIImageInstanceNamePrefix,
		ActionDigest:   d,
		DigestFunction: repb.DigestFunction_SHA256,
	})
	require.True(t, status.IsUnauthenticatedError(getErr),
		"GetActionResult with restricted prefix via proxy: expected UnauthenticatedError (bypass fixed), got: %v", getErr)

	_, updateErr := extACClient.UpdateActionResult(userCtx, &repb.UpdateActionResultRequest{
		InstanceName:   interfaces.OCIImageInstanceNamePrefix,
		ActionDigest:   d,
		DigestFunction: repb.DigestFunction_SHA256,
		ActionResult:   &repb.ActionResult{ExitCode: 0},
	})
	require.True(t, status.IsUnauthenticatedError(updateErr),
		"UpdateActionResult with restricted prefix via proxy: expected UnauthenticatedError (bypass fixed), got: %v", updateErr)
}
