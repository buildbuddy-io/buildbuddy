package cache_proxy_registration

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/cache_proxy_registry_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/upgrade"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	cppb "github.com/buildbuddy-io/buildbuddy/proto/cache_proxy"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
)

const (
	testGroupID = "GR1"
	testAPIKey  = "CP_KEY"
)

// userWithCapabilities builds a claims.Claims (implementing
// interfaces.UserInfo) with the given capabilities in its current group.
func userWithCapabilities(userID, groupID string, caps ...cappb.Capability) interfaces.UserInfo {
	return &claims.Claims{
		UserID:        userID,
		GroupID:       groupID,
		AllowedGroups: []string{groupID},
		GroupMemberships: []*interfaces.GroupMembership{{
			GroupID:      groupID,
			Capabilities: caps,
		}},
		Capabilities: caps,
	}
}

// startTestRegistry wires up a real CacheProxyRegistryServer behind a local
// bufconn gRPC server, returning the registry instance (for poking Redis
// directly) and a client connected to that server.
func startTestRegistry(t *testing.T, users map[string]interfaces.UserInfo) (*cache_proxy_registry_server.CacheProxyRegistryServer, cppb.CacheProxyRegistryClient) {
	redisTarget := testredis.Start(t).Target
	env := enterprise_testenv.GetCustomTestEnv(t, &enterprise_testenv.Options{
		RedisTarget: redisTarget,
	})
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, users))

	registry, err := cache_proxy_registry_server.NewCacheProxyRegistryServer(env, upgrade.NewDetector(nil))
	require.NoError(t, err)
	env.SetCacheProxyRegistryService(registry)

	server, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	cppb.RegisterCacheProxyRegistryServer(server, registry)
	go runFunc()

	dialCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	conn, err := testenv.LocalGRPCConn(dialCtx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return registry, cppb.NewCacheProxyRegistryClient(conn)
}

// outgoingCtx returns a context whose outgoing gRPC metadata advertises the
// given API key — what `run` would do before handing the context to the
// stream.
func outgoingCtx(ctx context.Context, key string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, key)
}

func groupReqContext(groupID string) *ctxpb.RequestContext {
	return &ctxpb.RequestContext{GroupId: groupID}
}

func TestStreamHeartbeats_RegistersProxy(t *testing.T) {
	// Use two distinct Claims objects with the same group: one is owned by
	// the server's auth map (read by the streaming RPC goroutine), the
	// other backs the test goroutine's AuthContextWithJWT call. They have
	// the same group/capabilities so both pass the ACL check, but sharing
	// a single Claims struct would race because AuthContextWithJWT mutates
	// the JWT field on its argument.
	streamUser := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	readerUser := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	registry, client := startTestRegistry(t, map[string]interfaces.UserInfo{testAPIKey: streamUser})

	node := &cppb.CacheProxyNode{
		Host: "host-1", ProxyId: "proxy-1", OsFamily: "linux", Arch: "amd64", Version: "v1",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	streamCtx := outgoingCtx(ctx, testAPIKey)

	// Run the stream loop in a goroutine; it returns once we cancel.
	done := make(chan error, 1)
	go func() {
		done <- streamHeartbeats(streamCtx, make(chan struct{}), client, node)
	}()

	// The initial heartbeat should make the proxy visible to GetCacheProxies
	// almost immediately.
	authedCtx := claims.AuthContextWithJWT(context.Background(), readerUser.(*claims.Claims), nil)
	require.Eventually(t, func() bool {
		resp, err := registry.GetCacheProxies(authedCtx, &cppb.GetCacheProxiesRequest{
			RequestContext: groupReqContext(testGroupID),
		})
		return err == nil && len(resp.GetCacheProxy()) == 1 && resp.GetCacheProxy()[0].GetNode().GetProxyId() == "proxy-1"
	}, 2*time.Second, 25*time.Millisecond, "proxy should have registered")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("streamHeartbeats did not return after context cancel")
	}
}

// TestStreamHeartbeats_ShutdownRemovesProxy verifies that closing
// shutdownCh causes streamHeartbeats to send a final shutting_down=true
// message that the server uses to drop the proxy from its registry
// immediately, rather than waiting for the heartbeat staleness TTL.
func TestStreamHeartbeats_ShutdownRemovesProxy(t *testing.T) {
	// Two distinct Claims with the same group: one is owned by the server's
	// auth map (read by the streaming RPC goroutine), the other backs the
	// test goroutine's AuthContextWithJWT call, which mutates the JWT field
	// on its argument and would race with a shared Claims struct.
	streamUser := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	readerUser := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	registry, client := startTestRegistry(t, map[string]interfaces.UserInfo{testAPIKey: streamUser})

	node := &cppb.CacheProxyNode{Host: "host-x", ProxyId: "proxy-x", Version: "v1"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	streamCtx := outgoingCtx(ctx, testAPIKey)

	shutdownCh := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- streamHeartbeats(streamCtx, shutdownCh, client, node)
	}()

	authedCtx := claims.AuthContextWithJWT(context.Background(), readerUser.(*claims.Claims), nil)
	require.Eventually(t, func() bool {
		resp, err := registry.GetCacheProxies(authedCtx, &cppb.GetCacheProxiesRequest{
			RequestContext: groupReqContext(testGroupID),
		})
		return err == nil && len(resp.GetCacheProxy()) == 1
	}, 2*time.Second, 25*time.Millisecond, "proxy should have registered")

	close(shutdownCh)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("streamHeartbeats did not return after shutdown signal")
	}

	// The shutdown message should have removed the proxy from the registry
	// immediately — we shouldn't have to wait the staleness TTL.
	resp, err := registry.GetCacheProxies(authedCtx, &cppb.GetCacheProxiesRequest{
		RequestContext: groupReqContext(testGroupID),
	})
	require.NoError(t, err)
	assert.Empty(t, resp.GetCacheProxy(), "shutting-down proxy should have been removed from registry")
}

func TestStreamHeartbeats_UnauthorizedReturnsError(t *testing.T) {
	// Drive heartbeats fast so a buffered first-Send EOF surfaces quickly
	// on the next tick.
	prev := heartbeatInterval
	heartbeatInterval = 50 * time.Millisecond
	t.Cleanup(func() { heartbeatInterval = prev })

	noCap := userWithCapabilities("U1", testGroupID, cappb.Capability_CACHE_WRITE)
	_, client := startTestRegistry(t, map[string]interfaces.UserInfo{"NOCAP_KEY": noCap})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	streamCtx := outgoingCtx(ctx, "NOCAP_KEY")

	err := streamHeartbeats(streamCtx, make(chan struct{}), client, &cppb.CacheProxyNode{Host: "h", ProxyId: "id"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "REGISTER_CACHE_PROXY", "expected capability error, got: %v", err)
}

// TestStreamHeartbeats_UnreachableTarget points the registration loop at a
// TCP listener that accepts connections but never speaks gRPC, simulating a
// black-hole app target. In production, run() passes a long-lived ctx (only
// cancelled on shutdown), so streamHeartbeats needs to return on its own
// when the dial/handshake fails — otherwise the surrounding retry-with-
// backoff loop never gets to run. This test uses an undecorated
// context.Background() to match that production scenario.
func TestStreamHeartbeats_UnreachableTarget(t *testing.T) {
	// Drive the initial-send timeout fast so the test doesn't have to wait
	// the production default (10s).
	prev := initialSendTimeout
	initialSendTimeout = 100 * time.Millisecond
	t.Cleanup(func() { initialSendTimeout = prev })

	// Accept TCP connections but never speak HTTP/2 — gRPC will keep
	// retrying the handshake forever.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := cppb.NewCacheProxyRegistryClient(conn)

	done := make(chan error, 1)
	go func() {
		done <- streamHeartbeats(context.Background(), make(chan struct{}), client, &cppb.CacheProxyNode{Host: "h", ProxyId: "id"})
	}()
	select {
	case err := <-done:
		require.Error(t, err)
		assert.True(t, status.IsDeadlineExceededError(err), "expected deadline exceeded, got: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("streamHeartbeats blocked on an unreachable target — retry loop will never progress")
	}
}

func TestSumByStatus(t *testing.T) {
	cv := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_sum_by_status",
	}, []string{metrics.CacheHitMissStatus, "other"})

	// Two "hit" series across different "other" labels — these should be
	// summed together.
	cv.WithLabelValues(metrics.HitStatusLabel, "a").Add(3)
	cv.WithLabelValues(metrics.HitStatusLabel, "b").Add(4)
	// One "miss" series.
	cv.WithLabelValues(metrics.MissStatusLabel, "a").Add(10)
	// One "uncacheable" series.
	cv.WithLabelValues(metrics.UncacheableStatusLabel, "a").Add(5)
	// A series with a status value the helper doesn't recognize — must be
	// ignored, not double-counted into any bucket.
	cv.WithLabelValues("unknown", "a").Add(99)

	hits, misses, uncacheable := sumByStatus(cv)
	assert.Equal(t, int64(7), hits)
	assert.Equal(t, int64(10), misses)
	assert.Equal(t, int64(5), uncacheable)
}

func TestSumByStatus_Empty(t *testing.T) {
	cv := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_sum_by_status_empty",
	}, []string{metrics.CacheHitMissStatus})

	hits, misses, uncacheable := sumByStatus(cv)
	assert.Equal(t, int64(0), hits)
	assert.Equal(t, int64(0), misses)
	assert.Equal(t, int64(0), uncacheable)
}

func TestConfiguredFlags(t *testing.T) {
	flags.Set(t, "cache_proxy.app_target", "grpcs://app.example.com")
	flags.Set(t, "cache_proxy.api_key", "SUPER_SECRET_KEY")

	configured := configuredFlags()

	assert.Contains(t, configured, "--cache_proxy.app_target=grpcs://app.example.com")
	// Secret flags are reported, but with their values redacted.
	assert.Contains(t, configured, "--cache_proxy.api_key=<redacted>")
	for _, f := range configured {
		assert.NotContains(t, f, "SUPER_SECRET_KEY")
		// Flags left at their defaults are omitted.
		assert.NotContains(t, f, "--cache_proxy.metadata_directory")
	}
}

// fakeHeartbeatStream records sent requests; the embedded interface panics on
// any other method, which sendHeartbeat won't call as long as Send succeeds.
type fakeHeartbeatStream struct {
	cppb.CacheProxyRegistry_RegisterAndStreamHeartbeatClient
	sent []*cppb.RegisterCacheProxyRequest
}

func (f *fakeHeartbeatStream) Send(req *cppb.RegisterCacheProxyRequest) error {
	// Clone the request: real gRPC streams serialize at Send time, but
	// sendHeartbeat mutates the shared node between sends.
	f.sent = append(f.sent, proto.Clone(req).(*cppb.RegisterCacheProxyRequest))
	return nil
}

// Flag values can change at runtime (the config file is re-read on SIGHUP),
// so every heartbeat should report the current configuration, not a snapshot
// from startup.
func TestSendHeartbeat_RefreshesConfiguredFlags(t *testing.T) {
	stream := &fakeHeartbeatStream{}
	node := &cppb.CacheProxyNode{Host: "h", ProxyId: "id"}

	flags.Set(t, "cache_proxy.app_target", "grpcs://before.example.com")
	require.NoError(t, sendHeartbeat(stream, &cppb.RegisterCacheProxyRequest{Node: node}))

	flags.Set(t, "cache_proxy.app_target", "grpcs://after.example.com")
	require.NoError(t, sendHeartbeat(stream, &cppb.RegisterCacheProxyRequest{Node: node}))

	require.Len(t, stream.sent, 2)
	assert.Contains(t, stream.sent[0].GetNode().GetConfiguredFlags(), "--cache_proxy.app_target=grpcs://before.example.com")
	assert.Contains(t, stream.sent[1].GetNode().GetConfiguredFlags(), "--cache_proxy.app_target=grpcs://after.example.com")
}
