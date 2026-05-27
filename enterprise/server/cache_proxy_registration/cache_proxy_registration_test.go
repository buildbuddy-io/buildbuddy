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
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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
	apiKey      = "CP_KEY"
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

	registry, err := cache_proxy_registry_server.NewCacheProxyRegistryServer(env)
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
	registry, client := startTestRegistry(t, map[string]interfaces.UserInfo{apiKey: streamUser})

	node := &cppb.CacheProxyNode{
		Host: "host-1", ProxyId: "proxy-1", OsFamily: "linux", Arch: "amd64", Version: "v1",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	streamCtx := outgoingCtx(ctx, apiKey)

	// Run the stream loop in a goroutine; it returns once we cancel.
	done := make(chan error, 1)
	go func() {
		done <- streamHeartbeats(streamCtx, client, node)
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

	err := streamHeartbeats(streamCtx, client, &cppb.CacheProxyNode{Host: "h", ProxyId: "id"})
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
		done <- streamHeartbeats(context.Background(), client, &cppb.CacheProxyNode{Host: "h", ProxyId: "id"})
	}()
	select {
	case err := <-done:
		require.Error(t, err)
		assert.True(t, status.IsDeadlineExceededError(err), "expected deadline exceeded, got: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("streamHeartbeats blocked on an unreachable target — retry loop will never progress")
	}
}
