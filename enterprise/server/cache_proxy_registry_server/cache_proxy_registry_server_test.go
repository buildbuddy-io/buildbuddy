package cache_proxy_registry_server

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	cppb "github.com/buildbuddy-io/buildbuddy/proto/cache_proxy"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
)

const (
	testGroupID = "GR1"
)

// newServer wires up a registry server backed by a real Redis and the
// provided test users. Note: the server snapshots env.GetAuthenticator() /
// env.GetClock() at construction, so everything the test needs from env
// must be set before calling this.
func newServer(t *testing.T, users map[string]interfaces.UserInfo) (*CacheProxyRegistryServer, *testenv.TestEnv) {
	redisTarget := testredis.Start(t).Target
	env := enterprise_testenv.GetCustomTestEnv(t, &enterprise_testenv.Options{
		RedisTarget: redisTarget,
	})
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, users))

	s, err := NewCacheProxyRegistryServer(env)
	require.NoError(t, err)
	env.SetCacheProxyRegistryService(s)
	return s, env
}

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

func ctxWithIncomingAPIKey(apiKey string) context.Context {
	md := metadata.New(map[string]string{authutil.APIKeyHeader: apiKey})
	return metadata.NewIncomingContext(context.Background(), md)
}

func ctxWithOutgoingAPIKey(apiKey string) context.Context {
	md := metadata.New(map[string]string{authutil.APIKeyHeader: apiKey})
	return metadata.NewOutgoingContext(context.Background(), md)
}

func TestAuthorize_MissingCredentials_Unauthenticated(t *testing.T) {
	s, _ := newServer(t, map[string]interfaces.UserInfo{})

	_, err := s.authorize(context.Background())
	require.Error(t, err)
	assert.True(t, status.IsUnauthenticatedError(err), "expected unauthenticated, got: %v", err)
}

func TestAuthorize_MissingCapability(t *testing.T) {
	s, _ := newServer(t, map[string]interfaces.UserInfo{
		"NOCAP_KEY": userWithCapabilities("U1", testGroupID, cappb.Capability_CACHE_WRITE),
	})

	_, err := s.authorize(ctxWithIncomingAPIKey("NOCAP_KEY"))
	require.Error(t, err)
	assert.True(t, status.IsPermissionDeniedError(err), "expected permission denied, got: %v", err)
}

func TestAuthorize_Success(t *testing.T) {
	s, _ := newServer(t, map[string]interfaces.UserInfo{
		"CP_KEY": userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY),
	})

	groupID, err := s.authorize(ctxWithIncomingAPIKey("CP_KEY"))
	require.NoError(t, err)
	assert.Equal(t, testGroupID, groupID)
}

func TestGetCacheProxies(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, _ := newServer(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	// Insert two proxies out of host-order; expect them returned alphabetically.
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxyNode{
		Host:    "host-b",
		ProxyId: "id-b",
		Version: "1.0",
	}, nil))
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxyNode{
		Host:    "host-a",
		ProxyId: "id-a",
		Version: "1.0",
	}, nil))

	ctx := claims.AuthContextWithJWT(context.Background(), user.(*claims.Claims), nil)
	resp, err := s.GetCacheProxies(ctx, &cppb.GetCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetCacheProxy(), 2)
	assert.Equal(t, "host-a", resp.GetCacheProxy()[0].GetNode().GetHost())
	assert.Equal(t, "host-b", resp.GetCacheProxy()[1].GetNode().GetHost())
}

func TestGetCacheProxies_Isolation(t *testing.T) {
	const otherGroupID = "GR2"
	other := userWithCapabilities("U2", otherGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, _ := newServer(t, map[string]interfaces.UserInfo{"OTHER_KEY": other})

	// A proxy was registered for the original test group.
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxyNode{
		Host: "host", ProxyId: "id",
	}, nil))

	// A user in a different group asks for that other group's proxies. The
	// ACL filter must drop the entry — the response should be empty even
	// though the entry exists in the queried hash.
	ctx := claims.AuthContextWithJWT(context.Background(), other.(*claims.Claims), nil)
	resp, err := s.GetCacheProxies(ctx, &cppb.GetCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	assert.Empty(t, resp.GetCacheProxy(), "user from %q should not see proxies registered under %q", otherGroupID, testGroupID)
}

func TestGetCacheProxies_Expiration(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, _ := newServer(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	// Insert a fresh proxy and a stale one directly into Redis.
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxyNode{
		Host: "fresh", ProxyId: "fresh",
	}, nil))
	stale := &cppb.RegisteredCacheProxy{
		Registration: &cppb.CacheProxyNode{Host: "stale", ProxyId: "stale"},
		GroupId:      testGroupID,
		LastPingTime: timestamppb.New(time.Now().Add(-2 * maxRegistrationStaleness)),
	}
	b, err := proto.Marshal(stale)
	require.NoError(t, err)
	require.NoError(t, s.rdb.HSet(context.Background(), redisKeyForCacheProxies(testGroupID), "stale", b).Err())

	ctx := claims.AuthContextWithJWT(context.Background(), user.(*claims.Claims), nil)
	resp, err := s.GetCacheProxies(ctx, &cppb.GetCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetCacheProxy(), 1)
	assert.Equal(t, "fresh", resp.GetCacheProxy()[0].GetNode().GetHost())
}

func TestGetCacheProxies_Statistics(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, _ := newServer(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	stats := &cppb.Statistics{
		AcReadHits:       100,
		AcReadMisses:     25,
		AcReadHitBytes:   1024,
		AcReadMissBytes:  256,
		CasReadHits:      4000,
		CasReadMisses:    1000,
		CasReadHitBytes:  50_000_000,
		CasReadMissBytes: 12_500_000,
		AcWrites:         100,
		AcWriteBytes:     1024,
		CasWrites:        1000,
		CasWriteBytes:    10_000_000,
	}
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxyNode{
		Host: "h", ProxyId: "id",
	}, stats))

	ctx := claims.AuthContextWithJWT(context.Background(), user.(*claims.Claims), nil)
	resp, err := s.GetCacheProxies(ctx, &cppb.GetCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetCacheProxy(), 1)
	got := resp.GetCacheProxy()[0].GetStatistics()
	require.NotNil(t, got)
	assert.Equal(t, int64(100), got.GetAcReadHits())
	assert.Equal(t, int64(25), got.GetAcReadMisses())
	assert.Equal(t, int64(1024), got.GetAcReadHitBytes())
	assert.Equal(t, int64(256), got.GetAcReadMissBytes())
	assert.Equal(t, int64(4000), got.GetCasReadHits())
	assert.Equal(t, int64(1000), got.GetCasReadMisses())
	assert.Equal(t, int64(50_000_000), got.GetCasReadHitBytes())
	assert.Equal(t, int64(12_500_000), got.GetCasReadMissBytes())
	assert.Equal(t, int64(100), got.GetAcWrites())
	assert.Equal(t, int64(1024), got.GetAcWriteBytes())
	assert.Equal(t, int64(1000), got.GetCasWrites())
	assert.Equal(t, int64(10_000_000), got.GetCasWriteBytes())
}

func TestGetCacheProxies_StatisticsNil(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, _ := newServer(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	// A proxy that reports no stats (older client, or no traffic yet) should
	// still round-trip cleanly, just with a nil Statistics on the response.
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxyNode{
		Host: "h", ProxyId: "id",
	}, nil))

	ctx := claims.AuthContextWithJWT(context.Background(), user.(*claims.Claims), nil)
	resp, err := s.GetCacheProxies(ctx, &cppb.GetCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetCacheProxy(), 1)
	assert.Nil(t, resp.GetCacheProxy()[0].GetStatistics())
}

func TestInsertOrUpdateProxy_StatisticsOverwritten(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, _ := newServer(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	// Each heartbeat reports cumulative absolute counters, so the second
	// write for a given proxy ID must fully replace the first — we don't
	// want the UI showing yesterday's numbers because today's heartbeat
	// happened to omit a field.
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxyNode{
		Host: "h", ProxyId: "id",
	}, &cppb.Statistics{AcReadHits: 1, CasReadHits: 2}))
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxyNode{
		Host: "h", ProxyId: "id",
	}, &cppb.Statistics{AcReadHits: 10, CasReadHits: 20}))

	ctx := claims.AuthContextWithJWT(context.Background(), user.(*claims.Claims), nil)
	resp, err := s.GetCacheProxies(ctx, &cppb.GetCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetCacheProxy(), 1)
	got := resp.GetCacheProxy()[0].GetStatistics()
	require.NotNil(t, got)
	assert.Equal(t, int64(10), got.GetAcReadHits())
	assert.Equal(t, int64(20), got.GetCasReadHits())
}

func startGRPCRegistry(t *testing.T, users map[string]interfaces.UserInfo) (*CacheProxyRegistryServer, cppb.CacheProxyRegistryClient) {
	s, env := newServer(t, users)

	server, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	cppb.RegisterCacheProxyRegistryServer(server, s)
	go runFunc()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return s, cppb.NewCacheProxyRegistryClient(conn)
}
func TestStreamHeartbeat_PersistsRegistration(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	stream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey("CP_KEY"))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&cppb.RegisterCacheProxyRequest{
		Node: &cppb.CacheProxyNode{
			Host: "proxy-1", ProxyId: "id-1", Version: "v1",
		},
	}))
	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.NotNil(t, resp)

	// The heartbeat should be visible via GetCacheProxies (which uses the
	// AuthenticatedUser context, so we build one directly).
	ctx := claims.AuthContextWithJWT(context.Background(), user.(*claims.Claims), nil)
	getResp, err := s.GetCacheProxies(ctx, &cppb.GetCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	require.Len(t, getResp.GetCacheProxy(), 1)
	assert.Equal(t, "id-1", getResp.GetCacheProxy()[0].GetNode().GetProxyId())
}

func TestStreamHeartbeat_PersistsStatistics(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	stream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey("CP_KEY"))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&cppb.RegisterCacheProxyRequest{
		Node: &cppb.CacheProxyNode{
			Host: "proxy-1", ProxyId: "id-1", Version: "v1",
		},
		Statistics: &cppb.Statistics{
			AcReadHits: 7, AcReadMisses: 3, CasReadHits: 70, CasReadMisses: 30,
		},
	}))
	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	ctx := claims.AuthContextWithJWT(context.Background(), user.(*claims.Claims), nil)
	getResp, err := s.GetCacheProxies(ctx, &cppb.GetCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	require.Len(t, getResp.GetCacheProxy(), 1)
	got := getResp.GetCacheProxy()[0].GetStatistics()
	require.NotNil(t, got)
	assert.Equal(t, int64(7), got.GetAcReadHits())
	assert.Equal(t, int64(3), got.GetAcReadMisses())
	assert.Equal(t, int64(70), got.GetCasReadHits())
	assert.Equal(t, int64(30), got.GetCasReadMisses())
}

func TestStreamHeartbeat_ShutDown(t *testing.T) {
	// Two distinct Claims for the streaming user (read by the server-side
	// auth goroutine) and the reader user (mutated by AuthContextWithJWT
	// on the test goroutine), to avoid a race on the JWT field.
	streamUser := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	readerUser := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"CP_KEY": streamUser})

	// Register two proxies under the same group so we can confirm that
	// only the one that sent ShuttingDown=true is removed.
	keepStream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey("CP_KEY"))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = keepStream.CloseSend()
		_, _ = keepStream.CloseAndRecv()
	})
	require.NoError(t, keepStream.Send(&cppb.RegisterCacheProxyRequest{
		Node: &cppb.CacheProxyNode{Host: "h-keep", ProxyId: "id-keep"},
	}))

	shutdownStream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey("CP_KEY"))
	require.NoError(t, err)
	require.NoError(t, shutdownStream.Send(&cppb.RegisterCacheProxyRequest{
		Node: &cppb.CacheProxyNode{Host: "h-bye", ProxyId: "id-bye"},
	}))

	// stream.Send only buffers — the server-side recv→insertOrUpdateProxy
	// runs on a separate goroutine, so we have to wait for the registry to
	// reflect both heartbeats before sending the shutdown signal.
	ctx := claims.AuthContextWithJWT(context.Background(), readerUser.(*claims.Claims), nil)
	require.Eventually(t, func() bool {
		resp, err := s.GetCacheProxies(ctx, &cppb.GetCacheProxiesRequest{
			RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
		})
		return err == nil && len(resp.GetCacheProxy()) == 2
	}, 2*time.Second, 25*time.Millisecond, "both proxies should have registered")

	// Now send ShuttingDown=true on the second stream only.
	require.NoError(t, shutdownStream.Send(&cppb.RegisterCacheProxyRequest{
		Node:         &cppb.CacheProxyNode{Host: "h-bye", ProxyId: "id-bye"},
		ShuttingDown: true,
	}))
	_, err = shutdownStream.CloseAndRecv()
	require.NoError(t, err)

	// Wait for the registry to reflect the removal of only the shutting-down
	// proxy. The other one must still be present.
	require.Eventually(t, func() bool {
		resp, err := s.GetCacheProxies(ctx, &cppb.GetCacheProxiesRequest{
			RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
		})
		if err != nil || len(resp.GetCacheProxy()) != 1 {
			return false
		}
		return resp.GetCacheProxy()[0].GetNode().GetProxyId() == "id-keep"
	}, 2*time.Second, 25*time.Millisecond, "only the shutting-down proxy should have been removed")
}

func TestStreamHeartbeat_Anonymous(t *testing.T) {
	_, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{})

	stream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey(""))
	require.NoError(t, err)
	_, err = stream.CloseAndRecv()
	require.Error(t, err)
	assert.True(t, status.IsUnauthenticatedError(err), "expected unauthenticated, got: %v", err)
}

func TestStreamHeartbeat_Unauthorized(t *testing.T) {
	noCap := userWithCapabilities("U1", testGroupID, cappb.Capability_CACHE_WRITE)
	_, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"NOCAP_KEY": noCap})

	stream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey("NOCAP_KEY"))
	require.NoError(t, err)
	_, err = stream.CloseAndRecv()
	require.Error(t, err)
	assert.True(t, status.IsPermissionDeniedError(err), "expected permission denied, got: %v", err)
}

func TestStreamHeartbeat_MissingNode(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	_, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	stream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey("CP_KEY"))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&cppb.RegisterCacheProxyRequest{}))
	_, err = stream.CloseAndRecv()
	require.Error(t, err)
	assert.True(t, status.IsInvalidArgumentError(err), "expected invalid argument, got: %v", err)
}

func TestStreamHeartbeat_MissingID(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	_, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	stream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey("CP_KEY"))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&cppb.RegisterCacheProxyRequest{
		Node: &cppb.CacheProxyNode{Host: "h", ProxyId: ""},
	}))
	_, err = stream.CloseAndRecv()
	require.Error(t, err)
	assert.True(t, status.IsInvalidArgumentError(err), "expected invalid argument, got: %v", err)
}

func TestStreamHeartbeat_AccessRevoked(t *testing.T) {
	// The server snapshots authenticator + clock at construction, so wire
	// the test fakes onto env before calling NewCacheProxyRegistryServer.
	redisTarget := testredis.Start(t).Target
	env := enterprise_testenv.GetCustomTestEnv(t, &enterprise_testenv.Options{
		RedisTarget: redisTarget,
	})

	fakeClock := clockwork.NewFakeClock()
	env.SetClock(fakeClock)

	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	// The authenticator's APIKeyProvider is read by the server goroutine
	// while the test goroutine flips `revoked` — use atomic.Bool so the
	// race detector stays happy.
	var revoked atomic.Bool
	auth := testauth.NewTestAuthenticator(t, nil)
	auth.APIKeyProvider = func(ctx context.Context, apiKey string) (interfaces.UserInfo, error) {
		if revoked.Load() || apiKey != "CP_KEY" {
			return nil, nil
		}
		return user, nil
	}
	env.SetAuthenticator(auth)

	s, err := NewCacheProxyRegistryServer(env)
	require.NoError(t, err)
	env.SetCacheProxyRegistryService(s)

	server, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	cppb.RegisterCacheProxyRegistryServer(server, s)
	go runFunc()

	dialCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	conn, err := testenv.LocalGRPCConn(dialCtx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := cppb.NewCacheProxyRegistryClient(conn)

	stream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey("CP_KEY"))
	require.NoError(t, err)
	// Confirm the stream is up by sending an initial heartbeat.
	require.NoError(t, stream.Send(&cppb.RegisterCacheProxyRequest{
		Node: &cppb.CacheProxyNode{Host: "h", ProxyId: "id"},
	}))

	// Wait until the server-side ticker is registered on the fake clock
	// before we start poking it.
	fakeClock.BlockUntil(1)

	// Revoke. Advancing past the revalidation interval should make the
	// server's next tick fire, fail authorize(), and terminate the stream.
	revoked.Store(true)
	fakeClock.Advance(checkRegistrationCredentialsInterval + time.Second)

	// Wait until the server tears down the stream — Send will return io.EOF
	// once the server has closed its side.
	require.Eventually(t, func() bool {
		return stream.Send(&cppb.RegisterCacheProxyRequest{
			Node: &cppb.CacheProxyNode{Host: "h", ProxyId: "id"},
		}) == io.EOF
	}, 5*time.Second, 25*time.Millisecond, "server did not terminate stream after revocation")

	_, err = stream.CloseAndRecv()
	require.Error(t, err)
	assert.True(t, status.IsUnauthenticatedError(err), "expected unauthenticated, got: %v", err)
}
