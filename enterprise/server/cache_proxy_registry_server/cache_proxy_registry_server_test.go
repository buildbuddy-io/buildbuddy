package cache_proxy_registry_server

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/upgrade"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	cppb "github.com/buildbuddy-io/buildbuddy/proto/cache_proxy"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	uppb "github.com/buildbuddy-io/buildbuddy/proto/upgrade"
)

const (
	testGroupID = "GR1"
	// The group whose proxies are operated by BuildBuddy; the newest version
	// among its registrations sets the bar for upgrade prompts.
	testSharedPoolGroupID = "GR-SHARED"
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
	flags.Set(t, "cache_proxy.shared_pool_group_id", testSharedPoolGroupID)

	s, err := NewCacheProxyRegistryServer(env, testDetector())
	require.NoError(t, err)
	env.SetCacheProxyRegistryService(s)
	return s, env
}

// testDetector prompts an upgrade when a proxy is more than 10 minor
// versions behind the newest registered version, escalating at 20 and 40.
func testDetector() *upgrade.Detector {
	return upgrade.NewDetector(map[uppb.Prompt_Urgency]upgrade.Trigger{
		uppb.Prompt_LOW:    {MaxLag: semver.MustParse("0.10.0")},
		uppb.Prompt_MEDIUM: {MaxLag: semver.MustParse("0.20.0")},
		uppb.Prompt_HIGH:   {MaxLag: semver.MustParse("0.40.0")},
	})
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

func TestListCacheProxies(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, _ := newServer(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	// Insert two proxies out of host-order; expect them returned alphabetically.
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxySummary{
		Host:    "host-b",
		ProxyId: "id-b",
		Version: "1.0",
	}, nil))
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxySummary{
		Host:    "host-a",
		ProxyId: "id-a",
		Version: "1.0",
	}, nil))

	ctx := claims.AuthContextWithJWT(context.Background(), user.(*claims.Claims), nil)
	resp, err := s.ListCacheProxies(ctx, &cppb.ListCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetSummary(), 2)
	assert.Equal(t, "host-a", resp.GetSummary()[0].GetHost())
	assert.Equal(t, "host-b", resp.GetSummary()[1].GetHost())
}

func TestUpgradeTriggersFromFlags(t *testing.T) {
	flags.Set(t, "cache_proxy.upgrade_prompt_max_lags", map[string]string{"low": "0.10.0", "HIGH": "0.40.0"})
	flags.Set(t, "cache_proxy.upgrade_prompt_min_versions", map[string]string{"HIGH": "2.100.0", "critical": "2.50.0"})

	triggers, err := upgradeTriggersFromFlags()
	require.NoError(t, err)
	require.Len(t, triggers, 3)
	assert.Equal(t, "0.10.0", triggers[uppb.Prompt_LOW].MaxLag.String())
	assert.Nil(t, triggers[uppb.Prompt_LOW].MinVersion)
	// Both criteria land on the same trigger.
	assert.Equal(t, "0.40.0", triggers[uppb.Prompt_HIGH].MaxLag.String())
	assert.Equal(t, "2.100.0", triggers[uppb.Prompt_HIGH].MinVersion.String())
	assert.Nil(t, triggers[uppb.Prompt_CRITICAL].MaxLag)
	assert.Equal(t, "2.50.0", triggers[uppb.Prompt_CRITICAL].MinVersion.String())
}

func TestUpgradeTriggersFromFlags_Invalid(t *testing.T) {
	// An urgency that isn't part of the enum.
	flags.Set(t, "cache_proxy.upgrade_prompt_max_lags", map[string]string{"URGENT": "0.10.0"})
	_, err := upgradeTriggersFromFlags()
	require.Error(t, err)

	// UNKNOWN_URGENCY is not a valid prompt level.
	flags.Set(t, "cache_proxy.upgrade_prompt_max_lags", map[string]string{"UNKNOWN_URGENCY": "0.10.0"})
	_, err = upgradeTriggersFromFlags()
	require.Error(t, err)

	// A version value that doesn't parse as semver.
	flags.Set(t, "cache_proxy.upgrade_prompt_max_lags", map[string]string{"LOW": "ten minors"})
	_, err = upgradeTriggersFromFlags()
	require.Error(t, err)

	flags.Set(t, "cache_proxy.upgrade_prompt_max_lags", map[string]string{})
	flags.Set(t, "cache_proxy.upgrade_prompt_min_versions", map[string]string{"LOW": "not-a-version"})
	_, err = upgradeTriggersFromFlags()
	require.Error(t, err)
}

func TestListCacheProxies_UpgradePrompt(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, _ := newServer(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	// The newest version is registered by the shared executor pool group
	// (BuildBuddy's own proxies); the prompt compares against it.
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testSharedPoolGroupID, &cppb.CacheProxySummary{
		Host: "host-new", ProxyId: "id-new", Version: "v2.153.0",
	}, nil))
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxySummary{
		Host: "host-old", ProxyId: "id-old", Version: "v2.140.0",
	}, nil))

	ctx := claims.AuthContextWithJWT(context.Background(), user.(*claims.Claims), nil)
	resp, err := s.ListCacheProxies(ctx, &cppb.ListCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetSummary(), 1)
	require.NotNil(t, resp.GetUpgradePrompt())
	assert.Equal(t, uppb.Prompt_LOW, resp.GetUpgradePrompt().GetUrgency())
}

func TestListCacheProxies_UpgradePrompt_WithinAllowance(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, _ := newServer(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testSharedPoolGroupID, &cppb.CacheProxySummary{
		Host: "host-new", ProxyId: "id-new", Version: "v2.153.0",
	}, nil))
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxySummary{
		Host: "host-old", ProxyId: "id-old", Version: "v2.152.0",
	}, nil))

	ctx := claims.AuthContextWithJWT(context.Background(), user.(*claims.Claims), nil)
	resp, err := s.ListCacheProxies(ctx, &cppb.ListCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	assert.Nil(t, resp.GetUpgradePrompt())
}

func TestListCacheProxies_UpgradePrompt_IgnoresOtherGroups(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, _ := newServer(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	// A newer version registered outside the shared executor pool group
	// shouldn't set the bar.
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), "GR-OTHER", &cppb.CacheProxySummary{
		Host: "host-new", ProxyId: "id-new", Version: "v2.153.0",
	}, nil))
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxySummary{
		Host: "host-old", ProxyId: "id-old", Version: "v2.140.0",
	}, nil))

	ctx := claims.AuthContextWithJWT(context.Background(), user.(*claims.Claims), nil)
	resp, err := s.ListCacheProxies(ctx, &cppb.ListCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	assert.Nil(t, resp.GetUpgradePrompt())
}

func TestListCacheProxies_UpgradePrompt_SkipsUnparseableVersions(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, _ := newServer(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	// Dev builds report "unknown"; they should neither count as the newest
	// version nor trigger the prompt themselves.
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testSharedPoolGroupID, &cppb.CacheProxySummary{
		Host: "host-dev", ProxyId: "id-dev", Version: "unknown",
	}, nil))
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxySummary{
		Host: "host-old", ProxyId: "id-old", Version: "unknown",
	}, nil))

	ctx := claims.AuthContextWithJWT(context.Background(), user.(*claims.Claims), nil)
	resp, err := s.ListCacheProxies(ctx, &cppb.ListCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	assert.Nil(t, resp.GetUpgradePrompt())
}

func TestListCacheProxies_UpgradePrompt_IgnoresStaleRegistrations(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, _ := newServer(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	// A long-gone proxy with a very new version shouldn't set the bar.
	stale := &cppb.RegisteredCacheProxy{
		Summary:      &cppb.CacheProxySummary{Host: "host-new", ProxyId: "id-new", Version: "v2.199.0"},
		GroupId:      testSharedPoolGroupID,
		LastPingTime: timestamppb.New(time.Now().Add(-2 * maxRegistrationStaleness)),
	}
	b, err := proto.Marshal(stale)
	require.NoError(t, err)
	require.NoError(t, s.rdb.HSet(context.Background(), redisKeyForCacheProxies(testSharedPoolGroupID), "id-new", b).Err())
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxySummary{
		Host: "host-old", ProxyId: "id-old", Version: "v2.150.0",
	}, nil))

	ctx := claims.AuthContextWithJWT(context.Background(), user.(*claims.Claims), nil)
	resp, err := s.ListCacheProxies(ctx, &cppb.ListCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	assert.Nil(t, resp.GetUpgradePrompt())
}

func TestListCacheProxies_Isolation(t *testing.T) {
	const otherGroupID = "GR2"
	other := userWithCapabilities("U2", otherGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, _ := newServer(t, map[string]interfaces.UserInfo{"OTHER_KEY": other})

	// A proxy was registered for the original test group.
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxySummary{
		Host: "host", ProxyId: "id",
	}, nil))

	// A user in a different group asks for that other group's proxies. The
	// ACL filter must drop the entry — the response should be empty even
	// though the entry exists in the queried hash.
	ctx := claims.AuthContextWithJWT(context.Background(), other.(*claims.Claims), nil)
	resp, err := s.ListCacheProxies(ctx, &cppb.ListCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	assert.Empty(t, resp.GetSummary(), "user from %q should not see proxies registered under %q", otherGroupID, testGroupID)
}

func TestListCacheProxies_Expiration(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, _ := newServer(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	// Insert a fresh proxy and a stale one directly into Redis.
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxySummary{
		Host: "fresh", ProxyId: "fresh",
	}, nil))
	stale := &cppb.RegisteredCacheProxy{
		Summary:      &cppb.CacheProxySummary{Host: "stale", ProxyId: "stale"},
		GroupId:      testGroupID,
		LastPingTime: timestamppb.New(time.Now().Add(-2 * maxRegistrationStaleness)),
	}
	b, err := proto.Marshal(stale)
	require.NoError(t, err)
	require.NoError(t, s.rdb.HSet(context.Background(), redisKeyForCacheProxies(testGroupID), "stale", b).Err())

	ctx := claims.AuthContextWithJWT(context.Background(), user.(*claims.Claims), nil)
	resp, err := s.ListCacheProxies(ctx, &cppb.ListCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetSummary(), 1)
	assert.Equal(t, "fresh", resp.GetSummary()[0].GetHost())
}

func startGRPCRegistry(t *testing.T, users map[string]interfaces.UserInfo) (*CacheProxyRegistryServer, cppb.CacheProxyRegistryClient) {
	s, env := newServer(t, users)

	server, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	cppb.RegisterCacheProxyRegistryServer(server, s)
	go runFunc()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	conn, err := testenv.LocalGRPCConn(t, ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return s, cppb.NewCacheProxyRegistryClient(conn)
}

// closeAndWait closes the stream and waits for the server to finish it,
// returning the stream's final status (nil if the server returned OK).
func closeAndWait(stream cppb.CacheProxyRegistry_RegisterAndStreamHeartbeatClient) error {
	if err := stream.CloseSend(); err != nil {
		return err
	}
	for {
		if _, err := stream.Recv(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func TestStreamHeartbeat_PersistsRegistration(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	stream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey("CP_KEY"))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&cppb.RegisterCacheProxyRequest{
		Summary: &cppb.CacheProxySummary{
			Host: "proxy-1", ProxyId: "id-1", Version: "v1",
		},
	}))
	require.NoError(t, closeAndWait(stream))

	// The heartbeat should be visible via ListCacheProxies (which uses the
	// AuthenticatedUser context, so we build one directly).
	ctx := claims.AuthContextWithJWT(context.Background(), user.(*claims.Claims), nil)
	listResp, err := s.ListCacheProxies(ctx, &cppb.ListCacheProxiesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
	})
	require.NoError(t, err)
	require.Len(t, listResp.GetSummary(), 1)
	assert.Equal(t, "id-1", listResp.GetSummary()[0].GetProxyId())

	// The stored registration should record which app instance holds the
	// stream, so other instances can route requests to it.
	data, err := s.rdb.HGet(ctx, redisKeyForCacheProxies(testGroupID), "id-1").Result()
	require.NoError(t, err)
	reg := &cppb.RegisteredCacheProxy{}
	require.NoError(t, proto.Unmarshal([]byte(data), reg))
	assert.Equal(t, s.ownHostPort, reg.GetAppHostPort())
	assert.NotEmpty(t, s.ownHostPort)
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
		_ = closeAndWait(keepStream)
	})
	require.NoError(t, keepStream.Send(&cppb.RegisterCacheProxyRequest{
		Summary: &cppb.CacheProxySummary{Host: "h-keep", ProxyId: "id-keep"},
	}))

	shutdownStream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey("CP_KEY"))
	require.NoError(t, err)
	require.NoError(t, shutdownStream.Send(&cppb.RegisterCacheProxyRequest{
		Summary: &cppb.CacheProxySummary{Host: "h-bye", ProxyId: "id-bye"},
	}))

	// stream.Send only buffers — the server-side recv→insertOrUpdateProxy
	// runs on a separate goroutine, so we have to wait for the registry to
	// reflect both heartbeats before sending the shutdown signal.
	ctx := claims.AuthContextWithJWT(context.Background(), readerUser.(*claims.Claims), nil)
	require.Eventually(t, func() bool {
		resp, err := s.ListCacheProxies(ctx, &cppb.ListCacheProxiesRequest{
			RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
		})
		return err == nil && len(resp.GetSummary()) == 2
	}, 2*time.Second, 25*time.Millisecond, "both proxies should have registered")

	// Now send ShuttingDown=true on the second stream only.
	require.NoError(t, shutdownStream.Send(&cppb.RegisterCacheProxyRequest{
		Summary:      &cppb.CacheProxySummary{Host: "h-bye", ProxyId: "id-bye"},
		ShuttingDown: true,
	}))
	err = closeAndWait(shutdownStream)
	require.NoError(t, err)

	// Wait for the registry to reflect the removal of only the shutting-down
	// proxy. The other one must still be present.
	require.Eventually(t, func() bool {
		resp, err := s.ListCacheProxies(ctx, &cppb.ListCacheProxiesRequest{
			RequestContext: &ctxpb.RequestContext{GroupId: testGroupID},
		})
		if err != nil || len(resp.GetSummary()) != 1 {
			return false
		}
		return resp.GetSummary()[0].GetProxyId() == "id-keep"
	}, 2*time.Second, 25*time.Millisecond, "only the shutting-down proxy should have been removed")
}

func TestStreamHeartbeat_Anonymous(t *testing.T) {
	_, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{})

	stream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey(""))
	require.NoError(t, err)
	err = closeAndWait(stream)
	require.Error(t, err)
	assert.True(t, status.IsUnauthenticatedError(err), "expected unauthenticated, got: %v", err)
}

func TestStreamHeartbeat_Unauthorized(t *testing.T) {
	noCap := userWithCapabilities("U1", testGroupID, cappb.Capability_CACHE_WRITE)
	_, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"NOCAP_KEY": noCap})

	stream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey("NOCAP_KEY"))
	require.NoError(t, err)
	err = closeAndWait(stream)
	require.Error(t, err)
	assert.True(t, status.IsPermissionDeniedError(err), "expected permission denied, got: %v", err)
}

func TestStreamHeartbeat_MissingSummary(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	_, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	stream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey("CP_KEY"))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&cppb.RegisterCacheProxyRequest{}))
	err = closeAndWait(stream)
	require.Error(t, err)
	assert.True(t, status.IsInvalidArgumentError(err), "expected invalid argument, got: %v", err)
}

func TestStreamHeartbeat_MissingID(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	_, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	stream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey("CP_KEY"))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&cppb.RegisterCacheProxyRequest{
		Summary: &cppb.CacheProxySummary{Host: "h", ProxyId: ""},
	}))
	err = closeAndWait(stream)
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

	s, err := NewCacheProxyRegistryServer(env, upgrade.NewDetector(nil))
	require.NoError(t, err)
	env.SetCacheProxyRegistryService(s)

	server, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	cppb.RegisterCacheProxyRegistryServer(server, s)
	go runFunc()

	dialCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	conn, err := testenv.LocalGRPCConn(t, dialCtx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := cppb.NewCacheProxyRegistryClient(conn)

	stream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey("CP_KEY"))
	require.NoError(t, err)
	// Confirm the stream is up by sending an initial heartbeat.
	require.NoError(t, stream.Send(&cppb.RegisterCacheProxyRequest{
		Summary: &cppb.CacheProxySummary{Host: "h", ProxyId: "id"},
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
			Summary: &cppb.CacheProxySummary{Host: "h", ProxyId: "id"},
		}) == io.EOF
	}, 5*time.Second, 25*time.Millisecond, "server did not terminate stream after revocation")

	err = closeAndWait(stream)
	require.Error(t, err)
	assert.True(t, status.IsUnauthenticatedError(err), "expected unauthenticated, got: %v", err)
}

// registerProxy opens a registration stream for the given proxy, sends an
// initial heartbeat, and waits until the server is tracking the stream.
func registerProxy(t *testing.T, s *CacheProxyRegistryServer, client cppb.CacheProxyRegistryClient, proxyID string) cppb.CacheProxyRegistry_RegisterAndStreamHeartbeatClient {
	ctx, cancel := context.WithCancel(ctxWithOutgoingAPIKey("CP_KEY"))
	t.Cleanup(cancel)
	stream, err := client.RegisterAndStreamHeartbeat(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&cppb.RegisterCacheProxyRequest{
		Summary: &cppb.CacheProxySummary{Host: "host", ProxyId: proxyID},
	}))
	require.Eventually(t, func() bool {
		return s.lookupStream(testGroupID, proxyID) != nil
	}, 2*time.Second, 10*time.Millisecond, "server did not track registration stream")
	return stream
}

func getCacheProxyRequest(proxyID string) *cppb.GetCacheProxyRequest {
	return &cppb.GetCacheProxyRequest{
		RequestContext:         &ctxpb.RequestContext{GroupId: testGroupID},
		Selector:               &cppb.CacheProxySelector{ProxyId: proxyID},
		IncludeConfiguredFlags: true,
		IncludeStatistics:      true,
	}
}

func TestGetCacheProxy_ReturnsDetails(t *testing.T) {
	streamUser := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	// A UI user: a member of the proxy's group with no capabilities.
	readerUser := userWithCapabilities("U2", testGroupID)
	s, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"CP_KEY": streamUser})
	stream := registerProxy(t, s, client, "id-1")

	// Play the proxy: answer the details request, interleaved with a
	// heartbeat that the server must not mistake for the answer.
	proxyErr := make(chan error, 1)
	go func() {
		rsp, err := stream.Recv()
		if err != nil {
			proxyErr <- err
			return
		}
		summary := &cppb.CacheProxySummary{Host: "host", ProxyId: "id-1"}
		if err := stream.Send(&cppb.RegisterCacheProxyRequest{Summary: summary}); err != nil {
			proxyErr <- err
			return
		}
		details := &cppb.CacheProxyDetails{Summary: summary}
		if rsp.GetDetailsRequest().GetIncludeConfiguredFlags() {
			details.ConfiguredFlags = []string{"--foo=bar"}
		}
		if rsp.GetDetailsRequest().GetIncludeStatistics() {
			details.Statistics = &cppb.Statistics{AcReadHits: 42}
		}
		proxyErr <- stream.Send(&cppb.RegisterCacheProxyRequest{
			Summary:   summary,
			Details:   details,
			RequestId: rsp.GetRequestId(),
		})
	}()

	ctx := claims.AuthContextWithJWT(context.Background(), readerUser.(*claims.Claims), nil)
	rsp, err := s.GetCacheProxy(ctx, getCacheProxyRequest("id-1"))
	require.NoError(t, err)
	require.NoError(t, <-proxyErr)
	assert.Equal(t, "id-1", rsp.GetDetails().GetSummary().GetProxyId())
	assert.NotNil(t, rsp.GetDetails().GetSummary().GetLastCheckInTime())
	assert.Equal(t, []string{"--foo=bar"}, rsp.GetDetails().GetConfiguredFlags())
	assert.Equal(t, int64(42), rsp.GetDetails().GetStatistics().GetAcReadHits())
}

func TestGetCacheProxy_NotConnected(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, _ := newServer(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	// The proxy is registered in Redis, but its stream is held elsewhere.
	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxySummary{
		Host: "host", ProxyId: "id-1",
	}, nil))

	ctx := claims.AuthContextWithJWT(context.Background(), user.(*claims.Claims), nil)
	for _, doNotForward := range []bool{false, true} {
		req := getCacheProxyRequest("id-1")
		req.DoNotForward = doNotForward
		_, err := s.GetCacheProxy(ctx, req)
		require.Error(t, err)
		assert.True(t, status.IsNotFoundError(err), "expected not found, got: %v", err)
	}
}

func TestGetCacheProxy_OtherGroup(t *testing.T) {
	const otherGroupID = "GR2"
	streamUser := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	other := userWithCapabilities("U2", otherGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"CP_KEY": streamUser})
	registerProxy(t, s, client, "id-1")

	ctx := claims.AuthContextWithJWT(context.Background(), other.(*claims.Claims), nil)
	_, err := s.GetCacheProxy(ctx, getCacheProxyRequest("id-1"))
	require.Error(t, err)
	assert.True(t, status.IsPermissionDeniedError(err), "expected permission denied, got: %v", err)

	// Asking for the proxy under the caller's own group must not find it
	// either.
	req := getCacheProxyRequest("id-1")
	req.RequestContext.GroupId = otherGroupID
	_, err = s.GetCacheProxy(ctx, req)
	require.Error(t, err)
	assert.True(t, status.IsNotFoundError(err), "expected not found, got: %v", err)
}

func TestGetCacheProxy_StreamClosed(t *testing.T) {
	streamUser := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	// A UI user: a member of the proxy's group with no capabilities.
	readerUser := userWithCapabilities("U2", testGroupID)
	s, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"CP_KEY": streamUser})
	stream := registerProxy(t, s, client, "id-1")

	// The proxy receives the details request, then disconnects without
	// answering.
	go func() {
		if _, err := stream.Recv(); err != nil {
			return
		}
		_ = closeAndWait(stream)
	}()

	ctx := claims.AuthContextWithJWT(context.Background(), readerUser.(*claims.Claims), nil)
	_, err := s.GetCacheProxy(ctx, getCacheProxyRequest("id-1"))
	require.Error(t, err)
	assert.True(t, status.IsUnavailableError(err), "expected unavailable, got: %v", err)
	assert.Nil(t, s.lookupStream(testGroupID, "id-1"))
}

func TestGetCacheProxy_Reconnect(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	oldStream := registerProxy(t, s, client, "id-1")
	oldHandle := s.lookupStream(testGroupID, "id-1")

	// The proxy reconnects before the old stream is torn down.
	newStream, err := client.RegisterAndStreamHeartbeat(ctxWithOutgoingAPIKey("CP_KEY"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeAndWait(newStream) })
	require.NoError(t, newStream.Send(&cppb.RegisterCacheProxyRequest{
		Summary: &cppb.CacheProxySummary{Host: "host", ProxyId: "id-1"},
	}))
	require.Eventually(t, func() bool {
		return s.lookupStream(testGroupID, "id-1") != oldHandle
	}, 2*time.Second, 10*time.Millisecond, "server did not track new registration stream")
	newHandle := s.lookupStream(testGroupID, "id-1")

	// Closing the old stream must not untrack the new one.
	require.NoError(t, closeAndWait(oldStream))
	assert.Same(t, newHandle, s.lookupStream(testGroupID, "id-1"))
}

func TestStreamHeartbeat_ProxyIDChanged(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	stream := registerProxy(t, s, client, "id-1")
	require.NoError(t, stream.Send(&cppb.RegisterCacheProxyRequest{
		Summary: &cppb.CacheProxySummary{Host: "host", ProxyId: "id-2"},
	}))
	err := closeAndWait(stream)
	require.Error(t, err)
	assert.True(t, status.IsInvalidArgumentError(err), "expected invalid argument, got: %v", err)
}

func TestStreamHeartbeat_ShutDownWithChangedProxyID(t *testing.T) {
	user := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	s, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"CP_KEY": user})

	require.NoError(t, s.insertOrUpdateProxy(context.Background(), testGroupID, &cppb.CacheProxySummary{
		Host: "host", ProxyId: "id-2",
	}, nil))

	// A stream registered as id-1 must not be able to remove id-2.
	stream := registerProxy(t, s, client, "id-1")
	require.NoError(t, stream.Send(&cppb.RegisterCacheProxyRequest{
		Summary:      &cppb.CacheProxySummary{Host: "host", ProxyId: "id-2"},
		ShuttingDown: true,
	}))
	err := closeAndWait(stream)
	require.Error(t, err)
	assert.True(t, status.IsInvalidArgumentError(err), "expected invalid argument, got: %v", err)

	exists, err := s.rdb.HExists(context.Background(), redisKeyForCacheProxies(testGroupID), "id-2").Result()
	require.NoError(t, err)
	assert.True(t, exists, "id-2's registration should not have been removed")
}

func TestGetCacheProxy_CallerCanceled(t *testing.T) {
	streamUser := userWithCapabilities("U1", testGroupID, cappb.Capability_REGISTER_CACHE_PROXY)
	// A UI user: a member of the proxy's group with no capabilities.
	readerUser := userWithCapabilities("U2", testGroupID)
	s, client := startGRPCRegistry(t, map[string]interfaces.UserInfo{"CP_KEY": streamUser})
	stream := registerProxy(t, s, client, "id-1")

	ctx, cancel := context.WithCancel(claims.AuthContextWithJWT(context.Background(), readerUser.(*claims.Claims), nil))
	// The proxy receives the details request but never answers; the caller
	// gives up.
	go func() {
		if _, err := stream.Recv(); err == nil {
			cancel()
		}
	}()

	_, err := s.GetCacheProxy(ctx, getCacheProxyRequest("id-1"))
	require.Error(t, err)
	assert.True(t, status.IsCanceledError(err), "expected canceled, got: %v", err)
}
