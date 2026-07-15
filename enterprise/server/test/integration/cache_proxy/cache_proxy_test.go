package cache_proxy_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/action_cache_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/byte_stream_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/capabilities_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/content_addressable_storage_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testkeys"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/golang-jwt/jwt/v4"
	"github.com/jonboulle/clockwork"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/pkg/openfeature/memprovider"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	bspb "github.com/buildbuddy-io/buildbuddy/proto/bytestream"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	iprpb "github.com/buildbuddy-io/buildbuddy/proto/iprules"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func enableIPRules(t *testing.T, client bbspb.BuildBuddyServiceClient, ctx context.Context, groupID, clientIP string) {
	t.Helper()

	ctx = metadata.AppendToOutgoingContext(ctx, "X-Forwarded-For", clientIP)

	_, err := client.AddIPRule(ctx, &iprpb.AddRuleRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
		Rule: &iprpb.IPRule{
			Cidr:        clientIP + "/32",
			Description: "allow forwarded client IP",
		},
	})
	require.NoError(t, err)
	_, err = client.SetIPRulesConfig(ctx, &iprpb.SetRulesConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
		EnforceIpRules: true,
	})
	require.NoError(t, err)
}

func enableEncryption(t *testing.T, rbe *rbetest.Env, userID string) {
	ctx := rbe.WithUserID(t.Context(), userID)
	client := rbe.GetBuildBuddyServiceClient()
	_, err := client.SetEncryptionConfig(ctx, &enpb.SetEncryptionConfigRequest{
		Enabled: true,
		KmsConfig: &enpb.KMSConfig{
			LocalInsecureKmsConfig: &enpb.LocalInsecureKMSConfig{KeyId: "groupKey"},
		},
	})
	require.NoError(t, err, "Could not enable encryption")

	getResp, err := client.GetEncryptionConfig(ctx, &enpb.GetEncryptionConfigRequest{})
	require.NoError(t, err)
	require.True(t, getResp.Enabled, "Encryption should be enabled")
}

func writeBlob(t *testing.T, ctx context.Context, bs bspb.ByteStreamClient, uploadID, hash string, sizeBytes int64, data []byte) error {
	t.Helper()
	stream, err := bs.Write(ctx)
	require.NoError(t, err)
	err = stream.Send(&bspb.WriteRequest{
		ResourceName: fmt.Sprintf("uploads/%s/blobs/%s/%d", uploadID, hash, sizeBytes),
		Data:         data,
		FinishWrite:  true,
	})
	if err != nil && err != io.EOF {
		return err
	}
	_, err = stream.CloseAndRecv()
	return err
}

func readBlob(t *testing.T, ctx context.Context, bs bspb.ByteStreamClient, hash string, sizeBytes int64) []byte {
	t.Helper()
	stream, err := bs.Read(ctx, &bspb.ReadRequest{
		ResourceName: fmt.Sprintf("blobs/%s/%d", hash, sizeBytes),
	})
	require.NoError(t, err)
	var data []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return data
		}
		require.NoError(t, err)
		data = append(data, resp.GetData()...)
	}
}

func TestES256Auth(t *testing.T) {
	keyPair := testkeys.GenerateES256KeyPair(t)
	flags.Set(t, "auth.jwt_es256_private_key", keyPair.PrivateKeyPEM)
	flags.Set(t, "auth.remote.use_es256_jwts", true)
	flags.Set(t, "auth.reparse_jwts", false)
	require.NoError(t, claims.Init())

	var mu sync.Mutex
	var capturedJWT string
	jwtInterceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if vals := md.Get(authutil.ContextTokenStringKey); len(vals) > 0 {
				mu.Lock()
				capturedJWT = vals[len(vals)-1]
				mu.Unlock()
			}
		}
		return handler(ctx, req)
	}

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServerWithOptions(&rbetest.BuildBuddyServerOptions{
		GRPCServerConfig: grpc_server.GRPCServerConfig{
			ExtraChainedUnaryInterceptors: []grpc.UnaryServerInterceptor{jwtInterceptor},
		},
	})

	cas := rbe.AddCacheProxy().GetContentAddressableStorageClient()
	req := repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{
			{Hash: strings.Repeat("a", 64), SizeBytes: 1},
		},
	}

	ctx := metadata.AppendToOutgoingContext(t.Context(), authutil.APIKeyHeader, rbe.APIKey1)
	resp, err := cas.FindMissingBlobs(ctx, &req)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.MissingBlobDigests))

	mu.Lock()
	jwtStr := capturedJWT
	mu.Unlock()
	require.NotEmpty(t, jwtStr, "expected a JWT to be forwarded to the backend")

	token, _, err := new(jwt.Parser).ParseUnverified(jwtStr, &claims.Claims{})
	require.NoError(t, err)
	require.Equal(t, "ES256", token.Method.Alg())
}

func TestES256Auth_RemoteExecution(t *testing.T) {
	keyPair := testkeys.GenerateES256KeyPair(t)
	flags.Set(t, "auth.jwt_es256_private_key", keyPair.PrivateKeyPEM)
	flags.Set(t, "auth.remote.use_es256_jwts", true)
	flags.Set(t, "auth.reparse_jwts", false)
	require.NoError(t, claims.Init())

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	proxy := rbe.AddCacheProxy()
	conn, err := grpc_client.DialSimple(
		fmt.Sprintf("grpc://localhost:%d", proxy.Port))
	require.NoError(t, err)
	rbe.AddExecutorWithOptions(t, &rbetest.ExecutorOptions{
		Name:      "executor",
		CacheConn: conn,
	})

	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{"sh", "-c", "echo hello"},
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "container-image", Value: "none"},
				{Name: "OSFamily", Value: runtime.GOOS},
				{Name: "Arch", Value: runtime.GOARCH},
			},
		},
	}, &rbetest.ExecuteOpts{APIKey: rbe.APIKey1})
	res := cmd.Wait()
	require.Equal(t, 0, res.ExitCode)
	require.Equal(t, "hello\n", res.Stdout)
}

func TestByteStreamWriteSkipValidationDoesNotPoisonLocalCache(t *testing.T) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache_proxy.skip_write_validation": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true": true,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))
	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	proxy := rbe.AddCacheProxyWithOptions(&rbetest.CacheProxyOptions{
		EnvModifier: func(env *testenv.TestEnv) {
			env.SetExperimentFlagProvider(fp)
		},
	})
	bs := proxy.GetByteStreamClient()

	ctx := metadata.AppendToOutgoingContext(t.Context(), authutil.APIKeyHeader, rbe.APIKey1)
	correct := bytes.Repeat([]byte("correct content"), 3_000)
	corrupt := bytes.Clone(correct)
	corrupt[0] = ^corrupt[0]
	digest := sha256.Sum256(correct)
	hash := hex.EncodeToString(digest[:])
	sizeBytes := int64(len(correct))

	err = writeBlob(t, ctx, bs, "2148e1f1-aacc-41eb-a31c-22b6da7c7ac1", hash, sizeBytes, corrupt)
	require.True(t, status.IsInvalidArgumentError(err), "err = %v", err)

	err = writeBlob(t, ctx, bs, "2148e1f1-aacc-41eb-a31c-22b6da7c7ac2", hash, sizeBytes, correct)
	require.NoError(t, err)

	require.Equal(t, correct, readBlob(t, ctx, bs, hash, sizeBytes))
}

func TestFindMissing_Encryption(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()

	enableEncryption(t, rbe, rbe.UserID1)

	cas := rbe.AddCacheProxy().GetContentAddressableStorageClient()
	req := repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{
			&repb.Digest{Hash: strings.Repeat("a", 64), SizeBytes: 1},
		},
	}

	ctx := metadata.AppendToOutgoingContext(t.Context(), authutil.APIKeyHeader, rbe.APIKey1)
	resp, err := cas.FindMissingBlobs(ctx, &req)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.MissingBlobDigests))
}

func TestAtimeUpdateAuth(t *testing.T) {
	// Use a short batch interval so atime updates flush quickly.
	flags.Set(t, "cache_proxy.remote_atime_update_interval", 100*time.Millisecond)

	// Track FindMissingBlobs calls arriving at the backend.
	type fmbCall struct {
		digests []*repb.Digest
		hasJWT  bool
	}
	var mu sync.Mutex
	var fmbCalls []fmbCall
	interceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if strings.HasSuffix(info.FullMethod, "/FindMissingBlobs") {
			fmb := req.(*repb.FindMissingBlobsRequest)
			md, _ := metadata.FromIncomingContext(ctx)
			mu.Lock()
			fmbCalls = append(fmbCalls, fmbCall{
				digests: fmb.BlobDigests,
				hasJWT:  len(md.Get(authutil.ContextTokenStringKey)) > 0,
			})
			mu.Unlock()
		}
		return handler(ctx, req)
	}

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServerWithOptions(&rbetest.BuildBuddyServerOptions{
		GRPCServerConfig: grpc_server.GRPCServerConfig{
			ExtraChainedUnaryInterceptors: []grpc.UnaryServerInterceptor{interceptor},
		},
	})

	// Use a pebble cache for the proxy so the asynchronous atime update
	// code path (sendAtimeUpdate -> updateAtime -> Enqueue) is exercised.
	proxy := rbe.AddCacheProxyWithOptions(&rbetest.CacheProxyOptions{
		EnvModifier: func(env *testenv.TestEnv) {
			atimeThreshold := time.Duration(0)
			pc, err := pebble_cache.NewPebbleCache(env, &pebble_cache.Options{
				RootDirectory:        testfs.MakeTempDir(t),
				MaxSizeBytes:         1_000_000_000,
				AtimeUpdateThreshold: &atimeThreshold,
			})
			require.NoError(t, err)
			require.NoError(t, pc.Start())
			t.Cleanup(func() {
				require.NoError(t, pc.Stop())
			})
			env.SetCache(pc)
		},
	})
	cas := proxy.GetContentAddressableStorageClient()

	// Create a blob with a valid SHA-256 digest.
	blob := []byte("hello, atime update test")
	h := sha256.Sum256(blob)
	d := &repb.Digest{
		Hash:      hex.EncodeToString(h[:]),
		SizeBytes: int64(len(blob)),
	}

	ctx := metadata.AppendToOutgoingContext(t.Context(), authutil.APIKeyHeader, rbe.APIKey1)

	// Write through the proxy so the blob lands in both the local pebble
	// cache and the remote backend.
	updateResp, err := cas.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{
			{Digest: d, Data: blob},
		},
	})
	require.NoError(t, err)
	require.Len(t, updateResp.Responses, 1)
	require.EqualValues(t, 0, updateResp.Responses[0].Status.Code)

	// Reset interceptor state so we only observe atime-related calls.
	mu.Lock()
	fmbCalls = nil
	mu.Unlock()

	// Read through the proxy. The blob is in the local cache, so this is a
	// local hit that triggers an asynchronous atime update to the backend.
	readResp, err := cas.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{
		Digests: []*repb.Digest{d},
	})
	require.NoError(t, err)
	require.Len(t, readResp.Responses, 1)

	// Wait until the atime batch flushes and FindMissingBlobs arrives at
	// the backend carrying a JWT for the read digest, or time out after 5 s.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, call := range fmbCalls {
			for _, bd := range call.digests {
				if bd.Hash == d.Hash {
					// Only succeed once we observe the digest with a JWT.
					return call.hasJWT
				}
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond,
		"expected the atime updater to send FindMissingBlobs for the read digest carrying a JWT auth header; auth headers were likely not propagated to the async atime update")
}

func TestIPRules(t *testing.T) {
	flags.Set(t, "auth.ip_rules.enable", true)
	flags.Set(t, "auth.trust_xforwardedfor_header", true)
	flags.Set(t, "app.client_identity.client", "cache-proxy")
	flags.Set(t, "app.client_identity.key", "key")
	flags.Set(t, "auth.ip_rules.permitted_clients", []string{"cache-proxy"})

	rbe := rbetest.NewRBETestEnv(t)
	backend := rbe.AddBuildBuddyServer()

	flags.Set(t, "auth.ip_rules.remote.target", backend.GRPCAddress())

	const allowedIP = "1.2.3.4"
	const blockedIP = "9.9.9.9"

	backendConn, err := grpc_client.DialSimple(backend.GRPCAddress())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backendConn.Close()) })

	enableIPRules(t, bbspb.NewBuildBuddyServiceClient(backendConn), rbe.WithUserID(t.Context(), rbe.UserID1), rbe.GroupID1, allowedIP)
	cas := rbe.AddCacheProxy().GetContentAddressableStorageClient()

	// Verify that requests from allowedIP succeed.
	ctx := metadata.AppendToOutgoingContext(t.Context(),
		authutil.APIKeyHeader, rbe.APIKey1,
		"X-Forwarded-For", allowedIP,
	)
	_, err = cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{{Hash: strings.Repeat("a", 64), SizeBytes: 1}},
	})
	require.NoError(t, err, "Requests from allowedIP should succeed")

	// Verify that requests from blockedIP fail.
	ctx = metadata.AppendToOutgoingContext(t.Context(),
		authutil.APIKeyHeader, rbe.APIKey1,
		"X-Forwarded-For", blockedIP,
	)
	_, err = cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{{Hash: strings.Repeat("a", 64), SizeBytes: 1}},
	})
	require.Error(t, err, "Requests from blockedIP should fail")
}

func TestIPRulesRBE(t *testing.T) {
	flags.Set(t, "auth.ip_rules.enable", true)
	flags.Set(t, "auth.trust_xforwardedfor_header", true)
	flags.Set(t, "app.client_identity.client", "cache-proxy")
	flags.Set(t, "app.client_identity.key", "key")

	rbe := rbetest.NewRBETestEnv(t)
	backend := rbe.AddBuildBuddyServer()

	flags.Set(t, "auth.ip_rules.remote.target", backend.GRPCAddress())

	const allowedIP = "1.2.3.4"
	const blockedIP = "9.9.9.9"

	backendConn, err := grpc_client.DialSimple(backend.GRPCAddress())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backendConn.Close()) })
	enableIPRules(t, bbspb.NewBuildBuddyServiceClient(backendConn),
		rbe.WithUserID(t.Context(), rbe.UserID1), rbe.GroupID1, allowedIP)
	cas := rbe.AddCacheProxy().GetContentAddressableStorageClient()
	cis, err := clientidentity.New(clockwork.NewRealClock())
	require.NoError(t, err)

	// RBE Requests from a blockedIP + allowed client identity should succeed.
	allowedIdentity, err := cis.NewIdentityHeader(&interfaces.ClientIdentity{
		Client: interfaces.ClientIdentityExecutor,
		Origin: "test",
	}, time.Minute)
	require.NoError(t, err)
	ctx := metadata.AppendToOutgoingContext(t.Context(),
		authutil.APIKeyHeader, rbe.APIKey1,
		authutil.ClientIdentityHeaderName, allowedIdentity,
		"X-Forwarded-For", blockedIP,
	)
	_, err = cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{{Hash: strings.Repeat("a", 64), SizeBytes: 1}},
	})
	require.NoError(t, err, "RBE requests from allowed client should succeed")

	// RBE Requests from a blockedIP + blocked client identity should fail.
	// Sign some other client-identity using the shared signing key.
	blockedIdentity, err := cis.NewIdentityHeader(&interfaces.ClientIdentity{
		Client: "foo",
		Origin: "test",
	}, time.Minute)
	require.NoError(t, err)
	ctx = metadata.AppendToOutgoingContext(t.Context(),
		authutil.APIKeyHeader, rbe.APIKey1,
		authutil.ClientIdentityHeaderName, blockedIdentity,
		"X-Forwarded-For", blockedIP,
	)
	_, err = cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{{Hash: strings.Repeat("a", 64), SizeBytes: 1}},
	})
	require.Error(t, err, "RBE requests from blocked client should fail")
}

func BenchmarkActionCacheTTLFullBuildWithProxyAppLatency(b *testing.B) {
	const actionCount = 25
	const proxyAppDelay = 75 * time.Millisecond
	oldLogLevel := *log.LogLevel
	*log.LogLevel = "error"
	require.NoError(b, log.Configure())
	b.Cleanup(func() {
		*log.LogLevel = oldLogLevel
		require.NoError(b, log.Configure())
	})
	flags.Set(b, "cache.check_client_action_result_digests", true)

	for _, test := range []struct {
		name       string
		ttlSeconds int
	}{
		{name: "ttl_disabled", ttlSeconds: 0},
		{name: "ttl_enabled", ttlSeconds: 60},
	} {
		b.Run(test.name, func(b *testing.B) {
			var backendACGets atomic.Int64
			latencyInterceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				if info.FullMethod == "/build.bazel.remote.execution.v2.ActionCache/GetActionResult" {
					backendACGets.Add(1)
					time.Sleep(proxyAppDelay)
				}
				return handler(ctx, req)
			}

			backendEnv := testenv.GetTestEnv(b)
			backendPort := testport.FindFree(b)
			backendServer, err := grpc_server.New(backendEnv, backendPort, false, grpc_server.GRPCServerConfig{
				ExtraChainedUnaryInterceptors: []grpc.UnaryServerInterceptor{latencyInterceptor},
			})
			require.NoError(b, err)
			backendBS, err := byte_stream_server.NewByteStreamServer(backendEnv)
			require.NoError(b, err)
			bspb.RegisterByteStreamServer(backendServer.GetServer(), backendBS)
			backendCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(backendEnv)
			require.NoError(b, err)
			repb.RegisterContentAddressableStorageServer(backendServer.GetServer(), backendCAS)
			backendAC, err := action_cache_server.NewActionCacheServer(backendEnv)
			require.NoError(b, err)
			repb.RegisterActionCacheServer(backendServer.GetServer(), backendAC)
			repb.RegisterCapabilitiesServer(backendServer.GetServer(), capabilities_server.NewCapabilitiesServer(backendEnv, true, true, true))
			require.NoError(b, backendServer.Start())

			proxyEnv := testenv.GetTestEnv(b)
			atimeThreshold := time.Duration(0)
			pc, err := pebble_cache.NewPebbleCache(proxyEnv, &pebble_cache.Options{
				RootDirectory:        testfs.MakeTempDir(b),
				MaxSizeBytes:         1_000_000_000,
				AtimeUpdateThreshold: &atimeThreshold,
			})
			require.NoError(b, err)
			require.NoError(b, pc.Start())
			b.Cleanup(func() {
				require.NoError(b, pc.Stop())
			})
			proxyEnv.SetCache(pc)

			testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
				"cache_proxy.action_cache_ttl_seconds": {
					State:          memprovider.Enabled,
					DefaultVariant: "enabled",
					Variants: map[string]any{
						"enabled": test.ttlSeconds,
					},
				},
			})
			require.NoError(b, openfeature.SetNamedProviderAndWait(b.Name(), testProvider))
			fp, err := experiments.NewFlagProvider(b.Name())
			require.NoError(b, err)
			proxyEnv.SetExperimentFlagProvider(fp)

			backendTarget := fmt.Sprintf("grpc://localhost:%d", backendPort)
			conn, err := grpc_client.DialSimple(backendTarget)
			require.NoError(b, err)
			b.Cleanup(func() {
				require.NoError(b, conn.Close())
			})
			proxyEnv.SetActionCacheClient(repb.NewActionCacheClient(conn))
			proxyEnv.SetByteStreamClient(bspb.NewByteStreamClient(conn))
			proxyEnv.SetCapabilitiesClient(repb.NewCapabilitiesClient(conn))
			proxyEnv.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))

			localBSS, err := byte_stream_server.NewByteStreamServer(proxyEnv)
			require.NoError(b, err)
			proxyEnv.SetLocalByteStreamServer(localBSS)
			localCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(proxyEnv)
			require.NoError(b, err)
			proxyEnv.SetLocalCASServer(localCAS)
			localAC, err := action_cache_server.NewActionCacheServer(proxyEnv)
			require.NoError(b, err)
			proxyEnv.SetLocalActionCacheServer(localAC)

			proxyPort := testport.FindFree(b)
			proxyServer, err := grpc_server.New(proxyEnv, proxyPort, false, grpc_server.GRPCServerConfig{})
			require.NoError(b, err)
			require.NoError(b, capabilities_server_proxy.Register(proxyEnv))
			require.NoError(b, action_cache_server_proxy.Register(proxyEnv))
			require.NoError(b, byte_stream_server_proxy.Register(proxyEnv))
			require.NoError(b, content_addressable_storage_server_proxy.Register(proxyEnv))
			repb.RegisterActionCacheServer(proxyServer.GetServer(), proxyEnv.GetActionCacheServer())
			bspb.RegisterByteStreamServer(proxyServer.GetServer(), proxyEnv.GetByteStreamServer())
			repb.RegisterContentAddressableStorageServer(proxyServer.GetServer(), proxyEnv.GetCASServer())
			repb.RegisterCapabilitiesServer(proxyServer.GetServer(), proxyEnv.GetCapabilitiesServer())
			require.NoError(b, proxyServer.Start())

			var buildFile strings.Builder
			for i := 0; i < actionCount; i++ {
				fmt.Fprintf(&buildFile, "genrule(name = \"out_%03d\", outs = [\"out_%03d.txt\"], cmd = \"echo out_%03d > $@\")\n", i, i, i)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			workspace := testbazel.MakeTempModule(b, map[string]string{"BUILD": buildFile.String()})
			buildFlags := []string{
				"//:all",
				"--jobs=1",
				"--remote_timeout=30s",
				fmt.Sprintf("--remote_cache=grpc://localhost:%d", proxyPort),
			}

			build := func() {
				result := testbazel.Invoke(ctx, b, workspace, "build", buildFlags...)
				require.NoError(b, result.Error, "stdout: %s\nstderr: %s", result.Stdout, result.Stderr)
			}
			clean := func() {
				result := testbazel.Clean(ctx, b, workspace)
				require.NoError(b, result.Error, "stdout: %s\nstderr: %s", result.Stdout, result.Stderr)
			}

			build()
			clean()
			build()

			var measuredBackendACGets int64
			var measuredBuildTime time.Duration
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				clean()
				before := backendACGets.Load()
				start := time.Now()
				b.StartTimer()
				build()
				b.StopTimer()
				measuredBuildTime += time.Since(start)
				measuredBackendACGets += backendACGets.Load() - before
			}

			backendACGetsPerOp := float64(measuredBackendACGets) / float64(b.N)
			backendDelayMsPerOp := backendACGetsPerOp * float64(proxyAppDelay/time.Millisecond)
			buildMsPerOp := float64(measuredBuildTime) / float64(b.N) / float64(time.Millisecond)
			b.ReportMetric(backendACGetsPerOp, "backend_ac_gets/op")
			b.ReportMetric(backendDelayMsPerOp, "proxy_app_delay_ms/op")
			b.ReportMetric(buildMsPerOp, "build_ms/op")
			b.Logf("ttl_seconds=%d build_ms/op=%.1f backend_ac_gets/op=%.1f proxy_app_delay_ms/op=%.1f", test.ttlSeconds, buildMsPerOp, backendACGetsPerOp, backendDelayMsPerOp)
			if test.ttlSeconds == 0 {
				require.Positive(b, measuredBackendACGets)
			} else {
				require.Zero(b, measuredBackendACGets)
			}
		})
	}
}
