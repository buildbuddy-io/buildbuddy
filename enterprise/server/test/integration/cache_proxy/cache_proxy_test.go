package cache_proxy_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testkeys"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
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

	const clientIP = "1.2.3.4"

	backendConn, err := grpc_client.DialSimple(backend.GRPCAddress())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backendConn.Close()) })

	enableIPRules(t, bbspb.NewBuildBuddyServiceClient(backendConn), rbe.WithUserID(t.Context(), rbe.UserID1), rbe.GroupID1, clientIP)

	cas := rbe.AddCacheProxy().GetContentAddressableStorageClient()
	ctx := metadata.AppendToOutgoingContext(t.Context(),
		authutil.APIKeyHeader, rbe.APIKey1,
		"X-Forwarded-For", clientIP,
	)
	_, err = cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{{Hash: strings.Repeat("a", 64), SizeBytes: 1}},
	})
	require.NoError(t, err, "FindMissingBlobs via cache proxy should succeed for allowed client IP %q", clientIP)
}
