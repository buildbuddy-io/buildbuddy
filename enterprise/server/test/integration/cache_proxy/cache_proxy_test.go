package cache_proxy_test

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
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

	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

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

func TestAtimeUpdater_AuthHeadersPropagated(t *testing.T) {
	// This test exercises the code path where auth headers are captured
	// via GetAuthHeaders, stored, and later reconstructed on a new context
	// via AddAuthHeadersToContext. The reconstructed context must support
	// GetAuthHeaders (for the batch operator's Enqueue check) and must
	// carry the JWT in outgoing gRPC metadata (for dispatch calls to the
	// remote CAS).
	//
	// This is the exact pattern used in pebble_cache.updateAtime and
	// batch_operator.dispatch.
	authenticator := &testauth.TestAuthenticator{}

	// Simulate auth headers captured from a request context. In
	// production, these are captured by ContextWithCachedAuthHeaders
	// during request processing. When the request only has JWT auth (no
	// ClientIdentityHeader), the headers contain only ContextTokenStringKey.
	originalHeaders := map[string][]string{
		authutil.ContextTokenStringKey: {"test-jwt-token"},
	}

	// Reconstruct a context from background + saved auth headers. This is
	// what pebble_cache.updateAtime (line 998) and batch_operator.dispatch
	// (line 577) do.
	reconstructed := authutil.AddAuthHeadersToContext(
		context.Background(), originalHeaders, authenticator)

	// 1. Verify GetAuthHeaders round-trip works (batch_operator.Enqueue
	//    checks this at line 323; failure causes the "Dropping batch op
	//    due to missing auth headers" log).
	recovered := authutil.GetAuthHeaders(reconstructed)
	require.NotEmpty(t, recovered,
		"GetAuthHeaders must find headers on a context created by "+
			"AddAuthHeadersToContext (needed for batch_operator.Enqueue)")
	require.Equal(t, originalHeaders[authutil.ContextTokenStringKey],
		recovered[authutil.ContextTokenStringKey],
		"JWT must survive the GetAuthHeaders -> AddAuthHeadersToContext "+
			"-> GetAuthHeaders round-trip")

	// 2. Verify JWT is in outgoing gRPC metadata (batch_operator.dispatch
	//    passes this context to the atime updater's FindMissingBlobs call;
	//    without the JWT in outgoing metadata, the remote server can't
	//    authenticate the request).
	outMD, _ := metadata.FromOutgoingContext(reconstructed)
	require.NotEmpty(t, outMD.Get(authutil.ContextTokenStringKey),
		"JWT must be in outgoing gRPC metadata so downstream RPCs "+
			"from the batch operator are authenticated")
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
