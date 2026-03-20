package cache_proxy_test

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/action_cache_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/atime_updater"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/byte_stream_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/capabilities_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/content_addressable_storage_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remoteauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testkeys"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/golang-jwt/jwt/v4"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
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

type casRequestRecorder struct {
	findMissingCount atomic.Int32
	batchReadCount   atomic.Int32

	mu                sync.Mutex
	lastFindMissingMD metadata.MD
}

func (r *casRequestRecorder) recordFindMissingMetadata(md metadata.MD) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastFindMissingMD = md.Copy()
}

func (r *casRequestRecorder) lastMetadata() metadata.MD {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastFindMissingMD.Copy()
}

func recordCASRequests(rec *casRequestRecorder) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		switch req.(type) {
		case *repb.FindMissingBlobsRequest:
			rec.findMissingCount.Add(1)
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				rec.recordFindMissingMetadata(md)
			}
		case *repb.BatchReadBlobsRequest:
			rec.batchReadCount.Add(1)
		}
		return handler(ctx, req)
	}
}

func newPebbleBackedProxyEnv(t *testing.T, clock clockwork.Clock) *testenv.TestEnv {
	env := testenv.GetTestEnv(t)
	env.SetClock(clock)

	atimeThreshold := time.Duration(0)
	atimeBufferSize := 0
	atimeWorkers := 1
	cache, err := pebble_cache.NewPebbleCache(env, &pebble_cache.Options{
		RootDirectory:         testfs.MakeTempDir(t),
		MaxSizeBytes:          1_000_000_000,
		AtimeUpdateThreshold:  &atimeThreshold,
		AtimeBufferSize:       &atimeBufferSize,
		NumAtimeUpdateWorkers: &atimeWorkers,
		Clock:                 clock,
	})
	require.NoError(t, err)
	require.NoError(t, cache.Start())
	t.Cleanup(func() {
		require.NoError(t, cache.Stop())
	})
	env.SetCache(cache)
	return env
}

func addCacheProxyWithEnv(t *testing.T, proxyEnv *testenv.TestEnv, appTarget string) *grpc_client.ClientConnPool {
	appConn, err := grpc_client.DialSimple(appTarget)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, appConn.Close())
	})

	grpcServerConfig := grpc_server.GRPCServerConfig{
		ExtraChainedUnaryInterceptors: []grpc.UnaryServerInterceptor{
			interceptors.PropagateMetadataUnaryInterceptor(proxy_util.HeadersToPropagate...),
		},
		ExtraChainedStreamInterceptors: []grpc.StreamServerInterceptor{
			interceptors.PropagateMetadataStreamInterceptor(proxy_util.HeadersToPropagate...),
		},
	}

	authenticator, err := remoteauth.NewWithTarget(proxyEnv, appConn)
	require.NoError(t, err)
	t.Cleanup(authenticator.Stop)
	proxyEnv.SetAuthenticator(authenticator)

	proxyEnv.SetActionCacheClient(repb.NewActionCacheClient(appConn))
	proxyEnv.SetByteStreamClient(bspb.NewByteStreamClient(appConn))
	proxyEnv.SetCapabilitiesClient(repb.NewCapabilitiesClient(appConn))
	proxyEnv.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(appConn))
	require.NoError(t, atime_updater.Register(proxyEnv))

	localBSS, err := byte_stream_server.NewByteStreamServer(proxyEnv)
	require.NoError(t, err)
	proxyEnv.SetLocalByteStreamServer(localBSS)
	localCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(proxyEnv)
	require.NoError(t, err)
	proxyEnv.SetLocalCASServer(localCAS)

	port := testport.FindFree(t)
	s, err := grpc_server.New(proxyEnv, port, false, grpcServerConfig)
	require.NoError(t, err)
	require.NoError(t, capabilities_server_proxy.Register(proxyEnv))
	require.NoError(t, action_cache_server_proxy.Register(proxyEnv))
	require.NoError(t, byte_stream_server_proxy.Register(proxyEnv))
	require.NoError(t, content_addressable_storage_server_proxy.Register(proxyEnv))
	repb.RegisterActionCacheServer(s.GetServer(), proxyEnv.GetActionCacheServer())
	bspb.RegisterByteStreamServer(s.GetServer(), proxyEnv.GetByteStreamServer())
	repb.RegisterContentAddressableStorageServer(s.GetServer(), proxyEnv.GetCASServer())
	repb.RegisterCapabilitiesServer(s.GetServer(), proxyEnv.GetCapabilitiesServer())
	require.NoError(t, s.Start())

	conn, err := grpc_client.DialSimple(fmt.Sprintf("grpc://localhost:%d", port))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})
	return conn
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

func TestBatchRead_LocalCacheHit_PropagatesAuthToRemoteAtimeUpdate(t *testing.T) {
	flags.Set(t, "cache_proxy.remote_atime_update_interval", time.Minute)

	recorder := &casRequestRecorder{}
	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServerWithOptions(&rbetest.BuildBuddyServerOptions{
		GRPCServerConfig: grpc_server.GRPCServerConfig{
			ExtraChainedUnaryInterceptors: []grpc.UnaryServerInterceptor{recordCASRequests(recorder)},
		},
	})

	clock := clockwork.NewFakeClock()
	proxyEnv := newPebbleBackedProxyEnv(t, clock)
	cas := repb.NewContentAddressableStorageClient(addCacheProxyWithEnv(t, proxyEnv, rbe.GetBuildBuddyServerTarget()))

	rn, blob := testdigest.RandomCASResourceBuf(t, 1024)
	ctx := metadata.AppendToOutgoingContext(t.Context(), authutil.APIKeyHeader, rbe.APIKey1)

	writeResp, err := cas.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
		InstanceName:   rn.GetInstanceName(),
		DigestFunction: rn.GetDigestFunction(),
		Requests: []*repb.BatchUpdateBlobsRequest_Request{
			{
				Digest: rn.GetDigest(),
				Data:   blob,
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, writeResp.GetResponses(), 1)
	require.Equal(t, int32(codes.OK), writeResp.GetResponses()[0].GetStatus().GetCode())

	readResp, err := cas.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{
		InstanceName:   rn.GetInstanceName(),
		DigestFunction: rn.GetDigestFunction(),
		Digests:        []*repb.Digest{rn.GetDigest()},
	})
	require.NoError(t, err)
	require.Len(t, readResp.GetResponses(), 1)
	require.Equal(t, int32(codes.OK), readResp.GetResponses()[0].GetStatus().GetCode())
	require.Equal(t, blob, readResp.GetResponses()[0].GetData())
	require.Equal(t, int32(0), recorder.batchReadCount.Load(), "read should be served from the proxy's local cache")

	clock.Advance(time.Minute)
	require.Eventually(t, func() bool {
		return recorder.findMissingCount.Load() == 1
	}, time.Second, 10*time.Millisecond, "expected a backend FindMissingBlobs request for the async atime update")

	md := recorder.lastMetadata()
	require.NotEmpty(t, md.Get(authutil.ContextTokenStringKey), "expected the async atime update to carry a JWT to the backend")
}
