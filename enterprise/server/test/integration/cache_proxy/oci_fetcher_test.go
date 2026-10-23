package cache_proxy_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocicache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testkeys"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// failingOCIFetcherClient stands in for the apps' OCIFetcher, counting and
// failing every call.
type failingOCIFetcherClient struct {
	calls atomic.Int32
}

func (c *failingOCIFetcherClient) fail() error {
	c.calls.Add(1)
	return status.UnimplementedError("apps OCIFetcher should not be called")
}

func (c *failingOCIFetcherClient) FetchManifest(context.Context, *ofpb.FetchManifestRequest, ...grpc.CallOption) (*ofpb.FetchManifestResponse, error) {
	return nil, c.fail()
}

func (c *failingOCIFetcherClient) FetchManifestMetadata(context.Context, *ofpb.FetchManifestMetadataRequest, ...grpc.CallOption) (*ofpb.FetchManifestMetadataResponse, error) {
	return nil, c.fail()
}

func (c *failingOCIFetcherClient) FetchBlobMetadata(context.Context, *ofpb.FetchBlobMetadataRequest, ...grpc.CallOption) (*ofpb.FetchBlobMetadataResponse, error) {
	return nil, c.fail()
}

func (c *failingOCIFetcherClient) FetchBlob(context.Context, *ofpb.FetchBlobRequest, ...grpc.CallOption) (ofpb.OCIFetcher_FetchBlobClient, error) {
	return nil, c.fail()
}

// TestOCIFetcher_LocalFetcher checks that, with the local fetcher enabled, the
// cache proxy serves OCIFetcher requests without the apps' OCIFetcher, and
// that the fetched blob is cached in both the apps' cache and the proxy's
// local cache.
func TestOCIFetcher_LocalFetcher(t *testing.T) {
	flags.Set(t, "cache_proxy.oci_fetcher.use_local_fetcher", true)
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.0/8", "::1/128"})
	flags.Set(t, "app.client_identity.client", "cache-proxy")
	flags.Set(t, "app.client_identity.key", "key")
	keyPair := testkeys.GenerateES256KeyPair(t)
	flags.Set(t, "auth.jwt_es256_private_key", keyPair.PrivateKeyPEM)
	flags.Set(t, "auth.remote.use_es256_jwts", true)
	flags.Set(t, "auth.reparse_jwts", false)
	require.NoError(t, claims.Init())

	reg := testregistry.Run(t, testregistry.Opts{})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	layers, err := img.Layers()
	require.NoError(t, err)
	layerDigest, err := layers[0].Digest()
	require.NoError(t, err)
	rc, err := layers[0].Compressed()
	require.NoError(t, err)
	layerBytes, err := io.ReadAll(rc)
	require.NoError(t, err)
	blobRef, err := name.NewDigest(imageName + "@" + layerDigest.String())
	require.NoError(t, err)

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	apps := &failingOCIFetcherClient{}
	var proxyEnv *testenv.TestEnv
	cp := rbe.AddCacheProxyWithOptions(&rbetest.CacheProxyOptions{
		EnvModifier: func(env *testenv.TestEnv) {
			proxyEnv = env
			// Give the proxy its own local cache, so that the ProxyLocalCache
			// check below only passes if the proxy itself wrote the blob.
			mc, err := memory_cache.NewMemoryCache(100_000_000)
			require.NoError(t, err)
			env.SetCache(mc)
			env.SetOCIFetcherClient(apps)
			// Unused: legacy FetchBlob fails on the apps before reading it.
			env.SetLocalByteStreamClient(bspb.NewByteStreamClient(nil))
		},
	})

	ctx := metadata.AppendToOutgoingContext(context.Background(), authutil.APIKeyHeader, rbe.APIKey1)
	client := ofpb.NewOCIFetcherClient(cp.Conn())
	_, err = client.FetchManifest(ctx, &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	_, err = client.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{Ref: blobRef.String()})
	require.NoError(t, err)
	stream, err := client.FetchBlob(ctx, &ofpb.FetchBlobRequest{Ref: blobRef.String()})
	require.NoError(t, err)
	var got []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, resp.GetData()...)
	}
	require.Equal(t, layerBytes, got)
	require.Equal(t, int32(0), apps.calls.Load())

	// Read the blob's cache entries as the cache proxy would, with its client
	// identity.
	dial := func(target string) grpc.ClientConnInterface {
		conn, err := grpc_client.DialInternalWithoutPooling(proxyEnv, target)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	appsConn := dial(rbe.AppProxy.GRPCTarget())
	proxyConn := dial(fmt.Sprintf("grpc://localhost:%d", cp.Port))
	for _, tc := range []struct {
		name string
		ctx  context.Context
		bs   bspb.ByteStreamClient
	}{
		{name: "AppsCache", ctx: ctx, bs: bspb.NewByteStreamClient(appsConn)},
		{name: "ProxyLocalCache", ctx: proxy_util.SetSkipRemote(ctx), bs: bspb.NewByteStreamClient(proxyConn)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The blob's _bb_ociregistry_ action result lives in the apps'
			// action cache; its outputs are read from tc.bs.
			md, err := ocicache.FetchBlobMetadataFromCache(tc.ctx, tc.bs, repb.NewActionCacheClient(appsConn), blobRef.Context(), layerDigest)
			require.NoError(t, err)
			var buf bytes.Buffer
			require.NoError(t, ocicache.FetchBlobFromCache(tc.ctx, &buf, tc.bs, layerDigest, md.GetContentLength()))
			require.Equal(t, layerBytes, buf.Bytes())
		})
	}
}
