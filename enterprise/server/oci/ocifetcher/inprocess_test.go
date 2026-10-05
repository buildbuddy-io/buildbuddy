package ocifetcher_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
)

func newUncachedInProcessClient(t *testing.T) ofpb.OCIFetcherClient {
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.0/8", "::1/128"})
	server, err := ocifetcher.NewInProcessServer(nil, nil)
	require.NoError(t, err)
	return ocifetcher.NewInProcessClient(server)
}

func readBlob(t *testing.T, stream ofpb.OCIFetcher_FetchBlobClient) []byte {
	var buf bytes.Buffer
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return buf.Bytes()
		}
		require.NoError(t, err)
		buf.Write(resp.GetData())
	}
}

func TestNewInProcessServerRequiresBothOrNeitherCacheClient(t *testing.T) {
	_, bsClient, acClient := setupCacheEnv(t)
	_, err := ocifetcher.NewInProcessServer(bsClient, nil)
	require.Error(t, err)
	_, err = ocifetcher.NewInProcessServer(nil, acClient)
	require.Error(t, err)
}

// TestUncachedInProcessServer verifies that a server without a cache goes
// straight to the registry, without the extra HEAD requests that the cached
// server needs to check access and to size blobs for the cache.
func TestUncachedInProcessServer(t *testing.T) {
	reg, counter := setupRegistry(t, nil, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	expectedManifest, err := img.RawManifest()
	require.NoError(t, err)
	layers, err := img.Layers()
	require.NoError(t, err)
	require.Len(t, layers, 1)
	layerDigest, err := layers[0].Digest()
	require.NoError(t, err)
	expectedLayer := layerData(t, layers[0])

	client := newUncachedInProcessClient(t)
	ctx := context.Background()
	counter.Reset()

	for range 2 {
		manifestResp, err := client.FetchManifest(ctx, &ofpb.FetchManifestRequest{Ref: imageName})
		require.NoError(t, err)
		require.Equal(t, expectedManifest, manifestResp.GetManifest())

		// bypass_registry is ignored without a cache.
		stream, err := client.FetchBlob(ctx, &ofpb.FetchBlobRequest{
			Ref:            imageName + "@" + layerDigest.String(),
			BypassRegistry: true,
		})
		require.NoError(t, err)
		require.Equal(t, expectedLayer, readBlob(t, stream))
	}

	// Nothing is cached, so the second round of fetches goes to the registry
	// again. The puller is reused, so there is only one ping.
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                                         1,
		http.MethodGet + " /v2/test-image/manifests/latest":              2,
		http.MethodGet + " /v2/test-image/blobs/" + layerDigest.String(): 2,
	})
}

func TestInProcessClientFetchBlobError(t *testing.T) {
	reg, _ := setupRegistry(t, nil, nil)
	imageName, _ := reg.PushNamedImage(t, "test-image", nil)
	client := newUncachedInProcessClient(t)

	stream, err := client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{
		Ref: imageName + "@sha256:0000000000000000000000000000000000000000000000000000000000000000",
	})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Error(t, err)
	require.NotEqual(t, io.EOF, err)
}

// TestInProcessClientFetchBlobCancel verifies that canceling the context of
// a FetchBlob call that hasn't been read to the end stops the server instead
// of leaving it blocked on a send.
func TestInProcessClientFetchBlobCancel(t *testing.T) {
	reg, _ := setupRegistry(t, nil, nil)
	imageName, img := reg.PushNamedImageWithMultipleLayers(t, "test-image", nil)
	layers, err := img.Layers()
	require.NoError(t, err)
	layerDigest, err := layers[0].Digest()
	require.NoError(t, err)
	client := newUncachedInProcessClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref: imageName + "@" + layerDigest.String(),
	})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)
	cancel()

	// Once the server returns, Recv returns its error (or EOF, if it had
	// already sent everything) rather than blocking.
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
}
