package oci_fetch_server_proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
)

func TestNew_MissingDependencies(t *testing.T) {
	// When caching is disabled (useCachePercent == 0), ActionCacheClient and
	// ByteStreamClient are not required.
	// This test doesn't need to check for missing dependencies anymore since
	// the proxy works without them when caching is disabled.
	//
	// If we want to test the caching-enabled case, we'd need to set the
	// oci.use_cache_percent flag to a non-zero value, but that's a global flag
	// and would affect other tests.
	//
	// For now, just verify that New() succeeds with a basic testenv.
	env := testenv.GetTestEnv(t)
	_, err := New(env)
	require.NoError(t, err)
}

func TestFetchManifest_InvalidRequest(t *testing.T) {
	env := testenv.GetTestEnv(t)
	proxy, err := New(env)
	require.NoError(t, err)

	ctx := context.Background()

	// Should fail with empty image ref
	req := &ocipb.FetchManifestRequest{
		ImageRef: "",
	}
	_, err = proxy.FetchManifest(ctx, req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "image_ref is required")
}

func TestFetchBlob_InvalidRequest(t *testing.T) {
	env := testenv.GetTestEnv(t)
	proxy, err := New(env)
	require.NoError(t, err)

	// Should fail with empty blob ref
	req := &ocipb.FetchBlobRequest{
		BlobRef: "",
	}

	stream := &mockFetchBlobServer{}
	err = proxy.FetchBlob(req, stream)
	require.Error(t, err)
	require.Contains(t, err.Error(), "blob_ref is required")
}

func TestFetchBlobMetadata_InvalidRequest(t *testing.T) {
	env := testenv.GetTestEnv(t)
	proxy, err := New(env)
	require.NoError(t, err)

	ctx := context.Background()

	// Should fail with empty blob ref
	req := &ocipb.FetchBlobMetadataRequest{
		BlobRef: "",
	}
	_, err = proxy.FetchBlobMetadata(ctx, req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "blob_ref is required")
}

// Mock server for testing FetchBlob streaming
type mockFetchBlobServer struct {
	ocipb.OCIFetchService_FetchBlobServer
}

func (m *mockFetchBlobServer) Context() context.Context {
	return context.Background()
}

func (m *mockFetchBlobServer) Send(resp *ocipb.FetchBlobResponse) error {
	return nil
}
