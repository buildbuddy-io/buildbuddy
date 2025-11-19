package oci_fetch_server_proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
)

func TestNew_MissingDependencies(t *testing.T) {
	env := testenv.GetTestEnv(t)

	// Should fail without ActionCacheClient
	env.SetActionCacheClient(nil)
	_, err := New(env)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ActionCacheClient")

	// Should fail without ByteStreamClient
	env.SetByteStreamClient(nil)
	_, err = New(env)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ByteStreamClient")
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
