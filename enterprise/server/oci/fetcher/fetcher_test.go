package fetcher_test

import (
	"context"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/fetcher"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
)

func newFetcher(t *testing.T) *fetcher.LocalOCIFetcherClient {
	// Allow both IPv4 and IPv6 loopback since testregistry may bind to either
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32", "::1/128"})
	f, err := fetcher.New()
	require.NoError(t, err)
	return f
}

func TestFetchManifest(t *testing.T) {
	registry := testregistry.Run(t, testregistry.Opts{})
	imageName, img := registry.PushNamedImage(t, "test_manifest")

	expectedManifest, err := img.RawManifest()
	require.NoError(t, err)

	f := newFetcher(t)
	resp, err := f.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{
		Ref: imageName,
	})
	require.NoError(t, err)
	require.Equal(t, expectedManifest, resp.GetManifest())
}

func TestFetchManifestMetadata(t *testing.T) {
	registry := testregistry.Run(t, testregistry.Opts{})
	imageName, img := registry.PushNamedImage(t, "test_metadata")

	expectedDigest, err := img.Digest()
	require.NoError(t, err)

	f := newFetcher(t)
	resp, err := f.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
	})
	require.NoError(t, err)
	require.Equal(t, expectedDigest.String(), resp.GetDigest())
	require.Greater(t, resp.GetSize(), int64(0))
	require.NotEmpty(t, resp.GetMediaType())
}

func TestFetchBlob(t *testing.T) {
	registry := testregistry.Run(t, testregistry.Opts{})
	_, img := registry.PushNamedImage(t, "test_blob")

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)

	layer := layers[0]
	digest, err := layer.Digest()
	require.NoError(t, err)

	// Get expected bytes
	expectedReader, err := layer.Compressed()
	require.NoError(t, err)
	expectedBytes, err := io.ReadAll(expectedReader)
	require.NoError(t, err)
	expectedReader.Close()

	// Construct the blob reference: registry/repo@sha256:...
	blobRef := registry.ImageAddress("test_blob") + "@" + digest.String()

	f := newFetcher(t)
	stream, err := f.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{
		Ref: blobRef,
	})
	require.NoError(t, err)

	// Read all chunks from stream
	var actualBytes []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		actualBytes = append(actualBytes, resp.GetData()...)
	}

	require.Equal(t, expectedBytes, actualBytes)
}

func TestFetchBlobMetadata(t *testing.T) {
	registry := testregistry.Run(t, testregistry.Opts{})
	_, img := registry.PushNamedImage(t, "test_blob_metadata")

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)

	layer := layers[0]
	digest, err := layer.Digest()
	require.NoError(t, err)

	expectedSize, err := layer.Size()
	require.NoError(t, err)

	// Construct the blob reference
	blobRef := registry.ImageAddress("test_blob_metadata") + "@" + digest.String()

	f := newFetcher(t)
	resp, err := f.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{
		Ref: blobRef,
	})
	require.NoError(t, err)
	require.Equal(t, expectedSize, resp.GetSizeBytes())
	require.NotEmpty(t, resp.GetMediaType())
}

func TestCanAccess(t *testing.T) {
	registry := testregistry.Run(t, testregistry.Opts{})
	imageName, _ := registry.PushNamedImage(t, "test_access")

	f := newFetcher(t)
	resp, err := f.CanAccess(context.Background(), &ofpb.CanAccessRequest{
		Reference: imageName,
	})
	require.NoError(t, err)
	require.True(t, resp.GetCanAccess())
}

func TestCanAccess_NonExistent(t *testing.T) {
	registry := testregistry.Run(t, testregistry.Opts{})
	// Don't push an image - just try to access a non-existent one
	imageName := registry.ImageAddress("nonexistent")

	f := newFetcher(t)
	_, err := f.CanAccess(context.Background(), &ofpb.CanAccessRequest{
		Reference: imageName,
	})
	// Should return an error for non-existent image
	require.Error(t, err)
}
