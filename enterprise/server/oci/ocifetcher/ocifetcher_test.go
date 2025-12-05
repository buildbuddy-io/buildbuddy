package ocifetcher_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/httpcounter"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-cmp/cmp"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/stretchr/testify/require"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
)

func TestFetchManifestMetadata_NoAuth(t *testing.T) {
	counter := httpcounter.New()
	imageName, img := pushTestImage(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	}, nil)
	digest, err := img.Digest()
	require.NoError(t, err)
	size, err := img.Size()
	require.NoError(t, err)
	mediaType, err := img.MediaType()
	require.NoError(t, err)

	counter.Reset()
	client := newTestClient(t)
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)

	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expectedRequests := map[string]int{
		"GET /v2/":                              1,
		"HEAD /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expectedRequests, counter.Snapshot()))
}

func TestFetchManifestMetadata_WithValidCredentials(t *testing.T) {
	counter := httpcounter.New()
	creds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
	imageName, img := pushTestImage(t, testregistry.Opts{
		Creds: creds,
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	}, creds)
	digest, err := img.Digest()
	require.NoError(t, err)
	size, err := img.Size()
	require.NoError(t, err)
	mediaType, err := img.MediaType()
	require.NoError(t, err)

	counter.Reset()
	client := newTestClient(t)
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
		Credentials: &rgpb.Credentials{
			Username: "testuser",
			Password: "testpass",
		},
	})
	require.NoError(t, err)

	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expectedRequests := map[string]int{
		"GET /v2/":                              1,
		"HEAD /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expectedRequests, counter.Snapshot()))
}

func TestFetchManifestMetadata_WithInvalidCredentials(t *testing.T) {
	counter := httpcounter.New()
	creds := &testregistry.BasicAuthCreds{
		Username: "testuser",
		Password: "testpass",
	}
	imageName, _ := pushTestImage(t, testregistry.Opts{
		Creds: creds,
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	}, creds)

	counter.Reset()
	client := newTestClient(t)
	_, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
		Credentials: &rgpb.Credentials{
			Username: "wronguser",
			Password: "wrongpass",
		},
	})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied error, got: %v", err)

	expectedRequests := map[string]int{
		"GET /v2/":                              1,
		"HEAD /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expectedRequests, counter.Snapshot()))
}

func TestFetchManifestMetadata_MissingCredentials(t *testing.T) {
	counter := httpcounter.New()
	creds := &testregistry.BasicAuthCreds{
		Username: "testuser",
		Password: "testpass",
	}
	imageName, _ := pushTestImage(t, testregistry.Opts{
		Creds: creds,
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	}, creds)

	counter.Reset()
	client := newTestClient(t)
	_, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
		// No credentials provided
	})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied error, got: %v", err)

	expectedRequests := map[string]int{
		"GET /v2/":                              1,
		"HEAD /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expectedRequests, counter.Snapshot()))
}

func TestFetchManifest_NoAuth(t *testing.T) {
	counter := httpcounter.New()
	imageName, img := pushTestImage(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	}, nil)
	digest, err := img.Digest()
	require.NoError(t, err)
	size, err := img.Size()
	require.NoError(t, err)
	mediaType, err := img.MediaType()
	require.NoError(t, err)

	counter.Reset()
	client := newTestClient(t)
	resp, err := client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{
		Ref: imageName,
	})
	require.NoError(t, err)

	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())
	require.NotEmpty(t, resp.GetManifest())

	expectedRequests := map[string]int{
		"GET /v2/":                             1,
		"GET /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expectedRequests, counter.Snapshot()))
}

func TestFetchManifest_WithValidCredentials(t *testing.T) {
	counter := httpcounter.New()
	creds := &testregistry.BasicAuthCreds{
		Username: "testuser",
		Password: "testpass",
	}
	imageName, img := pushTestImage(t, testregistry.Opts{
		Creds: creds,
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	}, creds)
	digest, err := img.Digest()
	require.NoError(t, err)
	size, err := img.Size()
	require.NoError(t, err)
	mediaType, err := img.MediaType()
	require.NoError(t, err)

	counter.Reset()
	client := newTestClient(t)
	resp, err := client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{
		Ref: imageName,
		Credentials: &rgpb.Credentials{
			Username: "testuser",
			Password: "testpass",
		},
	})
	require.NoError(t, err)

	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())
	require.NotEmpty(t, resp.GetManifest())

	expectedRequests := map[string]int{
		"GET /v2/":                             1,
		"GET /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expectedRequests, counter.Snapshot()))
}

func TestFetchManifest_WithInvalidCredentials(t *testing.T) {
	counter := httpcounter.New()
	creds := &testregistry.BasicAuthCreds{
		Username: "testuser",
		Password: "testpass",
	}
	imageName, _ := pushTestImage(t, testregistry.Opts{
		Creds: creds,
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	}, creds)

	counter.Reset()
	client := newTestClient(t)
	_, err := client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{
		Ref: imageName,
		Credentials: &rgpb.Credentials{
			Username: "wronguser",
			Password: "wrongpass",
		},
	})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied error, got: %v", err)

	expectedRequests := map[string]int{
		"GET /v2/":                             1,
		"GET /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expectedRequests, counter.Snapshot()))
}

func TestFetchBlob_NoAuth(t *testing.T) {
	counter := httpcounter.New()
	imageName, img := pushTestImage(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	}, nil)
	layer := firstLayer(t, img)
	layerDigest, err := layer.Digest()
	require.NoError(t, err)
	blobRef := blobRefForLayer(t, imageName, layer)
	expectedData := layerBytes(t, layer)

	counter.Reset()
	client := newTestClient(t)
	stream, err := client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{
		Ref: blobRef,
	})
	require.NoError(t, err)

	var data []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		data = append(data, resp.GetData()...)
	}

	require.Empty(t, cmp.Diff(expectedData, data))

	expectedRequests := map[string]int{
		"GET /v2/": 1,
		"GET /v2/test-image/blobs/" + layerDigest.String(): 1,
	}
	require.Empty(t, cmp.Diff(expectedRequests, counter.Snapshot()))
}

func TestReadBlob_NoAuth(t *testing.T) {
	counter := httpcounter.New()
	imageName, img := pushTestImage(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	}, nil)
	layer := firstLayer(t, img)
	layerDigest, err := layer.Digest()
	require.NoError(t, err)
	blobRef := blobRefForLayer(t, imageName, layer)
	expectedData := layerBytes(t, layer)

	counter.Reset()
	client := newTestClient(t)
	rc, err := ocifetcher.ReadBlob(context.Background(), client, blobRef, nil)
	require.NoError(t, err)
	defer rc.Close()

	actualData, err := io.ReadAll(rc)
	require.NoError(t, err)

	require.Empty(t, cmp.Diff(expectedData, actualData))

	expectedRequests := map[string]int{
		"GET /v2/": 1,
		"GET /v2/test-image/blobs/" + layerDigest.String(): 1,
	}
	require.Empty(t, cmp.Diff(expectedRequests, counter.Snapshot()))
}

func TestReadBlob_WithValidCredentials(t *testing.T) {
	counter := httpcounter.New()
	creds := &testregistry.BasicAuthCreds{
		Username: "testuser",
		Password: "testpass",
	}
	imageName, img := pushTestImage(t, testregistry.Opts{
		Creds: creds,
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	}, creds)
	layer := firstLayer(t, img)
	layerDigest, err := layer.Digest()
	require.NoError(t, err)
	blobRef := blobRefForLayer(t, imageName, layer)

	counter.Reset()
	client := newTestClient(t)
	rc, err := ocifetcher.ReadBlob(context.Background(), client, blobRef, &rgpb.Credentials{
		Username: "testuser",
		Password: "testpass",
	})
	require.NoError(t, err)
	defer rc.Close()

	_, err = io.ReadAll(rc)
	require.NoError(t, err)

	expectedRequests := map[string]int{
		"GET /v2/": 1,
		"GET /v2/test-image/blobs/" + layerDigest.String(): 1,
	}
	require.Empty(t, cmp.Diff(expectedRequests, counter.Snapshot()))
}

func TestReadBlob_WithInvalidCredentials(t *testing.T) {
	counter := httpcounter.New()
	creds := &testregistry.BasicAuthCreds{
		Username: "testuser",
		Password: "testpass",
	}
	imageName, img := pushTestImage(t, testregistry.Opts{
		Creds: creds,
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	}, creds)
	layer := firstLayer(t, img)
	layerDigest, err := layer.Digest()
	require.NoError(t, err)
	blobRef := blobRefForLayer(t, imageName, layer)

	counter.Reset()
	client := newTestClient(t)
	_, err = ocifetcher.ReadBlob(context.Background(), client, blobRef, &rgpb.Credentials{
		Username: "wronguser",
		Password: "wrongpass",
	})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied error, got: %v", err)

	expectedRequests := map[string]int{
		"GET /v2/": 1,
		"GET /v2/test-image/blobs/" + layerDigest.String(): 1,
	}
	require.Empty(t, cmp.Diff(expectedRequests, counter.Snapshot()))
}

func TestReadBlob_BufferSizes(t *testing.T) {
	counter := httpcounter.New()
	imageName, img := pushTestImage(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	}, nil)
	layer := firstLayer(t, img)
	layerDigest, err := layer.Digest()
	require.NoError(t, err)
	blobRef := blobRefForLayer(t, imageName, layer)
	expectedData := layerBytes(t, layer)

	client := newTestClient(t)

	readSizes := []int{
		1,           // tiny reads
		64,          // small reads
		1024,        // medium reads
		32*1024 - 1, // just under chunk size
		32 * 1024,   // exactly chunk size
		32*1024 + 1, // just over chunk size
		64 * 1024,   // larger than chunk
	}

	counter.Reset()
	for _, readSize := range readSizes {
		rc, err := ocifetcher.ReadBlob(context.Background(), client, blobRef, nil)
		require.NoError(t, err)
		defer rc.Close()

		var result []byte
		buf := make([]byte, readSize)
		for {
			n, err := rc.Read(buf)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			require.LessOrEqual(t, n, len(buf))
			result = append(result, buf[:n]...)
		}

		require.Equal(t, expectedData, result, "data mismatch for readSize=%d", readSize)
	}

	expectedRequests := map[string]int{
		"GET /v2/": len(readSizes),
		"GET /v2/test-image/blobs/" + layerDigest.String(): len(readSizes),
	}
	require.Empty(t, cmp.Diff(expectedRequests, counter.Snapshot()))
}

func localhostIPs(t *testing.T) []*net.IPNet {
	_, ipv4Net, err := net.ParseCIDR("127.0.0.0/8")
	require.NoError(t, err)
	_, ipv6Net, err := net.ParseCIDR("::1/128")
	require.NoError(t, err)
	return []*net.IPNet{ipv4Net, ipv6Net}
}

func newTestClient(t *testing.T) ofpb.OCIFetcherClient {
	return ocifetcher.NewClient(localhostIPs(t), nil)
}

func pushTestImage(t *testing.T, opts testregistry.Opts, pushCreds *testregistry.BasicAuthCreds) (string, v1.Image) {
	reg := testregistry.Run(t, opts)
	imageName, img := reg.PushNamedImage(t, "test-image", pushCreds)
	return imageName, img
}

func firstLayer(t *testing.T, img v1.Image) v1.Layer {
	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	return layers[0]
}

func blobRefForLayer(t *testing.T, imageName string, layer v1.Layer) string {
	layerDigest, err := layer.Digest()
	require.NoError(t, err)
	return imageName + "@" + layerDigest.String()
}

func layerBytes(t *testing.T, layer v1.Layer) []byte {
	layerReader, err := layer.Compressed()
	require.NoError(t, err)
	data, err := io.ReadAll(layerReader)
	require.NoError(t, err)
	require.NoError(t, layerReader.Close())
	return data
}

