package ocifetcher_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-cmp/cmp"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/stretchr/testify/require"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
)

func TestFetch_HappyPath(t *testing.T) {
	env := testenv.GetTestEnv(t)
	reg, counter := setupRegistry(t, nil, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, size, mediaType := imageMetadata(t, img)
	manifest := rawManifest(t, img)
	layers, err := img.Layers()
	require.NoError(t, err)
	require.Greater(t, len(layers), 0)
	layer := layers[0]
	blobRef := layerBlobRef(t, reg, "test-image", layer)
	expectedLayerData := expectedLayerBytes(t, layer)
	layerDigest, err := layer.Digest()
	require.NoError(t, err)

	// FetchManifestMetadata - fresh client
	counter.Reset()
	client1 := newTestClient(t, env)
	metadataResp, err := client1.FetchManifestMetadata(context.Background(),
		&ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, metadataResp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})

	// FetchManifest - fresh client
	counter.Reset()
	client2 := newTestClient(t, env)
	manifestResp, err := client2.FetchManifest(context.Background(),
		&ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	assertManifestResponse(t, manifestResp, digest, size, mediaType, manifest)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                            1,
		http.MethodGet + " /v2/test-image/manifests/latest": 1,
	})

	// FetchBlob - fresh client
	counter.Reset()
	client3 := newTestClient(t, env)
	reader, err := ocifetcher.ReadBlob(context.Background(), client3, blobRef, nil, false)
	require.NoError(t, err)
	defer reader.Close()
	actualBytes, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, expectedLayerData, actualBytes)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                                         1,
		http.MethodGet + " /v2/test-image/blobs/" + layerDigest.String(): 1,
	})

	// FetchBlob (streaming client) - fresh client
	counter.Reset()
	client3b := newTestClient(t, env)
	_, err = client3b.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRef})
	require.NoError(t, err)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                                         1,
		http.MethodGet + " /v2/test-image/blobs/" + layerDigest.String(): 1,
	})

	// --- With valid credentials ---
	creds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
	reqCreds := &rgpb.Credentials{Username: "testuser", Password: "testpass"}
	reg2, counter2 := setupRegistry(t, creds, nil)
	imageName2, img2 := reg2.PushNamedImage(t, "test-image", creds)
	digest2, size2, mediaType2 := imageMetadata(t, img2)
	manifest2 := rawManifest(t, img2)
	layers2, err := img2.Layers()
	require.NoError(t, err)
	require.Greater(t, len(layers2), 0)
	layer2 := layers2[0]
	blobRef2 := layerBlobRef(t, reg2, "test-image", layer2)
	expectedLayerData2 := expectedLayerBytes(t, layer2)
	layerDigest2, err := layer2.Digest()
	require.NoError(t, err)

	// FetchManifestMetadata with creds
	counter2.Reset()
	client4 := newTestClient(t, env)
	metadataResp2, err := client4.FetchManifestMetadata(context.Background(),
		&ofpb.FetchManifestMetadataRequest{Ref: imageName2, Credentials: reqCreds})
	require.NoError(t, err)
	assertMetadata(t, metadataResp2, digest2, size2, mediaType2)
	assertRequests(t, counter2, map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})

	// FetchManifest with creds
	counter2.Reset()
	client5 := newTestClient(t, env)
	manifestResp2, err := client5.FetchManifest(context.Background(),
		&ofpb.FetchManifestRequest{Ref: imageName2, Credentials: reqCreds})
	require.NoError(t, err)
	assertManifestResponse(t, manifestResp2, digest2, size2, mediaType2, manifest2)
	assertRequests(t, counter2, map[string]int{
		http.MethodGet + " /v2/":                            1,
		http.MethodGet + " /v2/test-image/manifests/latest": 1,
	})

	// FetchBlob with creds
	counter2.Reset()
	client6 := newTestClient(t, env)
	reader2, err := ocifetcher.ReadBlob(context.Background(), client6, blobRef2, reqCreds, false)
	require.NoError(t, err)
	defer reader2.Close()
	actualBytes2, err := io.ReadAll(reader2)
	require.NoError(t, err)
	require.Equal(t, expectedLayerData2, actualBytes2)
	assertRequests(t, counter2, map[string]int{
		http.MethodGet + " /v2/":                                          1,
		http.MethodGet + " /v2/test-image/blobs/" + layerDigest2.String(): 1,
	})

	// FetchBlob (streaming client) with creds
	counter2.Reset()
	client6b := newTestClient(t, env)
	_, err = client6b.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRef2, Credentials: reqCreds})
	require.NoError(t, err)
	assertRequests(t, counter2, map[string]int{
		http.MethodGet + " /v2/":                                          1,
		http.MethodGet + " /v2/test-image/blobs/" + layerDigest2.String(): 1,
	})
}

func TestFetch_MissingCredentials(t *testing.T) {
	creds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
	env := testenv.GetTestEnv(t)
	reg, _ := setupRegistry(t, creds, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", creds)
	layers, err := img.Layers()
	require.NoError(t, err)
	require.Greater(t, len(layers), 0)
	blobRef := layerBlobRef(t, reg, "test-image", layers[0])

	// FetchManifestMetadata - missing creds
	client1 := newTestClient(t, env)
	_, err = client1.FetchManifestMetadata(context.Background(),
		&ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied, got: %v", err)

	// FetchManifest - missing creds
	client2 := newTestClient(t, env)
	_, err = client2.FetchManifest(context.Background(),
		&ofpb.FetchManifestRequest{Ref: imageName})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied, got: %v", err)

	// FetchBlob - missing creds
	client3 := newTestClient(t, env)
	_, err = client3.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRef})
	require.Error(t, err)
	require.True(t, status.IsUnavailableError(err), "expected Unavailable, got: %v", err)
}

func TestFetch_InvalidCredentials(t *testing.T) {
	creds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
	badCreds := &rgpb.Credentials{Username: "wronguser", Password: "wrongpass"}
	env := testenv.GetTestEnv(t)
	reg, _ := setupRegistry(t, creds, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", creds)
	layers, err := img.Layers()
	require.NoError(t, err)
	require.Greater(t, len(layers), 0)
	blobRef := layerBlobRef(t, reg, "test-image", layers[0])

	// FetchManifestMetadata - invalid creds
	client1 := newTestClient(t, env)
	_, err = client1.FetchManifestMetadata(context.Background(),
		&ofpb.FetchManifestMetadataRequest{Ref: imageName, Credentials: badCreds})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied, got: %v", err)

	// FetchManifest - invalid creds
	client2 := newTestClient(t, env)
	_, err = client2.FetchManifest(context.Background(),
		&ofpb.FetchManifestRequest{Ref: imageName, Credentials: badCreds})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied, got: %v", err)

	// FetchBlob - invalid creds
	client3 := newTestClient(t, env)
	_, err = client3.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRef, Credentials: badCreds})
	require.Error(t, err)
	require.True(t, status.IsUnavailableError(err), "expected Unavailable, got: %v", err)
}

func TestFetch_RetryOnHTTPError(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var attempts atomic.Int32
	reg, _ := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		// Fail first attempt for manifest/blob requests
		if attempts.Add(1) == 1 && (strings.Contains(r.URL.Path, "/manifests/") || strings.Contains(r.URL.Path, "/blobs/")) {
			w.WriteHeader(http.StatusInternalServerError)
			return false
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, size, mediaType := imageMetadata(t, img)
	manifest := rawManifest(t, img)
	layers, err := img.Layers()
	require.NoError(t, err)
	require.Greater(t, len(layers), 0)
	blobRef := layerBlobRef(t, reg, "test-image", layers[0])
	expectedLayerData := expectedLayerBytes(t, layers[0])

	// FetchManifestMetadata - first attempt fails, retry succeeds
	attempts.Store(0)
	client1 := newTestClient(t, env)
	metadataResp, err := client1.FetchManifestMetadata(context.Background(),
		&ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, metadataResp, digest, size, mediaType)

	// FetchManifest - first attempt fails, retry succeeds
	attempts.Store(0)
	client2 := newTestClient(t, env)
	manifestResp, err := client2.FetchManifest(context.Background(),
		&ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	assertManifestResponse(t, manifestResp, digest, size, mediaType, manifest)

	// FetchBlob - first attempt fails, retry succeeds
	attempts.Store(0)
	client3 := newTestClient(t, env)
	reader, err := ocifetcher.ReadBlob(context.Background(), client3, blobRef, nil, false)
	require.NoError(t, err)
	defer reader.Close()
	actualBytes, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, expectedLayerData, actualBytes)
}

func TestFetch_NoRetryOnContextError(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var cancelFunc func()
	reg, _ := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		// Cancel the context when manifest/blob requests arrive
		if strings.Contains(r.URL.Path, "/manifests/") || strings.Contains(r.URL.Path, "/blobs/") {
			if cancelFunc != nil {
				cancelFunc()
			}
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	layers, err := img.Layers()
	require.NoError(t, err)
	require.Greater(t, len(layers), 0)
	blobRef := layerBlobRef(t, reg, "test-image", layers[0])

	// FetchManifestMetadata - canceled
	ctx1, cancel1 := context.WithCancel(context.Background())
	cancelFunc = cancel1
	client1 := newTestClient(t, env)
	_, err = client1.FetchManifestMetadata(ctx1, &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got: %v", err)

	// FetchManifest - canceled
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancelFunc = cancel2
	client2 := newTestClient(t, env)
	_, err = client2.FetchManifest(ctx2, &ofpb.FetchManifestRequest{Ref: imageName})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got: %v", err)

	// FetchBlob - canceled
	ctx3, cancel3 := context.WithCancel(context.Background())
	cancelFunc = cancel3
	client3 := newTestClient(t, env)
	_, err = client3.FetchBlob(ctx3, &ofpb.FetchBlobRequest{Ref: blobRef})
	require.Error(t, err)
	require.True(t, status.IsUnavailableError(err), "expected Unavailable, got: %v", err)
}

func localhostIPs(t *testing.T) []*net.IPNet {
	_, ipv4Net, err := net.ParseCIDR("127.0.0.0/8")
	require.NoError(t, err)
	_, ipv6Net, err := net.ParseCIDR("::1/128")
	require.NoError(t, err)
	return []*net.IPNet{ipv4Net, ipv6Net}
}

func imageMetadata(t *testing.T, img v1.Image) (digest string, size int64, mediaType string) {
	d, err := img.Digest()
	require.NoError(t, err)
	s, err := img.Size()
	require.NoError(t, err)
	m, err := img.MediaType()
	require.NoError(t, err)
	return d.String(), s, string(m)
}

func assertMetadata(t *testing.T, resp *ofpb.FetchManifestMetadataResponse, digest string, size int64, mediaType string) {
	require.Equal(t, digest, resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, mediaType, resp.GetMediaType())
}

func assertRequests(t *testing.T, counter *testhttp.RequestCounter, expected map[string]int) {
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

func setupRegistry(t *testing.T, creds *testregistry.BasicAuthCreds, interceptor func(http.ResponseWriter, *http.Request) bool) (*testregistry.Registry, *testhttp.RequestCounter) {
	counter := testhttp.NewRequestCounter()
	reg := testregistry.Run(t, testregistry.Opts{
		Creds: creds,
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			if interceptor != nil {
				return interceptor(w, r)
			}
			return true
		},
	})
	return reg, counter
}

func newTestClient(t *testing.T, env environment.Env) ofpb.OCIFetcherClient {
	client, err := ocifetcher.NewClient(localhostIPs(t), nil)
	require.NoError(t, err)
	return client
}

// TestFetchManifestMetadata_PullerCacheHit verifies that the same Puller is
// reused when fetching the same image with the same credentials.
func TestFetchManifestMetadata_PullerCacheHit(t *testing.T) {
	env := testenv.GetTestEnv(t)
	reg, counter := setupRegistry(t, nil, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, size, mediaType := imageMetadata(t, img)
	client := newTestClient(t, env)

	// First call - should create a new Puller and make a /v2/ auth request
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})

	// Second call - should reuse the cached Puller and NOT make another /v2/ auth request
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})
}

// TestFetchManifestMetadata_DifferentCredentials_DifferentPullers verifies that
// different credentials result in different Pullers being created.
func TestFetchManifestMetadata_DifferentCredentials_DifferentPullers(t *testing.T) {
	env := testenv.GetTestEnv(t)
	reg, counter := setupRegistry(t, nil, nil) // Registry accepts any credentials
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, size, mediaType := imageMetadata(t, img)
	client := newTestClient(t, env)

	// First call with credentials A
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref:         imageName,
		Credentials: &rgpb.Credentials{Username: "userA", Password: "passA"},
	})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})

	// Second call with different credentials B - should create a new Puller
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref:         imageName,
		Credentials: &rgpb.Credentials{Username: "userB", Password: "passB"},
	})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})
}

// TestFetchManifestMetadata_SameRepoDifferentTags_SamePuller verifies that
// fetching different tags from the same repository reuses the same Puller.
func TestFetchManifestMetadata_SameRepoDifferentTags_SamePuller(t *testing.T) {
	env := testenv.GetTestEnv(t)
	reg, counter := setupRegistry(t, nil, nil)

	// Push two images with different tags under the same repository
	imageName1, img1 := reg.PushNamedImage(t, "test-image:v1", nil)
	digest1, size1, mediaType1 := imageMetadata(t, img1)
	imageName2, img2 := reg.PushNamedImage(t, "test-image:v2", nil)
	digest2, size2, mediaType2 := imageMetadata(t, img2)
	client := newTestClient(t, env)

	// First call for tag v1
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName1})
	require.NoError(t, err)
	assertMetadata(t, resp, digest1, size1, mediaType1)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                         1,
		http.MethodHead + " /v2/test-image/manifests/v1": 1,
	})

	// Second call for tag v2 - should reuse the same Puller (same repo + creds)
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName2})
	require.NoError(t, err)
	assertMetadata(t, resp, digest2, size2, mediaType2)
	assertRequests(t, counter, map[string]int{
		http.MethodHead + " /v2/test-image/manifests/v2": 1,
	})
}

func rawManifest(t *testing.T, img v1.Image) []byte {
	m, err := img.RawManifest()
	require.NoError(t, err)
	return m
}

func assertManifestResponse(t *testing.T, resp *ofpb.FetchManifestResponse, digest string, size int64, mediaType string, manifest []byte) {
	require.Equal(t, digest, resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, mediaType, resp.GetMediaType())
	require.Equal(t, manifest, resp.GetManifest())
}

// TestFetchManifest_PullerCacheHit verifies that the same Puller is
// reused when fetching the same image with the same credentials.
func TestFetchManifest_PullerCacheHit(t *testing.T) {
	env := testenv.GetTestEnv(t)
	reg, counter := setupRegistry(t, nil, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, size, mediaType := imageMetadata(t, img)
	manifest := rawManifest(t, img)
	client := newTestClient(t, env)

	// First call - should create a new Puller and make a /v2/ auth request
	counter.Reset()
	resp, err := client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	assertManifestResponse(t, resp, digest, size, mediaType, manifest)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                            1,
		http.MethodGet + " /v2/test-image/manifests/latest": 1,
	})

	// Second call - should reuse the cached Puller and NOT make another /v2/ auth request
	counter.Reset()
	resp, err = client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	assertManifestResponse(t, resp, digest, size, mediaType, manifest)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/test-image/manifests/latest": 1,
	})
}

func layerBlobRef(t *testing.T, reg *testregistry.Registry, repoName string, layer v1.Layer) string {
	digest, err := layer.Digest()
	require.NoError(t, err)
	return reg.Address() + "/" + repoName + "@" + digest.String()
}

func expectedLayerBytes(t *testing.T, layer v1.Layer) []byte {
	rc, err := layer.Compressed()
	require.NoError(t, err)
	defer rc.Close()
	b, err := io.ReadAll(rc)
	require.NoError(t, err)
	return b
}

func TestFetchBlob_InvalidReference(t *testing.T) {
	env := testenv.GetTestEnv(t)
	reg, _ := setupRegistry(t, nil, nil)
	reg.PushNamedImage(t, "test-image", nil)

	client := newTestClient(t, env)
	// Tag reference instead of digest reference should fail
	tagRef := reg.Address() + "/test-image:latest"
	_, err := client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: tagRef})
	require.Error(t, err)
	require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgumentError, got: %v", err)
}
