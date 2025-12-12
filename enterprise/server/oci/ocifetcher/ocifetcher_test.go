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
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-cmp/cmp"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
)

func TestFetchManifestMetadata_AuthVariants(t *testing.T) {
	cases := []struct {
		name           string
		registryCreds  *testregistry.BasicAuthCreds
		requestCreds   *rgpb.Credentials
		expectError    bool
		checkError     func(error) bool
		expectedCounts map[string]int
	}{
		{
			name:          "NoAuth",
			registryCreds: nil,
			requestCreds:  nil,
			expectError:   false,
			expectedCounts: map[string]int{
				http.MethodGet + " /v2/":                             1,
				http.MethodHead + " /v2/test-image/manifests/latest": 1,
			},
		},
		{
			name:          "WithValidCredentials",
			registryCreds: &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"},
			requestCreds:  &rgpb.Credentials{Username: "testuser", Password: "testpass"},
			expectError:   false,
			expectedCounts: map[string]int{
				http.MethodGet + " /v2/":                             1,
				http.MethodHead + " /v2/test-image/manifests/latest": 1,
			},
		},
		{
			name:          "WithInvalidCredentials",
			registryCreds: &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"},
			requestCreds:  &rgpb.Credentials{Username: "wronguser", Password: "wrongpass"},
			expectError:   true,
			checkError:    status.IsPermissionDeniedError,
			expectedCounts: map[string]int{
				http.MethodGet + " /v2/":                             2,
				http.MethodHead + " /v2/test-image/manifests/latest": 2,
			},
		},
		{
			name:          "MissingCredentials",
			registryCreds: &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"},
			requestCreds:  nil,
			expectError:   true,
			checkError:    status.IsPermissionDeniedError,
			expectedCounts: map[string]int{
				http.MethodGet + " /v2/":                             2,
				http.MethodHead + " /v2/test-image/manifests/latest": 2,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := testenv.GetTestEnv(t)
			reg, counter := setupRegistry(t, tc.registryCreds, nil)
			imageName, img := reg.PushNamedImage(t, "test-image", tc.registryCreds)
			digest, size, mediaType := imageMetadata(t, img)

			counter.Reset()
			client := newTestClient(t, env)
			resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
				Ref:         imageName,
				Credentials: tc.requestCreds,
			})

			if tc.expectError {
				require.Error(t, err)
				if tc.checkError != nil {
					require.True(t, tc.checkError(err), "unexpected error type: %v", err)
				}
			} else {
				require.NoError(t, err)
				assertMetadata(t, resp, digest, size, mediaType)
			}
			assertRequests(t, counter, tc.expectedCounts)
		})
	}
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

func newTestServer(t *testing.T) ofpb.OCIFetcherServer {
	server, err := ocifetcher.NewServer(localhostIPs(t), nil)
	require.NoError(t, err)
	return server
}

// mockFetchBlobServer implements ofpb.OCIFetcher_FetchBlobServer for testing.
type mockFetchBlobServer struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*ofpb.FetchBlobResponse
}

func (m *mockFetchBlobServer) Send(resp *ofpb.FetchBlobResponse) error {
	m.responses = append(m.responses, resp)
	return nil
}

func (m *mockFetchBlobServer) Context() context.Context {
	return m.ctx
}

func (m *mockFetchBlobServer) collectData() []byte {
	var data []byte
	for _, resp := range m.responses {
		data = append(data, resp.GetData()...)
	}
	return data
}

func layerMetadata(t *testing.T, layer v1.Layer) (size int64, mediaType string) {
	s, err := layer.Size()
	require.NoError(t, err)
	mt, err := layer.MediaType()
	require.NoError(t, err)
	return s, string(mt)
}

func layerData(t *testing.T, layer v1.Layer) []byte {
	rc, err := layer.Compressed()
	require.NoError(t, err)
	defer rc.Close()
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	return data
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

// TestFetchManifestMetadata_PullerEvictionOnHeadError verifies that when Head()
// returns an error, the Puller is evicted from the cache and a new one is
// created on the next request.
func TestFetchManifestMetadata_PullerEvictionOnHeadError(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var failHead atomic.Bool
	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if failHead.Load() && r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/manifests/") {
			w.WriteHeader(http.StatusInternalServerError)
			return false
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, size, mediaType := imageMetadata(t, img)
	client := newTestClient(t, env)

	// First call - should create a new Puller
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})

	// Second call - should reuse cached Puller (no /v2/ auth request)
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})

	// Third call - Head() fails, Puller should be evicted
	failHead.Store(true)
	_, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.Error(t, err)

	// Fourth call - should create a NEW Puller because the old one was evicted
	failHead.Store(false)
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})
}

// TestFetchManifestMetadata_ContextErrorNoRetry verifies that when Head()
// returns a context error (canceled or deadline exceeded), no retry is
// attempted and the Puller is NOT evicted from the cache.
func TestFetchManifestMetadata_ContextErrorNoRetry(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var blockHead atomic.Bool
	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if blockHead.Load() && r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/manifests/") {
			time.Sleep(100 * time.Millisecond) // Block until context times out
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, size, mediaType := imageMetadata(t, img)
	client := newTestClient(t, env)

	// First call - should create a new Puller and cache it
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})

	// Second call - context times out during Head request
	blockHead.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = client.FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded), "expected DeadlineExceeded error, got: %v", err)

	// Third call - Puller should still be cached (no /v2/ auth request)
	blockHead.Store(false)
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})
}

// TestFetchManifestMetadata_HTTPErrorThenSuccess verifies that when Head()
// returns an HTTP error on the first attempt, the Puller is refreshed and
// the retry succeeds. The refreshed Puller should remain in the cache.
func TestFetchManifestMetadata_HTTPErrorThenSuccess(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var headAttempts atomic.Int32
	var failFirstHead atomic.Bool
	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if failFirstHead.Load() && r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/manifests/") {
			if headAttempts.Add(1) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				return false
			}
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, size, mediaType := imageMetadata(t, img)
	client := newTestClient(t, env)

	// First call - should create a new Puller and cache it
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})

	// Second call - first Head fails, Puller refreshed, retry succeeds
	headAttempts.Store(0)
	failFirstHead.Store(true)
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)

	// Third call - refreshed Puller should still be cached (no /v2/ auth request)
	failFirstHead.Store(false)
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})
}

// TestFetchManifestMetadata_HTTPErrorThenHTTPError verifies that when Head()
// returns HTTP errors on both the first attempt and the retry, the refreshed
// Puller is evicted from the cache.
func TestFetchManifestMetadata_HTTPErrorThenHTTPError(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var failHead atomic.Bool
	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if failHead.Load() && r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/manifests/") {
			w.WriteHeader(http.StatusInternalServerError)
			return false
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, size, mediaType := imageMetadata(t, img)
	client := newTestClient(t, env)

	// First call - should create a new Puller and cache it
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})

	// Second call - both Head attempts fail (don't assert exact counts due to internal retries)
	failHead.Store(true)
	_, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.Error(t, err)

	// Third call - refreshed Puller should have been evicted (needs /v2/ auth request)
	failHead.Store(false)
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})
}

// TestFetchManifestMetadata_HTTPErrorThenContextError verifies that when Head()
// returns an HTTP error on the first attempt and a context error on the retry,
// the refreshed Puller is NOT evicted from the cache.
func TestFetchManifestMetadata_HTTPErrorThenContextError(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var headAttempts atomic.Int32
	var failFirstThenBlock atomic.Bool
	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if failFirstThenBlock.Load() && r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/manifests/") {
			if headAttempts.Add(1) == 1 {
				w.WriteHeader(http.StatusInternalServerError) // First attempt: HTTP error
				return false
			}
			time.Sleep(100 * time.Millisecond) // Second attempt: block until timeout
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, size, mediaType := imageMetadata(t, img)
	client := newTestClient(t, env)

	// First call - should create a new Puller and cache it
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})

	// Second call - first Head returns HTTP error, Puller refreshed, second times out
	headAttempts.Store(0)
	failFirstThenBlock.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = client.FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.Error(t, err)

	// Third call - refreshed Puller should still be cached (no /v2/ auth request)
	failFirstThenBlock.Store(false)
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})
}

func TestServerHappyPath(t *testing.T) {
	authCases := []struct {
		name          string
		registryCreds *testregistry.BasicAuthCreds
		requestCreds  *rgpb.Credentials
	}{
		{
			name:          "NoAuth",
			registryCreds: nil,
			requestCreds:  nil,
		},
		{
			name:          "WithAuth",
			registryCreds: &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"},
			requestCreds:  &rgpb.Credentials{Username: "testuser", Password: "testpass"},
		},
	}

	for _, ac := range authCases {
		t.Run("FetchManifestMetadata/"+ac.name, func(t *testing.T) {
			reg, _ := setupRegistry(t, ac.registryCreds, nil)
			imageName, img := reg.PushNamedImage(t, "test-image", ac.registryCreds)
			expectedDigest, expectedSize, expectedMediaType := imageMetadata(t, img)

			server := newTestServer(t)
			resp, err := server.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
				Ref:         imageName,
				Credentials: ac.requestCreds,
			})

			require.NoError(t, err)
			require.Equal(t, expectedDigest, resp.GetDigest())
			require.Equal(t, expectedSize, resp.GetSize())
			require.Equal(t, expectedMediaType, resp.GetMediaType())
		})

		t.Run("FetchManifest/"+ac.name, func(t *testing.T) {
			reg, _ := setupRegistry(t, ac.registryCreds, nil)
			imageName, img := reg.PushNamedImage(t, "test-image", ac.registryCreds)
			expectedDigest, expectedSize, expectedMediaType := imageMetadata(t, img)
			expectedManifest, err := img.RawManifest()
			require.NoError(t, err)

			server := newTestServer(t)
			resp, err := server.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{
				Ref:         imageName,
				Credentials: ac.requestCreds,
			})

			require.NoError(t, err)
			require.Equal(t, expectedDigest, resp.GetDigest())
			require.Equal(t, expectedSize, resp.GetSize())
			require.Equal(t, expectedMediaType, resp.GetMediaType())
			require.Equal(t, expectedManifest, resp.GetManifest())
		})

		t.Run("FetchBlobMetadata/"+ac.name, func(t *testing.T) {
			reg, _ := setupRegistry(t, ac.registryCreds, nil)
			imageName, img := reg.PushNamedImage(t, "test-image", ac.registryCreds)

			layers, err := img.Layers()
			require.NoError(t, err)
			require.NotEmpty(t, layers)

			layer := layers[0]
			digest, err := layer.Digest()
			require.NoError(t, err)
			expectedSize, expectedMediaType := layerMetadata(t, layer)

			server := newTestServer(t)
			resp, err := server.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{
				Ref:         imageName + "@" + digest.String(),
				Credentials: ac.requestCreds,
			})

			require.NoError(t, err)
			require.Equal(t, expectedSize, resp.GetSize())
			require.Equal(t, expectedMediaType, resp.GetMediaType())
		})

		t.Run("FetchBlob/"+ac.name, func(t *testing.T) {
			reg, _ := setupRegistry(t, ac.registryCreds, nil)
			imageName, img := reg.PushNamedImage(t, "test-image", ac.registryCreds)

			layers, err := img.Layers()
			require.NoError(t, err)
			require.NotEmpty(t, layers)

			layer := layers[0]
			digest, err := layer.Digest()
			require.NoError(t, err)
			expectedData := layerData(t, layer)

			server := newTestServer(t)
			stream := &mockFetchBlobServer{ctx: context.Background()}
			err = server.FetchBlob(&ofpb.FetchBlobRequest{
				Ref:         imageName + "@" + digest.String(),
				Credentials: ac.requestCreds,
			}, stream)

			require.NoError(t, err)
			require.Equal(t, expectedData, stream.collectData())
		})
	}
}

func TestServerMissingAndInvalidCredentials(t *testing.T) {
	credsCases := []struct {
		name         string
		requestCreds *rgpb.Credentials
	}{
		{
			name:         "MissingCredentials",
			requestCreds: nil,
		},
		{
			name:         "InvalidCredentials",
			requestCreds: &rgpb.Credentials{Username: "wrong", Password: "wrong"},
		},
	}

	registryCreds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}

	for _, cc := range credsCases {
		t.Run(cc.name, func(t *testing.T) {
			reg, counter := setupRegistry(t, registryCreds, nil)
			imageName, img := reg.PushNamedImage(t, "test-image", registryCreds)
			layers, err := img.Layers()
			require.NoError(t, err)
			require.NotEmpty(t, layers)
			layer := layers[0]
			layerDigest, err := layer.Digest()
			require.NoError(t, err)
			server := newTestServer(t)

			// FetchManifestMetadata
			counter.Reset()
			_, err = server.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
				Ref:         imageName,
				Credentials: cc.requestCreds,
			})
			require.Error(t, err)
			require.True(t, status.IsPermissionDeniedError(err), "FetchManifestMetadata: expected PermissionDenied, got: %v", err)
			assertRequests(t, counter, map[string]int{
				http.MethodGet + " /v2/":                             2,
				http.MethodHead + " /v2/test-image/manifests/latest": 2,
			})

			// FetchManifest
			counter.Reset()
			_, err = server.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{
				Ref:         imageName,
				Credentials: cc.requestCreds,
			})
			require.Error(t, err)
			require.True(t, status.IsPermissionDeniedError(err), "FetchManifest: expected PermissionDenied, got: %v", err)
			assertRequests(t, counter, map[string]int{
				http.MethodGet + " /v2/":                            2,
				http.MethodGet + " /v2/test-image/manifests/latest": 2,
			})

			// FetchBlobMetadata - 401 errors are wrapped differently for blob methods
			counter.Reset()
			_, err = server.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{
				Ref:         imageName + "@" + layerDigest.String(),
				Credentials: cc.requestCreds,
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), "401 Unauthorized", "FetchBlobMetadata: expected 401 error, got: %v", err)

			// FetchBlob - 401 errors are wrapped differently for blob methods
			counter.Reset()
			stream := &mockFetchBlobServer{ctx: context.Background()}
			err = server.FetchBlob(&ofpb.FetchBlobRequest{
				Ref:         imageName + "@" + layerDigest.String(),
				Credentials: cc.requestCreds,
			}, stream)
			require.Error(t, err)
			require.Contains(t, err.Error(), "401 Unauthorized", "FetchBlob: expected 401 error, got: %v", err)
		})
	}
}

func TestServerRetryOnHTTPErrors(t *testing.T) {
	var headAttempts, getAttempts, blobAttempts atomic.Int32
	var failHeadOnce, failGetOnce, failBlobOnce atomic.Bool

	reg, _ := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if strings.Contains(r.URL.Path, "/manifests/") {
			if failHeadOnce.Load() && r.Method == http.MethodHead {
				if headAttempts.Add(1) == 1 {
					w.WriteHeader(http.StatusInternalServerError)
					return false
				}
			}
			if failGetOnce.Load() && r.Method == http.MethodGet {
				if getAttempts.Add(1) == 1 {
					w.WriteHeader(http.StatusInternalServerError)
					return false
				}
			}
		}
		if failBlobOnce.Load() && strings.Contains(r.URL.Path, "/blobs/") {
			if blobAttempts.Add(1) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				return false
			}
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	expectedDigest, expectedSize, expectedMediaType := imageMetadata(t, img)
	expectedManifest, err := img.RawManifest()
	require.NoError(t, err)
	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	layer := layers[0]
	layerDigest, err := layer.Digest()
	require.NoError(t, err)
	expectedLayerSize, expectedLayerMediaType := layerMetadata(t, layer)
	expectedLayerData := layerData(t, layer)

	server := newTestServer(t)

	// FetchManifestMetadata - first HEAD fails, retry succeeds
	failHeadOnce.Store(true)
	resp, err := server.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, expectedDigest, resp.GetDigest())
	require.Equal(t, expectedSize, resp.GetSize())
	require.Equal(t, expectedMediaType, resp.GetMediaType())
	require.Equal(t, int32(2), headAttempts.Load(), "expected 2 HEAD attempts")
	failHeadOnce.Store(false)

	// FetchManifest - first GET fails, retry succeeds
	failGetOnce.Store(true)
	manifestResp, err := server.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, expectedDigest, manifestResp.GetDigest())
	require.Equal(t, expectedSize, manifestResp.GetSize())
	require.Equal(t, expectedMediaType, manifestResp.GetMediaType())
	require.Equal(t, expectedManifest, manifestResp.GetManifest())
	require.Equal(t, int32(2), getAttempts.Load(), "expected 2 GET attempts")
	failGetOnce.Store(false)

	// FetchBlobMetadata - first blob request fails, retry succeeds
	failBlobOnce.Store(true)
	blobMetaResp, err := server.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: imageName + "@" + layerDigest.String()})
	require.NoError(t, err)
	require.Equal(t, expectedLayerSize, blobMetaResp.GetSize())
	require.Equal(t, expectedLayerMediaType, blobMetaResp.GetMediaType())
	require.Equal(t, int32(2), blobAttempts.Load(), "expected 2 blob attempts")

	// FetchBlob - reset counter, first blob request fails, retry succeeds
	blobAttempts.Store(0)
	stream := &mockFetchBlobServer{ctx: context.Background()}
	err = server.FetchBlob(&ofpb.FetchBlobRequest{Ref: imageName + "@" + layerDigest.String()}, stream)
	require.NoError(t, err)
	require.Equal(t, expectedLayerData, stream.collectData())
	require.Equal(t, int32(2), blobAttempts.Load(), "expected 2 blob attempts")
}

func TestServerNoRetryOnContextErrors(t *testing.T) {
	var headAttempts, getAttempts, blobAttempts atomic.Int32
	var blockHead, blockGet, blockBlob atomic.Bool

	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if strings.Contains(r.URL.Path, "/manifests/") {
			if blockHead.Load() && r.Method == http.MethodHead {
				headAttempts.Add(1)
				time.Sleep(100 * time.Millisecond)
			}
			if blockGet.Load() && r.Method == http.MethodGet {
				getAttempts.Add(1)
				time.Sleep(100 * time.Millisecond)
			}
		}
		if blockBlob.Load() && strings.Contains(r.URL.Path, "/blobs/") {
			blobAttempts.Add(1)
			time.Sleep(100 * time.Millisecond)
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	expectedDigest, expectedSize, expectedMediaType := imageMetadata(t, img)
	expectedManifest, err := img.RawManifest()
	require.NoError(t, err)
	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	layer := layers[0]
	layerDigest, err := layer.Digest()
	require.NoError(t, err)
	expectedLayerSize, expectedLayerMediaType := layerMetadata(t, layer)
	expectedLayerData := layerData(t, layer)

	server := newTestServer(t)

	// FetchManifestMetadata: establish cached Puller
	resp, err := server.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, expectedDigest, resp.GetDigest())

	// FetchManifestMetadata: times out, should NOT retry
	blockHead.Store(true)
	headAttempts.Store(0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	_, err = server.FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	cancel()
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded), "expected DeadlineExceeded, got: %v", err)
	require.Equal(t, int32(1), headAttempts.Load(), "should not retry on context error")

	// FetchManifestMetadata: Puller should still be cached
	blockHead.Store(false)
	counter.Reset()
	resp, err = server.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, expectedDigest, resp.GetDigest())
	require.Equal(t, expectedSize, resp.GetSize())
	require.Equal(t, expectedMediaType, resp.GetMediaType())
	assertRequests(t, counter, map[string]int{
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})

	// FetchManifest: times out, should NOT retry
	blockGet.Store(true)
	getAttempts.Store(0)
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	_, err = server.FetchManifest(ctx, &ofpb.FetchManifestRequest{Ref: imageName})
	cancel()
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded), "expected DeadlineExceeded, got: %v", err)
	require.Equal(t, int32(1), getAttempts.Load(), "should not retry on context error")

	// FetchManifest: Puller should still be cached
	blockGet.Store(false)
	counter.Reset()
	manifestResp, err := server.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, expectedDigest, manifestResp.GetDigest())
	require.Equal(t, expectedSize, manifestResp.GetSize())
	require.Equal(t, expectedMediaType, manifestResp.GetMediaType())
	require.Equal(t, expectedManifest, manifestResp.GetManifest())
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/test-image/manifests/latest": 1,
	})

	// FetchBlobMetadata: establish cached layer access
	blobMetaResp, err := server.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: imageName + "@" + layerDigest.String()})
	require.NoError(t, err)
	require.Equal(t, expectedLayerSize, blobMetaResp.GetSize())

	// FetchBlobMetadata: times out, should NOT retry
	blockBlob.Store(true)
	blobAttempts.Store(0)
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	_, err = server.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{Ref: imageName + "@" + layerDigest.String()})
	cancel()
	require.Error(t, err)
	require.Contains(t, err.Error(), "deadline exceeded", "expected deadline exceeded error, got: %v", err)
	require.Equal(t, int32(1), blobAttempts.Load(), "should not retry on context error")

	// FetchBlobMetadata: Puller should still be cached
	blockBlob.Store(false)
	counter.Reset()
	blobMetaResp, err = server.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: imageName + "@" + layerDigest.String()})
	require.NoError(t, err)
	require.Equal(t, expectedLayerSize, blobMetaResp.GetSize())
	require.Equal(t, expectedLayerMediaType, blobMetaResp.GetMediaType())

	// FetchBlob: establish cached layer access
	stream := &mockFetchBlobServer{ctx: context.Background()}
	err = server.FetchBlob(&ofpb.FetchBlobRequest{Ref: imageName + "@" + layerDigest.String()}, stream)
	require.NoError(t, err)
	require.Equal(t, expectedLayerData, stream.collectData())

	// FetchBlob: times out, should NOT retry
	blockBlob.Store(true)
	blobAttempts.Store(0)
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	stream = &mockFetchBlobServer{ctx: ctx}
	err = server.FetchBlob(&ofpb.FetchBlobRequest{Ref: imageName + "@" + layerDigest.String()}, stream)
	cancel()
	require.Error(t, err)
	require.Contains(t, err.Error(), "deadline exceeded", "expected deadline exceeded error, got: %v", err)
	require.Equal(t, int32(1), blobAttempts.Load(), "should not retry on context error")

	// FetchBlob: Puller should still be cached
	blockBlob.Store(false)
	counter.Reset()
	stream = &mockFetchBlobServer{ctx: context.Background()}
	err = server.FetchBlob(&ofpb.FetchBlobRequest{Ref: imageName + "@" + layerDigest.String()}, stream)
	require.NoError(t, err)
	require.Equal(t, expectedLayerData, stream.collectData())
}
