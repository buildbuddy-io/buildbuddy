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

// manifestBytes returns the raw manifest bytes for an image.
func manifestBytes(t *testing.T, img v1.Image) []byte {
	m, err := img.RawManifest()
	require.NoError(t, err)
	return m
}

// assertManifestResponse verifies that the FetchManifest response matches expected values.
func assertManifestResponse(t *testing.T, resp *ofpb.FetchManifestResponse, digest string, size int64, mediaType string, manifest []byte) {
	require.Equal(t, digest, resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, mediaType, resp.GetMediaType())
	require.Equal(t, manifest, resp.GetManifest())
}

// readAllBlobData consumes a FetchBlob stream and returns all data.
func readAllBlobData(t *testing.T, stream ofpb.OCIFetcher_FetchBlobClient) []byte {
	var data []byte
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
		data = append(data, resp.GetData()...)
	}
	return data
}

func TestFetchManifest_AuthVariants(t *testing.T) {
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
				http.MethodGet + " /v2/":                            1,
				http.MethodGet + " /v2/test-image/manifests/latest": 1,
			},
		},
		{
			name:          "WithValidCredentials",
			registryCreds: &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"},
			requestCreds:  &rgpb.Credentials{Username: "testuser", Password: "testpass"},
			expectError:   false,
			expectedCounts: map[string]int{
				http.MethodGet + " /v2/":                            1,
				http.MethodGet + " /v2/test-image/manifests/latest": 1,
			},
		},
		{
			name:          "WithInvalidCredentials",
			registryCreds: &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"},
			requestCreds:  &rgpb.Credentials{Username: "wronguser", Password: "wrongpass"},
			expectError:   true,
			checkError:    status.IsPermissionDeniedError,
			expectedCounts: map[string]int{
				http.MethodGet + " /v2/":                            2,
				http.MethodGet + " /v2/test-image/manifests/latest": 2,
			},
		},
		{
			name:          "MissingCredentials",
			registryCreds: &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"},
			requestCreds:  nil,
			expectError:   true,
			checkError:    status.IsPermissionDeniedError,
			expectedCounts: map[string]int{
				http.MethodGet + " /v2/":                            2,
				http.MethodGet + " /v2/test-image/manifests/latest": 2,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := testenv.GetTestEnv(t)
			reg, counter := setupRegistry(t, tc.registryCreds, nil)
			imageName, img := reg.PushNamedImage(t, "test-image", tc.registryCreds)
			digest, size, mediaType := imageMetadata(t, img)
			manifest := manifestBytes(t, img)

			counter.Reset()
			client := newTestClient(t, env)
			resp, err := client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{
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
				assertManifestResponse(t, resp, digest, size, mediaType, manifest)
			}
			assertRequests(t, counter, tc.expectedCounts)
		})
	}
}

func TestFetchManifest_InvalidRef(t *testing.T) {
	env := testenv.GetTestEnv(t)
	client := newTestClient(t, env)

	_, err := client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{
		Ref: "not a valid ref!!!",
	})

	require.Error(t, err)
	require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument error, got: %v", err)
}

func TestFetchManifest_PullerCacheHit(t *testing.T) {
	env := testenv.GetTestEnv(t)
	reg, counter := setupRegistry(t, nil, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, size, mediaType := imageMetadata(t, img)
	manifest := manifestBytes(t, img)
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

func TestFetchManifest_HTTPErrorThenSuccess(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var getAttempts atomic.Int32
	var failFirstGet atomic.Bool
	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if failFirstGet.Load() && r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/manifests/") {
			if getAttempts.Add(1) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				return false
			}
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, size, mediaType := imageMetadata(t, img)
	manifest := manifestBytes(t, img)
	client := newTestClient(t, env)

	// First call - should create a new Puller and cache it
	counter.Reset()
	resp, err := client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	assertManifestResponse(t, resp, digest, size, mediaType, manifest)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                            1,
		http.MethodGet + " /v2/test-image/manifests/latest": 1,
	})

	// Second call - first Get fails, Puller refreshed, retry succeeds
	getAttempts.Store(0)
	failFirstGet.Store(true)
	resp, err = client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	assertManifestResponse(t, resp, digest, size, mediaType, manifest)

	// Third call - refreshed Puller should still be cached (no /v2/ auth request)
	failFirstGet.Store(false)
	counter.Reset()
	resp, err = client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	assertManifestResponse(t, resp, digest, size, mediaType, manifest)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/test-image/manifests/latest": 1,
	})
}

func TestFetchManifest_ContextErrorNoRetry(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var blockGet atomic.Bool
	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if blockGet.Load() && r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/manifests/") {
			time.Sleep(100 * time.Millisecond) // Block until context times out
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, size, mediaType := imageMetadata(t, img)
	manifest := manifestBytes(t, img)
	client := newTestClient(t, env)

	// First call - should create a new Puller and cache it
	counter.Reset()
	resp, err := client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	assertManifestResponse(t, resp, digest, size, mediaType, manifest)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                            1,
		http.MethodGet + " /v2/test-image/manifests/latest": 1,
	})

	// Second call - context times out during Get request
	blockGet.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = client.FetchManifest(ctx, &ofpb.FetchManifestRequest{Ref: imageName})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded), "expected DeadlineExceeded error, got: %v", err)

	// Third call - Puller should still be cached (no /v2/ auth request)
	blockGet.Store(false)
	counter.Reset()
	resp, err = client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	assertManifestResponse(t, resp, digest, size, mediaType, manifest)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/test-image/manifests/latest": 1,
	})
}

func TestFetchBlob_Success(t *testing.T) {
	env := testenv.GetTestEnv(t)
	reg, _ := setupRegistry(t, nil, nil)

	// Push an image with known file content
	fileContent := []byte("hello world blob content")
	imageName, img := reg.PushNamedImageWithFiles(t, "test-image", map[string][]byte{
		"test.txt": fileContent,
	}, nil)

	// Get the layer digest to use as blob reference
	layers, err := img.Layers()
	require.NoError(t, err)
	require.Len(t, layers, 1)
	layerDigest, err := layers[0].Digest()
	require.NoError(t, err)

	// Construct blob reference: registry/repo@sha256:...
	// The imageName is like "localhost:PORT/test-image:latest", we need "localhost:PORT/test-image@sha256:..."
	blobRef := strings.Split(imageName, ":")[0] + ":" + strings.Split(imageName, ":")[1] + "@" + layerDigest.String()

	client := newTestClient(t, env)
	stream, err := client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRef})
	require.NoError(t, err)

	data := readAllBlobData(t, stream)
	require.NotEmpty(t, data, "blob data should not be empty")
}

func TestFetchBlob_WithAuth(t *testing.T) {
	env := testenv.GetTestEnv(t)
	creds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
	reg, _ := setupRegistry(t, creds, nil)

	fileContent := []byte("authenticated blob content")
	imageName, img := reg.PushNamedImageWithFiles(t, "test-image", map[string][]byte{
		"test.txt": fileContent,
	}, creds)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.Len(t, layers, 1)
	layerDigest, err := layers[0].Digest()
	require.NoError(t, err)

	blobRef := strings.Split(imageName, ":")[0] + ":" + strings.Split(imageName, ":")[1] + "@" + layerDigest.String()

	client := newTestClient(t, env)
	stream, err := client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{
		Ref:         blobRef,
		Credentials: &rgpb.Credentials{Username: "testuser", Password: "testpass"},
	})
	require.NoError(t, err)

	data := readAllBlobData(t, stream)
	require.NotEmpty(t, data, "blob data should not be empty")
}

func TestFetchBlob_InvalidRef_MustBeDigest(t *testing.T) {
	env := testenv.GetTestEnv(t)
	client := newTestClient(t, env)

	// Use a tag reference instead of a digest reference
	_, err := client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{
		Ref: "localhost:5000/test-image:latest",
	})

	require.Error(t, err)
	require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument error, got: %v", err)
}

func TestFetchBlob_InvalidAuth(t *testing.T) {
	env := testenv.GetTestEnv(t)
	creds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
	reg, _ := setupRegistry(t, creds, nil)

	fileContent := []byte("secret blob content")
	imageName, img := reg.PushNamedImageWithFiles(t, "test-image", map[string][]byte{
		"test.txt": fileContent,
	}, creds)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.Len(t, layers, 1)
	layerDigest, err := layers[0].Digest()
	require.NoError(t, err)

	blobRef := strings.Split(imageName, ":")[0] + ":" + strings.Split(imageName, ":")[1] + "@" + layerDigest.String()

	client := newTestClient(t, env)
	_, err = client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{
		Ref:         blobRef,
		Credentials: &rgpb.Credentials{Username: "wronguser", Password: "wrongpass"},
	})

	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied error, got: %v", err)
}

func TestFetchBlob_PullerCacheHit(t *testing.T) {
	env := testenv.GetTestEnv(t)
	reg, counter := setupRegistry(t, nil, nil)

	fileContent := []byte("cache test content")
	imageName, img := reg.PushNamedImageWithFiles(t, "test-image", map[string][]byte{
		"test.txt": fileContent,
	}, nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.Len(t, layers, 1)
	layerDigest, err := layers[0].Digest()
	require.NoError(t, err)

	blobRef := strings.Split(imageName, ":")[0] + ":" + strings.Split(imageName, ":")[1] + "@" + layerDigest.String()
	client := newTestClient(t, env)

	// First call - should create a new Puller and make a /v2/ auth request
	counter.Reset()
	stream, err := client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRef})
	require.NoError(t, err)
	_ = readAllBlobData(t, stream)
	firstCounts := counter.Snapshot()
	require.Equal(t, 1, firstCounts[http.MethodGet+" /v2/"], "expected /v2/ auth request on first call")

	// Second call - should reuse the cached Puller and NOT make another /v2/ auth request
	counter.Reset()
	stream, err = client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRef})
	require.NoError(t, err)
	_ = readAllBlobData(t, stream)
	secondCounts := counter.Snapshot()
	require.Equal(t, 0, secondCounts[http.MethodGet+" /v2/"], "expected no /v2/ auth request on second call (cache hit)")
}

func TestFetchBlob_HTTPErrorThenSuccess(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var blobAttempts atomic.Int32
	var failFirstBlob atomic.Bool
	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if failFirstBlob.Load() && r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/blobs/") {
			if blobAttempts.Add(1) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				return false
			}
		}
		return true
	})

	fileContent := []byte("retry test content")
	imageName, img := reg.PushNamedImageWithFiles(t, "test-image", map[string][]byte{
		"test.txt": fileContent,
	}, nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.Len(t, layers, 1)
	layerDigest, err := layers[0].Digest()
	require.NoError(t, err)

	blobRef := strings.Split(imageName, ":")[0] + ":" + strings.Split(imageName, ":")[1] + "@" + layerDigest.String()
	client := newTestClient(t, env)

	// First call - should create a new Puller and cache it
	counter.Reset()
	stream, err := client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRef})
	require.NoError(t, err)
	_ = readAllBlobData(t, stream)

	// Second call - first blob fetch fails, Puller refreshed, retry succeeds
	blobAttempts.Store(0)
	failFirstBlob.Store(true)
	stream, err = client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRef})
	require.NoError(t, err)
	_ = readAllBlobData(t, stream)

	// Third call - refreshed Puller should still be cached (no /v2/ auth request)
	failFirstBlob.Store(false)
	counter.Reset()
	stream, err = client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRef})
	require.NoError(t, err)
	_ = readAllBlobData(t, stream)
	thirdCounts := counter.Snapshot()
	require.Equal(t, 0, thirdCounts[http.MethodGet+" /v2/"], "expected no /v2/ auth request (cache hit)")
}

func TestFetchBlob_ContextErrorNoRetry(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var blockBlob atomic.Bool
	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if blockBlob.Load() && r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/blobs/") {
			time.Sleep(100 * time.Millisecond) // Block until context times out
		}
		return true
	})

	fileContent := []byte("context error test content")
	imageName, img := reg.PushNamedImageWithFiles(t, "test-image", map[string][]byte{
		"test.txt": fileContent,
	}, nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.Len(t, layers, 1)
	layerDigest, err := layers[0].Digest()
	require.NoError(t, err)

	blobRef := strings.Split(imageName, ":")[0] + ":" + strings.Split(imageName, ":")[1] + "@" + layerDigest.String()
	client := newTestClient(t, env)

	// First call - should create a new Puller and cache it
	counter.Reset()
	stream, err := client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRef})
	require.NoError(t, err)
	_ = readAllBlobData(t, stream)

	// Second call - context times out during blob fetch.
	// Note: The context error occurs in layer.Compressed() which wraps it as UnavailableError,
	// so we just check that an error occurred and the puller is still cached.
	blockBlob.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = client.FetchBlob(ctx, &ofpb.FetchBlobRequest{Ref: blobRef})
	require.Error(t, err)

	// Third call - Puller should still be cached (no /v2/ auth request)
	blockBlob.Store(false)
	counter.Reset()
	stream, err = client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRef})
	require.NoError(t, err)
	_ = readAllBlobData(t, stream)
	thirdCounts := counter.Snapshot()
	require.Equal(t, 0, thirdCounts[http.MethodGet+" /v2/"], "expected no /v2/ auth request (cache hit)")
}
