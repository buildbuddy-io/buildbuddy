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
	"github.com/google/go-containerregistry/pkg/v1"
	"github.com/jonboulle/clockwork"
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
	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
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

// TestFetchManifestMetadata_PullerExpiration verifies that Pullers are expired
// and recreated after the TTL.
func TestFetchManifestMetadata_PullerExpiration(t *testing.T) {
	env := testenv.GetTestEnv(t)
	fakeClock := clockwork.NewFakeClock()
	env.SetClock(fakeClock)

	reg, counter := setupRegistry(t, nil, nil)
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

	// Second call (within TTL) - should reuse the cached Puller
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	})

	// Advance clock past the 15-minute TTL
	fakeClock.Advance(16 * time.Minute)

	// Third call (after TTL) - should create a new Puller
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	assertMetadata(t, resp, digest, size, mediaType)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
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

// ==================== FetchManifest Tests ====================

func imageManifest(t *testing.T, img v1.Image) []byte {
	manifest, err := img.RawManifest()
	require.NoError(t, err)
	return manifest
}

func assertManifestResponse(t *testing.T, resp *ofpb.FetchManifestResponse, digest string, size int64, mediaType string, manifest []byte) {
	require.Equal(t, digest, resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, mediaType, resp.GetMediaType())
	require.Equal(t, manifest, resp.GetManifest())
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
			manifest := imageManifest(t, img)

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

// TestFetchManifest_PullerCacheHit verifies that the same Puller is
// reused when fetching the same image with the same credentials.
func TestFetchManifest_PullerCacheHit(t *testing.T) {
	env := testenv.GetTestEnv(t)
	reg, counter := setupRegistry(t, nil, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, size, mediaType := imageMetadata(t, img)
	manifest := imageManifest(t, img)
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

// TestFetchManifest_PullerEvictionOnGetError verifies that when Get()
// returns an error, the Puller is evicted from the cache and a new one is
// created on the next request.
func TestFetchManifest_PullerEvictionOnGetError(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var failGet atomic.Bool
	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if failGet.Load() && r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/manifests/") {
			w.WriteHeader(http.StatusInternalServerError)
			return false
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, size, mediaType := imageMetadata(t, img)
	manifest := imageManifest(t, img)
	client := newTestClient(t, env)

	// First call - should create a new Puller
	counter.Reset()
	resp, err := client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	assertManifestResponse(t, resp, digest, size, mediaType, manifest)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                            1,
		http.MethodGet + " /v2/test-image/manifests/latest": 1,
	})

	// Second call - should reuse cached Puller (no /v2/ auth request)
	counter.Reset()
	resp, err = client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	assertManifestResponse(t, resp, digest, size, mediaType, manifest)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/test-image/manifests/latest": 1,
	})

	// Third call - Get() fails, Puller should be evicted
	failGet.Store(true)
	_, err = client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.Error(t, err)

	// Fourth call - should create a NEW Puller because the old one was evicted
	failGet.Store(false)
	counter.Reset()
	resp, err = client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	assertManifestResponse(t, resp, digest, size, mediaType, manifest)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                            1,
		http.MethodGet + " /v2/test-image/manifests/latest": 1,
	})
}

// TestFetchManifest_ContextErrorNoRetry verifies that when Get()
// returns a context error (canceled or deadline exceeded), no retry is
// attempted and the Puller is NOT evicted from the cache.
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
	manifest := imageManifest(t, img)
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

// TestFetchManifest_HTTPErrorThenSuccess verifies that when Get()
// returns an HTTP error on the first attempt, the Puller is refreshed and
// the retry succeeds. The refreshed Puller should remain in the cache.
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
	manifest := imageManifest(t, img)
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

// TestFetchManifest_HTTPErrorThenHTTPError verifies that when Get()
// returns HTTP errors on both the first attempt and the retry, the refreshed
// Puller is evicted from the cache.
func TestFetchManifest_HTTPErrorThenHTTPError(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var failGet atomic.Bool
	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if failGet.Load() && r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/manifests/") {
			w.WriteHeader(http.StatusInternalServerError)
			return false
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, size, mediaType := imageMetadata(t, img)
	manifest := imageManifest(t, img)
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

	// Second call - both Get attempts fail (don't assert exact counts due to internal retries)
	failGet.Store(true)
	_, err = client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.Error(t, err)

	// Third call - refreshed Puller should have been evicted (needs /v2/ auth request)
	failGet.Store(false)
	counter.Reset()
	resp, err = client.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	assertManifestResponse(t, resp, digest, size, mediaType, manifest)
	assertRequests(t, counter, map[string]int{
		http.MethodGet + " /v2/":                            1,
		http.MethodGet + " /v2/test-image/manifests/latest": 1,
	})
}

// ==================== FetchBlobMetadata Tests ====================

func blobMetadata(t *testing.T, img v1.Image) (digest string, size int64, mediaType string) {
	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	layer := layers[0]

	d, err := layer.Digest()
	require.NoError(t, err)
	s, err := layer.Size()
	require.NoError(t, err)
	m, err := layer.MediaType()
	require.NoError(t, err)
	return d.String(), s, string(m)
}

func assertBlobMetadataResponse(t *testing.T, resp *ofpb.FetchBlobMetadataResponse, size int64, mediaType string) {
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, mediaType, resp.GetMediaType())
}

func TestFetchBlobMetadata_AuthVariants(t *testing.T) {
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
		},
		{
			name:          "WithValidCredentials",
			registryCreds: &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"},
			requestCreds:  &rgpb.Credentials{Username: "testuser", Password: "testpass"},
			expectError:   false,
		},
		{
			name:          "WithInvalidCredentials",
			registryCreds: &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"},
			requestCreds:  &rgpb.Credentials{Username: "wronguser", Password: "wrongpass"},
			expectError:   true,
			checkError:    status.IsPermissionDeniedError,
		},
		{
			name:          "MissingCredentials",
			registryCreds: &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"},
			requestCreds:  nil,
			expectError:   true,
			checkError:    status.IsPermissionDeniedError,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := testenv.GetTestEnv(t)
			reg, _ := setupRegistry(t, tc.registryCreds, nil)
			imageName, img := reg.PushNamedImage(t, "test-image", tc.registryCreds)
			blobDigest, blobSize, blobMediaType := blobMetadata(t, img)

			// Construct blob reference: registry/repo@digest
			// imageName is like "localhost:port/test-image:latest", we need "localhost:port/test-image@sha256:..."
			parts := strings.Split(imageName, ":")
			blobRefStr := parts[0] + ":" + parts[1] + "@" + blobDigest

			client := newTestClient(t, env)
			resp, err := client.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{
				Ref:         blobRefStr,
				Credentials: tc.requestCreds,
			})

			if tc.expectError {
				require.Error(t, err)
				if tc.checkError != nil {
					require.True(t, tc.checkError(err), "unexpected error type: %v", err)
				}
			} else {
				require.NoError(t, err)
				assertBlobMetadataResponse(t, resp, blobSize, blobMediaType)
			}
		})
	}
}

// TestFetchBlobMetadata_PullerCacheHit verifies that the same Puller is
// reused when fetching the same blob with the same credentials.
func TestFetchBlobMetadata_PullerCacheHit(t *testing.T) {
	env := testenv.GetTestEnv(t)
	reg, counter := setupRegistry(t, nil, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	blobDigest, blobSize, blobMediaType := blobMetadata(t, img)

	parts := strings.Split(imageName, ":")
	blobRefStr := parts[0] + ":" + parts[1] + "@" + blobDigest

	client := newTestClient(t, env)

	// First call - should create a new Puller and make a /v2/ auth request
	counter.Reset()
	resp, err := client.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: blobRefStr})
	require.NoError(t, err)
	assertBlobMetadataResponse(t, resp, blobSize, blobMediaType)
	firstCounts := counter.Snapshot()
	require.Equal(t, 1, firstCounts[http.MethodGet+" /v2/"], "expected /v2/ auth request on first call")

	// Second call - should reuse the cached Puller and NOT make another /v2/ auth request
	counter.Reset()
	resp, err = client.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: blobRefStr})
	require.NoError(t, err)
	assertBlobMetadataResponse(t, resp, blobSize, blobMediaType)
	secondCounts := counter.Snapshot()
	require.Equal(t, 0, secondCounts[http.MethodGet+" /v2/"], "expected no /v2/ auth request on second call (cached puller)")
}

// TestFetchBlobMetadata_PullerEvictionOnError verifies that when blob fetch
// returns an error, the Puller is evicted from the cache and a new one is
// created on the next request.
func TestFetchBlobMetadata_PullerEvictionOnError(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var failBlob atomic.Bool
	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if failBlob.Load() && strings.Contains(r.URL.Path, "/blobs/") {
			w.WriteHeader(http.StatusInternalServerError)
			return false
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	blobDigest, blobSize, blobMediaType := blobMetadata(t, img)

	parts := strings.Split(imageName, ":")
	blobRefStr := parts[0] + ":" + parts[1] + "@" + blobDigest

	client := newTestClient(t, env)

	// First call - should create a new Puller
	counter.Reset()
	resp, err := client.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: blobRefStr})
	require.NoError(t, err)
	assertBlobMetadataResponse(t, resp, blobSize, blobMediaType)
	firstCounts := counter.Snapshot()
	require.Equal(t, 1, firstCounts[http.MethodGet+" /v2/"], "expected /v2/ auth request on first call")

	// Second call - should reuse cached Puller (no /v2/ auth request)
	counter.Reset()
	resp, err = client.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: blobRefStr})
	require.NoError(t, err)
	assertBlobMetadataResponse(t, resp, blobSize, blobMediaType)
	secondCounts := counter.Snapshot()
	require.Equal(t, 0, secondCounts[http.MethodGet+" /v2/"], "expected no /v2/ auth request on second call")

	// Third call - blob fetch fails, Puller should be evicted
	failBlob.Store(true)
	_, err = client.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: blobRefStr})
	require.Error(t, err)

	// Fourth call - should create a NEW Puller because the old one was evicted
	failBlob.Store(false)
	counter.Reset()
	resp, err = client.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: blobRefStr})
	require.NoError(t, err)
	assertBlobMetadataResponse(t, resp, blobSize, blobMediaType)
	fourthCounts := counter.Snapshot()
	require.Equal(t, 1, fourthCounts[http.MethodGet+" /v2/"], "expected /v2/ auth request after eviction")
}

// TestFetchBlobMetadata_ContextErrorNoRetry verifies that when blob fetch
// returns a context error (canceled or deadline exceeded), no retry is
// attempted and the Puller is NOT evicted from the cache.
func TestFetchBlobMetadata_ContextErrorNoRetry(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var blockBlob atomic.Bool
	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if blockBlob.Load() && strings.Contains(r.URL.Path, "/blobs/") {
			time.Sleep(100 * time.Millisecond) // Block until context times out
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	blobDigest, blobSize, blobMediaType := blobMetadata(t, img)

	parts := strings.Split(imageName, ":")
	blobRefStr := parts[0] + ":" + parts[1] + "@" + blobDigest

	client := newTestClient(t, env)

	// First call - should create a new Puller and cache it
	counter.Reset()
	resp, err := client.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: blobRefStr})
	require.NoError(t, err)
	assertBlobMetadataResponse(t, resp, blobSize, blobMediaType)
	firstCounts := counter.Snapshot()
	require.Equal(t, 1, firstCounts[http.MethodGet+" /v2/"], "expected /v2/ auth request on first call")

	// Second call - context times out during blob request
	blockBlob.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = client.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{Ref: blobRefStr})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded), "expected DeadlineExceeded error, got: %v", err)

	// Third call - Puller should still be cached (no /v2/ auth request)
	blockBlob.Store(false)
	counter.Reset()
	resp, err = client.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: blobRefStr})
	require.NoError(t, err)
	assertBlobMetadataResponse(t, resp, blobSize, blobMediaType)
	thirdCounts := counter.Snapshot()
	require.Equal(t, 0, thirdCounts[http.MethodGet+" /v2/"], "expected no /v2/ auth request (puller still cached)")
}

// TestFetchBlobMetadata_HTTPErrorThenSuccess verifies that when blob fetch
// returns an HTTP error on the first attempt, the Puller is refreshed and
// the retry succeeds. The refreshed Puller should remain in the cache.
func TestFetchBlobMetadata_HTTPErrorThenSuccess(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var blobAttempts atomic.Int32
	var failFirstBlob atomic.Bool
	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if failFirstBlob.Load() && strings.Contains(r.URL.Path, "/blobs/") {
			if blobAttempts.Add(1) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				return false
			}
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	blobDigest, blobSize, blobMediaType := blobMetadata(t, img)

	parts := strings.Split(imageName, ":")
	blobRefStr := parts[0] + ":" + parts[1] + "@" + blobDigest

	client := newTestClient(t, env)

	// First call - should create a new Puller and cache it
	counter.Reset()
	resp, err := client.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: blobRefStr})
	require.NoError(t, err)
	assertBlobMetadataResponse(t, resp, blobSize, blobMediaType)
	firstCounts := counter.Snapshot()
	require.Equal(t, 1, firstCounts[http.MethodGet+" /v2/"], "expected /v2/ auth request on first call")

	// Second call - first blob attempt fails, Puller refreshed, retry succeeds
	blobAttempts.Store(0)
	failFirstBlob.Store(true)
	resp, err = client.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: blobRefStr})
	require.NoError(t, err)
	assertBlobMetadataResponse(t, resp, blobSize, blobMediaType)

	// Third call - refreshed Puller should still be cached (no /v2/ auth request)
	failFirstBlob.Store(false)
	counter.Reset()
	resp, err = client.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: blobRefStr})
	require.NoError(t, err)
	assertBlobMetadataResponse(t, resp, blobSize, blobMediaType)
	thirdCounts := counter.Snapshot()
	require.Equal(t, 0, thirdCounts[http.MethodGet+" /v2/"], "expected no /v2/ auth request (refreshed puller cached)")
}

// TestFetchBlobMetadata_HTTPErrorThenHTTPError verifies that when blob fetch
// returns HTTP errors on both the first attempt and the retry, the refreshed
// Puller is evicted from the cache.
func TestFetchBlobMetadata_HTTPErrorThenHTTPError(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var failBlob atomic.Bool
	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if failBlob.Load() && strings.Contains(r.URL.Path, "/blobs/") {
			w.WriteHeader(http.StatusInternalServerError)
			return false
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	blobDigest, blobSize, blobMediaType := blobMetadata(t, img)

	parts := strings.Split(imageName, ":")
	blobRefStr := parts[0] + ":" + parts[1] + "@" + blobDigest

	client := newTestClient(t, env)

	// First call - should create a new Puller and cache it
	counter.Reset()
	resp, err := client.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: blobRefStr})
	require.NoError(t, err)
	assertBlobMetadataResponse(t, resp, blobSize, blobMediaType)
	firstCounts := counter.Snapshot()
	require.Equal(t, 1, firstCounts[http.MethodGet+" /v2/"], "expected /v2/ auth request on first call")

	// Second call - both blob attempts fail
	failBlob.Store(true)
	_, err = client.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: blobRefStr})
	require.Error(t, err)

	// Third call - refreshed Puller should have been evicted (needs /v2/ auth request)
	failBlob.Store(false)
	counter.Reset()
	resp, err = client.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{Ref: blobRefStr})
	require.NoError(t, err)
	assertBlobMetadataResponse(t, resp, blobSize, blobMediaType)
	thirdCounts := counter.Snapshot()
	require.Equal(t, 1, thirdCounts[http.MethodGet+" /v2/"], "expected /v2/ auth request after eviction")
}

// ==================== FetchBlob Tests ====================

func blobContent(t *testing.T, img v1.Image) []byte {
	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	reader, err := layers[0].Compressed()
	require.NoError(t, err)
	defer reader.Close()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	return data
}

func readAllFromStream(t *testing.T, stream ofpb.OCIFetcher_FetchBlobClient) []byte {
	var result []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return result
		}
		require.NoError(t, err)
		result = append(result, resp.GetData()...)
	}
}

func TestFetchBlob_AuthVariants(t *testing.T) {
	cases := []struct {
		name          string
		registryCreds *testregistry.BasicAuthCreds
		requestCreds  *rgpb.Credentials
		expectError   bool
		checkError    func(error) bool
	}{
		{
			name:          "NoAuth",
			registryCreds: nil,
			requestCreds:  nil,
			expectError:   false,
		},
		{
			name:          "WithValidCredentials",
			registryCreds: &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"},
			requestCreds:  &rgpb.Credentials{Username: "testuser", Password: "testpass"},
			expectError:   false,
		},
		{
			name:          "WithInvalidCredentials",
			registryCreds: &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"},
			requestCreds:  &rgpb.Credentials{Username: "wronguser", Password: "wrongpass"},
			expectError:   true,
			checkError:    status.IsPermissionDeniedError,
		},
		{
			name:          "MissingCredentials",
			registryCreds: &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"},
			requestCreds:  nil,
			expectError:   true,
			checkError:    status.IsPermissionDeniedError,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := testenv.GetTestEnv(t)
			reg, _ := setupRegistry(t, tc.registryCreds, nil)
			imageName, img := reg.PushNamedImage(t, "test-image", tc.registryCreds)
			blobDigest, _, _ := blobMetadata(t, img)
			expectedContent := blobContent(t, img)

			parts := strings.Split(imageName, ":")
			blobRefStr := parts[0] + ":" + parts[1] + "@" + blobDigest

			client := newTestClient(t, env)
			stream, err := client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{
				Ref:         blobRefStr,
				Credentials: tc.requestCreds,
			})

			if tc.expectError {
				require.Error(t, err)
				if tc.checkError != nil {
					require.True(t, tc.checkError(err), "unexpected error type: %v", err)
				}
			} else {
				require.NoError(t, err)
				content := readAllFromStream(t, stream)
				require.Equal(t, expectedContent, content, "blob content mismatch")
			}
		})
	}
}

// TestFetchBlob_PullerCacheHit verifies that the same Puller is
// reused when fetching the same blob with the same credentials.
func TestFetchBlob_PullerCacheHit(t *testing.T) {
	env := testenv.GetTestEnv(t)
	reg, counter := setupRegistry(t, nil, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	blobDigest, _, _ := blobMetadata(t, img)
	expectedContent := blobContent(t, img)

	parts := strings.Split(imageName, ":")
	blobRefStr := parts[0] + ":" + parts[1] + "@" + blobDigest

	client := newTestClient(t, env)

	// First call - should create a new Puller and make a /v2/ auth request
	counter.Reset()
	stream, err := client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRefStr})
	require.NoError(t, err)
	content := readAllFromStream(t, stream)
	require.Equal(t, expectedContent, content)
	firstCounts := counter.Snapshot()
	require.Equal(t, 1, firstCounts[http.MethodGet+" /v2/"], "expected /v2/ auth request on first call")

	// Second call - should reuse the cached Puller and NOT make another /v2/ auth request
	counter.Reset()
	stream, err = client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRefStr})
	require.NoError(t, err)
	content = readAllFromStream(t, stream)
	require.Equal(t, expectedContent, content)
	secondCounts := counter.Snapshot()
	require.Equal(t, 0, secondCounts[http.MethodGet+" /v2/"], "expected no /v2/ auth request on second call (cached puller)")
}

// TestFetchBlob_PullerEvictionOnError verifies that when blob fetch
// returns an error, the Puller is evicted from the cache and a new one is
// created on the next request.
func TestFetchBlob_PullerEvictionOnError(t *testing.T) {
	env := testenv.GetTestEnv(t)
	var failBlob atomic.Bool
	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if failBlob.Load() && strings.Contains(r.URL.Path, "/blobs/") {
			w.WriteHeader(http.StatusInternalServerError)
			return false
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	blobDigest, _, _ := blobMetadata(t, img)
	expectedContent := blobContent(t, img)

	parts := strings.Split(imageName, ":")
	blobRefStr := parts[0] + ":" + parts[1] + "@" + blobDigest

	client := newTestClient(t, env)

	// First call - should create a new Puller
	counter.Reset()
	stream, err := client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRefStr})
	require.NoError(t, err)
	content := readAllFromStream(t, stream)
	require.Equal(t, expectedContent, content)
	firstCounts := counter.Snapshot()
	require.Equal(t, 1, firstCounts[http.MethodGet+" /v2/"], "expected /v2/ auth request on first call")

	// Second call - should reuse cached Puller (no /v2/ auth request)
	counter.Reset()
	stream, err = client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRefStr})
	require.NoError(t, err)
	content = readAllFromStream(t, stream)
	require.Equal(t, expectedContent, content)
	secondCounts := counter.Snapshot()
	require.Equal(t, 0, secondCounts[http.MethodGet+" /v2/"], "expected no /v2/ auth request on second call")

	// Third call - blob fetch fails, Puller should be evicted
	failBlob.Store(true)
	_, err = client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRefStr})
	require.Error(t, err)

	// Fourth call - should create a NEW Puller because the old one was evicted
	failBlob.Store(false)
	counter.Reset()
	stream, err = client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRefStr})
	require.NoError(t, err)
	content = readAllFromStream(t, stream)
	require.Equal(t, expectedContent, content)
	fourthCounts := counter.Snapshot()
	require.Equal(t, 1, fourthCounts[http.MethodGet+" /v2/"], "expected /v2/ auth request after eviction")
}

// TestFetchBlob_StreamsContent verifies that FetchBlob streams all blob
// content correctly, including for larger blobs that require multiple chunks.
func TestFetchBlob_StreamsContent(t *testing.T) {
	env := testenv.GetTestEnv(t)
	reg, _ := setupRegistry(t, nil, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	blobDigest, _, _ := blobMetadata(t, img)
	expectedContent := blobContent(t, img)

	parts := strings.Split(imageName, ":")
	blobRefStr := parts[0] + ":" + parts[1] + "@" + blobDigest

	client := newTestClient(t, env)

	stream, err := client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: blobRefStr})
	require.NoError(t, err)

	// Verify we get data in chunks and it all matches
	var allData []byte
	chunkCount := 0
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetData(), "received empty chunk")
		allData = append(allData, resp.GetData()...)
		chunkCount++
	}

	require.Equal(t, expectedContent, allData, "streamed content doesn't match expected blob content")
	require.GreaterOrEqual(t, chunkCount, 1, "expected at least one chunk")
}
