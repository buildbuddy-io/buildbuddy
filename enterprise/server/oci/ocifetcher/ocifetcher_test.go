package ocifetcher_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-cmp/cmp"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
)

func TestFetchManifestMetadata_NoAuth(t *testing.T) {
	env := testenv.GetTestEnv(t)
	counter := testhttp.NewRequestCounter()
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, err := img.Digest()
	require.NoError(t, err)
	size, err := img.Size()
	require.NoError(t, err)
	mediaType, err := img.MediaType()
	require.NoError(t, err)

	counter.Reset()
	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)

	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected := map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

func TestFetchManifestMetadata_WithValidCredentials(t *testing.T) {
	env := testenv.GetTestEnv(t)
	counter := testhttp.NewRequestCounter()
	creds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
	reg := testregistry.Run(t, testregistry.Opts{
		Creds: creds,
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	imageName, img := reg.PushNamedImage(t, "test-image", creds)
	digest, err := img.Digest()
	require.NoError(t, err)
	size, err := img.Size()
	require.NoError(t, err)
	mediaType, err := img.MediaType()
	require.NoError(t, err)

	counter.Reset()
	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)
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

	expected := map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

func TestFetchManifestMetadata_WithInvalidCredentials(t *testing.T) {
	env := testenv.GetTestEnv(t)
	counter := testhttp.NewRequestCounter()
	creds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
	reg := testregistry.Run(t, testregistry.Opts{
		Creds: creds,
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	imageName, _ := reg.PushNamedImage(t, "test-image", creds)

	counter.Reset()
	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)
	_, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
		Credentials: &rgpb.Credentials{
			Username: "wronguser",
			Password: "wrongpass",
		},
	})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied error, got: %v", err)

	// Expects 2 attempts: first fails, Puller refreshed, second fails
	expected := map[string]int{
		http.MethodGet + " /v2/":                             2,
		http.MethodHead + " /v2/test-image/manifests/latest": 2,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

func TestFetchManifestMetadata_MissingCredentials(t *testing.T) {
	env := testenv.GetTestEnv(t)
	counter := testhttp.NewRequestCounter()
	creds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
	reg := testregistry.Run(t, testregistry.Opts{
		Creds: creds,
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	imageName, _ := reg.PushNamedImage(t, "test-image", creds)

	counter.Reset()
	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)
	_, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied error, got: %v", err)

	// Expects 2 attempts: first fails, Puller refreshed, second fails
	expected := map[string]int{
		http.MethodGet + " /v2/":                             2,
		http.MethodHead + " /v2/test-image/manifests/latest": 2,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

func localhostIPs(t *testing.T) []*net.IPNet {
	_, ipv4Net, err := net.ParseCIDR("127.0.0.0/8")
	require.NoError(t, err)
	_, ipv6Net, err := net.ParseCIDR("::1/128")
	require.NoError(t, err)
	return []*net.IPNet{ipv4Net, ipv6Net}
}

// TestFetchManifestMetadata_PullerCacheHit verifies that the same Puller is
// reused when fetching the same image with the same credentials.
func TestFetchManifestMetadata_PullerCacheHit(t *testing.T) {
	env := testenv.GetTestEnv(t)
	counter := testhttp.NewRequestCounter()
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, err := img.Digest()
	require.NoError(t, err)
	size, err := img.Size()
	require.NoError(t, err)
	mediaType, err := img.MediaType()
	require.NoError(t, err)

	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)

	// First call - should create a new Puller and make a /v2/ auth request
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected := map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))

	// Second call - should reuse the cached Puller and NOT make another /v2/ auth request
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected = map[string]int{
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

// TestFetchManifestMetadata_DifferentCredentials_DifferentPullers verifies that
// different credentials result in different Pullers being created.
func TestFetchManifestMetadata_DifferentCredentials_DifferentPullers(t *testing.T) {
	env := testenv.GetTestEnv(t)
	counter := testhttp.NewRequestCounter()
	// Use a registry that accepts any credentials
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, err := img.Digest()
	require.NoError(t, err)
	size, err := img.Size()
	require.NoError(t, err)
	mediaType, err := img.MediaType()
	require.NoError(t, err)

	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)

	// First call with credentials A
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
		Credentials: &rgpb.Credentials{
			Username: "userA",
			Password: "passA",
		},
	})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected := map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))

	// Second call with different credentials B - should create a new Puller
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
		Credentials: &rgpb.Credentials{
			Username: "userB",
			Password: "passB",
		},
	})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected = map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

// TestFetchManifestMetadata_SameRepoDifferentTags_SamePuller verifies that
// fetching different tags from the same repository reuses the same Puller.
func TestFetchManifestMetadata_SameRepoDifferentTags_SamePuller(t *testing.T) {
	env := testenv.GetTestEnv(t)
	counter := testhttp.NewRequestCounter()
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})

	// Push two images with different tags under the same repository
	imageName1, img1 := reg.PushNamedImage(t, "test-image:v1", nil)
	digest1, err := img1.Digest()
	require.NoError(t, err)
	size1, err := img1.Size()
	require.NoError(t, err)
	mediaType1, err := img1.MediaType()
	require.NoError(t, err)

	imageName2, img2 := reg.PushNamedImage(t, "test-image:v2", nil)
	digest2, err := img2.Digest()
	require.NoError(t, err)
	size2, err := img2.Size()
	require.NoError(t, err)
	mediaType2, err := img2.MediaType()
	require.NoError(t, err)

	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)

	// First call for tag v1
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName1})
	require.NoError(t, err)
	require.Equal(t, digest1.String(), resp.GetDigest())
	require.Equal(t, size1, resp.GetSize())
	require.Equal(t, string(mediaType1), resp.GetMediaType())

	expected := map[string]int{
		http.MethodGet + " /v2/":                         1,
		http.MethodHead + " /v2/test-image/manifests/v1": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))

	// Second call for tag v2 - should reuse the same Puller (same repo + creds)
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName2})
	require.NoError(t, err)
	require.Equal(t, digest2.String(), resp.GetDigest())
	require.Equal(t, size2, resp.GetSize())
	require.Equal(t, string(mediaType2), resp.GetMediaType())

	expected = map[string]int{
		http.MethodHead + " /v2/test-image/manifests/v2": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

// TestFetchManifestMetadata_PullerExpiration verifies that Pullers are expired
// and recreated after the TTL.
func TestFetchManifestMetadata_PullerExpiration(t *testing.T) {
	env := testenv.GetTestEnv(t)
	fakeClock := clockwork.NewFakeClock()
	env.SetClock(fakeClock)

	counter := testhttp.NewRequestCounter()
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, err := img.Digest()
	require.NoError(t, err)
	size, err := img.Size()
	require.NoError(t, err)
	mediaType, err := img.MediaType()
	require.NoError(t, err)

	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)

	// First call - should create a new Puller
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected := map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))

	// Second call (within TTL) - should reuse the cached Puller
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected = map[string]int{
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))

	// Advance clock past the 15-minute TTL
	fakeClock.Advance(16 * time.Minute)

	// Third call (after TTL) - should create a new Puller
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected = map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

// TestFetchManifestMetadata_PullerEvictionOnHeadError verifies that when Head()
// returns an error, the Puller is evicted from the cache and a new one is
// created on the next request.
func TestFetchManifestMetadata_PullerEvictionOnHeadError(t *testing.T) {
	env := testenv.GetTestEnv(t)
	counter := testhttp.NewRequestCounter()
	var failHead atomic.Bool
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			if failHead.Load() && r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/manifests/") {
				w.WriteHeader(http.StatusInternalServerError)
				return false
			}
			return true
		},
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, err := img.Digest()
	require.NoError(t, err)
	size, err := img.Size()
	require.NoError(t, err)
	mediaType, err := img.MediaType()
	require.NoError(t, err)

	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)

	// First call - should create a new Puller
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected := map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))

	// Second call - should reuse cached Puller (no /v2/ auth request)
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected = map[string]int{
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))

	// Third call - Head() fails, Puller should be evicted
	failHead.Store(true)
	_, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.Error(t, err)

	// Fourth call - should create a NEW Puller because the old one was evicted
	failHead.Store(false)
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected = map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

// TestFetchManifestMetadata_ContextErrorNoRetry verifies that when Head()
// returns a context error (canceled or deadline exceeded), no retry is
// attempted and the Puller is NOT evicted from the cache.
func TestFetchManifestMetadata_ContextErrorNoRetry(t *testing.T) {
	env := testenv.GetTestEnv(t)
	counter := testhttp.NewRequestCounter()
	var blockHead atomic.Bool
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			if blockHead.Load() && r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/manifests/") {
				// Block until the context is canceled, which will cause a context error
				time.Sleep(100 * time.Millisecond)
				return true
			}
			return true
		},
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, err := img.Digest()
	require.NoError(t, err)
	size, err := img.Size()
	require.NoError(t, err)
	mediaType, err := img.MediaType()
	require.NoError(t, err)

	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)

	// First call - should create a new Puller and cache it
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected := map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))

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
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected = map[string]int{
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

// TestFetchManifestMetadata_HTTPErrorThenSuccess verifies that when Head()
// returns an HTTP error on the first attempt, the Puller is refreshed and
// the retry succeeds. The refreshed Puller should remain in the cache.
func TestFetchManifestMetadata_HTTPErrorThenSuccess(t *testing.T) {
	env := testenv.GetTestEnv(t)
	counter := testhttp.NewRequestCounter()
	var headAttempts atomic.Int32
	var failFirstHead atomic.Bool
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			if failFirstHead.Load() && r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/manifests/") {
				attempt := headAttempts.Add(1)
				if attempt == 1 {
					w.WriteHeader(http.StatusInternalServerError)
					return false
				}
			}
			return true
		},
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, err := img.Digest()
	require.NoError(t, err)
	size, err := img.Size()
	require.NoError(t, err)
	mediaType, err := img.MediaType()
	require.NoError(t, err)

	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)

	// First call - should create a new Puller and cache it
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected := map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))

	// Second call - first Head fails, Puller refreshed, retry succeeds
	headAttempts.Store(0)
	failFirstHead.Store(true)
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	// Third call - refreshed Puller should still be cached (no /v2/ auth request)
	failFirstHead.Store(false)
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected = map[string]int{
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

// TestFetchManifestMetadata_HTTPErrorThenHTTPError verifies that when Head()
// returns HTTP errors on both the first attempt and the retry, the refreshed
// Puller is evicted from the cache.
func TestFetchManifestMetadata_HTTPErrorThenHTTPError(t *testing.T) {
	env := testenv.GetTestEnv(t)
	counter := testhttp.NewRequestCounter()
	var failHead atomic.Bool
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			if failHead.Load() && r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/manifests/") {
				w.WriteHeader(http.StatusInternalServerError)
				return false
			}
			return true
		},
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, err := img.Digest()
	require.NoError(t, err)
	size, err := img.Size()
	require.NoError(t, err)
	mediaType, err := img.MediaType()
	require.NoError(t, err)

	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)

	// First call - should create a new Puller and cache it
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected := map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))

	// Second call - both Head attempts fail (don't assert exact counts due to internal retries)
	failHead.Store(true)
	_, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.Error(t, err)

	// Third call - refreshed Puller should have been evicted (needs /v2/ auth request)
	failHead.Store(false)
	counter.Reset()
	resp, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected = map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

// TestFetchManifestMetadata_HTTPErrorThenContextError verifies that when Head()
// returns an HTTP error on the first attempt and a context error on the retry,
// the refreshed Puller is NOT evicted from the cache.
func TestFetchManifestMetadata_HTTPErrorThenContextError(t *testing.T) {
	env := testenv.GetTestEnv(t)
	counter := testhttp.NewRequestCounter()
	var headAttempts atomic.Int32
	var failFirstThenBlock atomic.Bool
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			if failFirstThenBlock.Load() && r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/manifests/") {
				attempt := headAttempts.Add(1)
				if attempt == 1 {
					// First attempt: return HTTP error to trigger refresh
					w.WriteHeader(http.StatusInternalServerError)
					return false
				}
				// Second attempt: block until context times out
				time.Sleep(100 * time.Millisecond)
				return true
			}
			return true
		},
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, err := img.Digest()
	require.NoError(t, err)
	size, err := img.Size()
	require.NoError(t, err)
	mediaType, err := img.MediaType()
	require.NoError(t, err)

	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)

	// First call - should create a new Puller and cache it
	counter.Reset()
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected := map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))

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
	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected = map[string]int{
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}
