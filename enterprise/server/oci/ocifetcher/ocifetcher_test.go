package ocifetcher_test

import (
	"context"
	"net"
	"net/http"
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

	expected := map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
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

	expected := map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
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
	imageName, _ := reg.PushNamedImage(t, "test-image", nil)

	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)

	// First call - should create a new Puller and make a /v2/ auth request
	counter.Reset()
	_, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)

	firstCallSnapshot := counter.Snapshot()
	require.Equal(t, 1, firstCallSnapshot[http.MethodGet+" /v2/"], "first call should make one /v2/ auth request")

	// Second call - should reuse the cached Puller and NOT make another /v2/ auth request
	counter.Reset()
	_, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)

	secondCallSnapshot := counter.Snapshot()
	require.Equal(t, 0, secondCallSnapshot[http.MethodGet+" /v2/"], "second call should not make /v2/ auth request (puller cached)")
	require.Equal(t, 1, secondCallSnapshot[http.MethodHead+" /v2/test-image/manifests/latest"], "second call should still make HEAD request")
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
	imageName, _ := reg.PushNamedImage(t, "test-image", nil)

	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)

	// First call with credentials A
	counter.Reset()
	_, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
		Credentials: &rgpb.Credentials{
			Username: "userA",
			Password: "passA",
		},
	})
	require.NoError(t, err)

	firstCallSnapshot := counter.Snapshot()
	require.Equal(t, 1, firstCallSnapshot[http.MethodGet+" /v2/"], "first call with creds A should make /v2/ auth request")

	// Second call with different credentials B - should create a new Puller
	counter.Reset()
	_, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
		Credentials: &rgpb.Credentials{
			Username: "userB",
			Password: "passB",
		},
	})
	require.NoError(t, err)

	secondCallSnapshot := counter.Snapshot()
	require.Equal(t, 1, secondCallSnapshot[http.MethodGet+" /v2/"], "call with different creds should create new puller and make /v2/ auth request")
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
	imageName1, _ := reg.PushNamedImage(t, "test-image:v1", nil)
	imageName2, _ := reg.PushNamedImage(t, "test-image:v2", nil)

	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)

	// First call for tag v1
	counter.Reset()
	_, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName1})
	require.NoError(t, err)

	firstCallSnapshot := counter.Snapshot()
	require.Equal(t, 1, firstCallSnapshot[http.MethodGet+" /v2/"], "first call should make /v2/ auth request")

	// Second call for tag v2 - should reuse the same Puller (same repo + creds)
	counter.Reset()
	_, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName2})
	require.NoError(t, err)

	secondCallSnapshot := counter.Snapshot()
	require.Equal(t, 0, secondCallSnapshot[http.MethodGet+" /v2/"], "second call for different tag should reuse puller (no /v2/ request)")
	require.Equal(t, 1, secondCallSnapshot[http.MethodHead+" /v2/test-image/manifests/v2"], "should make HEAD request for v2 tag")
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
	imageName, _ := reg.PushNamedImage(t, "test-image", nil)

	client, err := ocifetcher.NewClient(env, localhostIPs(t), nil)
	require.NoError(t, err)

	// First call - should create a new Puller
	counter.Reset()
	_, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)

	firstCallSnapshot := counter.Snapshot()
	require.Equal(t, 1, firstCallSnapshot[http.MethodGet+" /v2/"], "first call should make /v2/ auth request")

	// Second call (within TTL) - should reuse the cached Puller
	counter.Reset()
	_, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)

	secondCallSnapshot := counter.Snapshot()
	require.Equal(t, 0, secondCallSnapshot[http.MethodGet+" /v2/"], "second call should reuse cached puller")

	// Advance clock past the 15-minute TTL
	fakeClock.Advance(16 * time.Minute)

	// Third call (after TTL) - should create a new Puller
	counter.Reset()
	_, err = client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)

	thirdCallSnapshot := counter.Snapshot()
	require.Equal(t, 1, thirdCallSnapshot[http.MethodGet+" /v2/"], "third call (after TTL) should create new puller and make /v2/ auth request")
}
