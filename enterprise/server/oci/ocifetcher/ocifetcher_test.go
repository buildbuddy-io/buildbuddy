package ocifetcher_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

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

func newTestServer(t *testing.T) ofpb.OCIFetcherServer {
	_, bsClient, acClient := setupCacheEnv(t)
	return newTestServerWithCache(t, bsClient, acClient)
}

func newTestServerWithCache(t *testing.T, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient) ofpb.OCIFetcherServer {
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.0/8", "::1/128"})
	server, err := ocifetcher.NewServer(bsClient, acClient)
	require.NoError(t, err)
	return server
}

func setupCacheEnv(t *testing.T) (*testenv.TestEnv, bspb.ByteStreamClient, repb.ActionCacheClient) {
	te := testenv.GetTestEnv(t)
	enterprise_testenv.AddClientIdentity(t, te, interfaces.ClientIdentityApp)
	_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, localGRPClis)
	go runServer()
	return te, te.GetByteStreamClient(), te.GetActionCacheClient()
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

		t.Run("FetchBlob/WithCaching/"+ac.name, func(t *testing.T) {
			reg, counter := setupRegistry(t, ac.registryCreds, nil)
			imageName, img := reg.PushNamedImage(t, "test-image", ac.registryCreds)

			layers, err := img.Layers()
			require.NoError(t, err)
			require.NotEmpty(t, layers)

			layer := layers[0]
			digest, err := layer.Digest()
			require.NoError(t, err)
			expectedData := layerData(t, layer)

			_, bsClient, acClient := setupCacheEnv(t)
			server := newTestServerWithCache(t, bsClient, acClient)

			// First fetch - cache miss, should hit registry
			counter.Reset()
			stream := &mockFetchBlobServer{ctx: context.Background()}
			err = server.FetchBlob(&ofpb.FetchBlobRequest{
				Ref:         imageName + "@" + digest.String(),
				Credentials: ac.requestCreds,
			}, stream)
			require.NoError(t, err)
			require.Equal(t, expectedData, stream.collectData())

			// Verify registry was hit (blob request)
			blobRequests := counter.Snapshot()[http.MethodGet+" /v2/test-image/blobs/"+digest.String()]
			require.Greater(t, blobRequests, 0, "expected at least one blob request to registry on cache miss")

			// Second fetch - cache hit, should NOT hit registry
			counter.Reset()
			stream = &mockFetchBlobServer{ctx: context.Background()}
			err = server.FetchBlob(&ofpb.FetchBlobRequest{
				Ref:         imageName + "@" + digest.String(),
				Credentials: ac.requestCreds,
			}, stream)
			require.NoError(t, err)
			require.Equal(t, expectedData, stream.collectData())

			// Verify registry was NOT hit (served from cache)
			assertRequests(t, counter, map[string]int{})
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
			require.True(t, status.IsUnauthenticatedError(err), "FetchManifestMetadata: expected Unauthenticated, got: %v", err)
			assertRequests(t, counter, map[string]int{
				http.MethodGet + " /v2/":                             2,
				http.MethodHead + " /v2/test-image/manifests/latest": 2,
			})

			// FetchManifest (uses HEAD to resolve tag to digest first, then fails on auth)
			counter.Reset()
			_, err = server.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{
				Ref:         imageName,
				Credentials: cc.requestCreds,
			})
			require.Error(t, err)
			require.True(t, status.IsUnauthenticatedError(err), "FetchManifest: expected Unauthenticated, got: %v", err)
			assertRequests(t, counter, map[string]int{
				http.MethodGet + " /v2/":                             2,
				http.MethodHead + " /v2/test-image/manifests/latest": 2,
			})

			// FetchBlobMetadata
			counter.Reset()
			_, err = server.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{
				Ref:         imageName + "@" + layerDigest.String(),
				Credentials: cc.requestCreds,
			})
			require.Error(t, err)
			require.True(t, status.IsUnauthenticatedError(err), "FetchBlobMetadata: expected Unauthenticated, got: %v", err)

			// FetchBlob
			counter.Reset()
			stream := &mockFetchBlobServer{ctx: context.Background()}
			err = server.FetchBlob(&ofpb.FetchBlobRequest{
				Ref:         imageName + "@" + layerDigest.String(),
				Credentials: cc.requestCreds,
			}, stream)
			require.Error(t, err)
			require.True(t, status.IsUnauthenticatedError(err), "FetchBlob: expected Unauthenticated, got: %v", err)
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
	// Note: with caching enabled, layer.Size() also makes a HEAD request after the retry succeeds
	blobAttempts.Store(0)
	stream := &mockFetchBlobServer{ctx: context.Background()}
	err = server.FetchBlob(&ofpb.FetchBlobRequest{Ref: imageName + "@" + layerDigest.String()}, stream)
	require.NoError(t, err)
	require.Equal(t, expectedLayerData, stream.collectData())
	require.GreaterOrEqual(t, blobAttempts.Load(), int32(2), "expected at least 2 blob attempts")
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

	// FetchManifest: Puller should still be cached (but HEAD and GET both happen for tag refs)
	blockGet.Store(false)
	counter.Reset()
	manifestResp, err := server.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, expectedDigest, manifestResp.GetDigest())
	require.Equal(t, expectedSize, manifestResp.GetSize())
	require.Equal(t, expectedMediaType, manifestResp.GetMediaType())
	require.Equal(t, expectedManifest, manifestResp.GetManifest())
	assertRequests(t, counter, map[string]int{
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
		http.MethodGet + " /v2/test-image/manifests/latest":  1,
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

	// FetchBlob: first fetch caches the blob
	stream := &mockFetchBlobServer{ctx: context.Background()}
	err = server.FetchBlob(&ofpb.FetchBlobRequest{Ref: imageName + "@" + layerDigest.String()}, stream)
	require.NoError(t, err)
	require.Equal(t, expectedLayerData, stream.collectData())

	// FetchBlob: second fetch serves from cache (no network calls), even with short timeout
	// Since blob is cached, there's no network call to timeout, so this succeeds instantly
	counter.Reset()
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	stream = &mockFetchBlobServer{ctx: ctx}
	err = server.FetchBlob(&ofpb.FetchBlobRequest{Ref: imageName + "@" + layerDigest.String()}, stream)
	cancel()
	require.NoError(t, err, "cached blob should serve without network calls")
	require.Equal(t, expectedLayerData, stream.collectData())
	// Verify no registry requests were made (served from cache)
	assertRequests(t, counter, map[string]int{})
}

func TestServerBypassRegistry(t *testing.T) {
	const adminGroupID = "GR123"

	adminUser := &claims.Claims{
		UserID:        "US1",
		GroupID:       adminGroupID,
		AllowedGroups: []string{adminGroupID},
		GroupMemberships: []*interfaces.GroupMembership{
			{
				GroupID:      adminGroupID,
				Capabilities: []cappb.Capability{cappb.Capability_ORG_ADMIN},
			},
		},
	}

	for _, tc := range []struct {
		name       string
		user       *claims.Claims
		checkError func(error) bool
	}{
		{
			name:       "NonAdmin_PermissionDenied",
			user:       nil,
			checkError: status.IsPermissionDeniedError,
		},
		{
			name:       "Admin_NotFound",
			user:       adminUser,
			checkError: status.IsNotFoundError,
		},
	} {
		t.Run("FetchManifestMetadata/"+tc.name, func(t *testing.T) {
			flags.Set(t, "auth.admin_group_id", adminGroupID)
			server := newTestServer(t)
			ctx := context.Background()
			if tc.user != nil {
				ctx = testauth.WithAuthenticatedUserInfo(ctx, tc.user)
			}
			_, err := server.FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{
				Ref:            "gcr.io/test/image:latest",
				BypassRegistry: true,
			})
			require.Error(t, err)
			require.True(t, tc.checkError(err), "unexpected error type: %v", err)
		})

		t.Run("FetchManifest/"+tc.name, func(t *testing.T) {
			flags.Set(t, "auth.admin_group_id", adminGroupID)
			server := newTestServer(t)
			ctx := context.Background()
			if tc.user != nil {
				ctx = testauth.WithAuthenticatedUserInfo(ctx, tc.user)
			}
			_, err := server.FetchManifest(ctx, &ofpb.FetchManifestRequest{
				Ref:            "gcr.io/test/image:latest",
				BypassRegistry: true,
			})
			require.Error(t, err)
			require.True(t, tc.checkError(err), "unexpected error type: %v", err)
		})

		t.Run("FetchBlobMetadata/CacheMiss/"+tc.name, func(t *testing.T) {
			flags.Set(t, "auth.admin_group_id", adminGroupID)
			server := newTestServer(t)
			ctx := context.Background()
			if tc.user != nil {
				ctx = testauth.WithAuthenticatedUserInfo(ctx, tc.user)
			}
			_, err := server.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{
				Ref:            "gcr.io/test/image@sha256:abc123abc123abc123abc123abc123abc123abc123abc123abc123abc123abc1",
				BypassRegistry: true,
			})
			require.Error(t, err)
			require.True(t, tc.checkError(err), "unexpected error type: %v", err)
		})

		t.Run("FetchBlob/CacheMiss/"+tc.name, func(t *testing.T) {
			flags.Set(t, "auth.admin_group_id", adminGroupID)
			server := newTestServer(t)
			ctx := context.Background()
			if tc.user != nil {
				ctx = testauth.WithAuthenticatedUserInfo(ctx, tc.user)
			}
			stream := &mockFetchBlobServer{ctx: ctx}
			err := server.FetchBlob(&ofpb.FetchBlobRequest{
				Ref:            "gcr.io/test/image@sha256:abc123abc123abc123abc123abc123abc123abc123abc123abc123abc123abc1",
				BypassRegistry: true,
			}, stream)
			require.Error(t, err)
			require.True(t, tc.checkError(err), "unexpected error type: %v", err)
		})
	}

	// Test that FetchBlob with bypass_registry serves from cache when blob is cached
	t.Run("FetchBlob/CacheHit/ServesFromCache", func(t *testing.T) {
		flags.Set(t, "auth.admin_group_id", adminGroupID)

		reg, counter := setupRegistry(t, nil, nil)
		imageName, img := reg.PushNamedImage(t, "test-image", nil)

		layers, err := img.Layers()
		require.NoError(t, err)
		require.NotEmpty(t, layers)

		layer := layers[0]
		digest, err := layer.Digest()
		require.NoError(t, err)
		expectedData := layerData(t, layer)

		_, bsClient, acClient := setupCacheEnv(t)
		server := newTestServerWithCache(t, bsClient, acClient)

		// First fetch - populate cache
		stream := &mockFetchBlobServer{ctx: context.Background()}
		err = server.FetchBlob(&ofpb.FetchBlobRequest{
			Ref: imageName + "@" + digest.String(),
		}, stream)
		require.NoError(t, err)
		require.Equal(t, expectedData, stream.collectData())

		// Second fetch with bypass_registry (admin required) - should serve from cache without registry contact
		counter.Reset()
		ctx := testauth.WithAuthenticatedUserInfo(context.Background(), adminUser)
		stream = &mockFetchBlobServer{ctx: ctx}
		err = server.FetchBlob(&ofpb.FetchBlobRequest{
			Ref:            imageName + "@" + digest.String(),
			BypassRegistry: true,
		}, stream)
		require.NoError(t, err)
		require.Equal(t, expectedData, stream.collectData())

		// Verify no registry requests were made
		assertRequests(t, counter, map[string]int{})
	})

	// Test that FetchBlob with bypass_registry for admin returns NotFound and doesn't fallback to registry
	t.Run("FetchBlob/CacheMiss/Admin_NoRegistryFallback", func(t *testing.T) {
		flags.Set(t, "auth.admin_group_id", adminGroupID)

		// Set up a real registry with a real blob
		reg, counter := setupRegistry(t, nil, nil)
		imageName, img := reg.PushNamedImage(t, "test-image", nil)
		layers, err := img.Layers()
		require.NoError(t, err)
		require.NotEmpty(t, layers)
		layer := layers[0]
		digest, err := layer.Digest()
		require.NoError(t, err)

		// Create server (cache is empty - blob not cached)
		server := newTestServer(t)

		// Admin user context
		ctx := testauth.WithAuthenticatedUserInfo(context.Background(), adminUser)

		// Reset counter before the bypass_registry request
		counter.Reset()

		stream := &mockFetchBlobServer{ctx: ctx}
		err = server.FetchBlob(&ofpb.FetchBlobRequest{
			Ref:            imageName + "@" + digest.String(),
			BypassRegistry: true,
		}, stream)

		// Should return NotFoundError (blob not in cache)
		require.Error(t, err)
		require.True(t, status.IsNotFoundError(err), "expected NotFoundError, got: %v", err)

		// Verify NO registry requests were made (no fallback to remote)
		assertRequests(t, counter, map[string]int{})
	})

	// Test that FetchBlobMetadata returns cached metadata when available
	t.Run("FetchBlobMetadata/WithCaching", func(t *testing.T) {
		reg, counter := setupRegistry(t, nil, nil)
		imageName, img := reg.PushNamedImage(t, "test-image", nil)

		layers, err := img.Layers()
		require.NoError(t, err)
		require.NotEmpty(t, layers)

		layer := layers[0]
		digest, err := layer.Digest()
		require.NoError(t, err)
		expectedSize, expectedMediaType := layerMetadata(t, layer)

		_, bsClient, acClient := setupCacheEnv(t)
		server := newTestServerWithCache(t, bsClient, acClient)

		// First, fetch the blob to populate the cache (FetchBlob writes metadata to AC)
		stream := &mockFetchBlobServer{ctx: context.Background()}
		err = server.FetchBlob(&ofpb.FetchBlobRequest{
			Ref: imageName + "@" + digest.String(),
		}, stream)
		require.NoError(t, err)

		// Now fetch metadata - should be served from cache
		counter.Reset()
		resp, err := server.FetchBlobMetadata(context.Background(), &ofpb.FetchBlobMetadataRequest{
			Ref: imageName + "@" + digest.String(),
		})
		require.NoError(t, err)
		require.Equal(t, expectedSize, resp.GetSize())
		require.Equal(t, expectedMediaType, resp.GetMediaType())

		// Verify no registry requests were made (served from cache)
		assertRequests(t, counter, map[string]int{})
	})

	// Test that FetchBlobMetadata with bypass_registry serves from cache when metadata is cached
	t.Run("FetchBlobMetadata/CacheHit/ServesFromCache", func(t *testing.T) {
		flags.Set(t, "auth.admin_group_id", adminGroupID)

		reg, counter := setupRegistry(t, nil, nil)
		imageName, img := reg.PushNamedImage(t, "test-image", nil)

		layers, err := img.Layers()
		require.NoError(t, err)
		require.NotEmpty(t, layers)

		layer := layers[0]
		digest, err := layer.Digest()
		require.NoError(t, err)
		expectedSize, expectedMediaType := layerMetadata(t, layer)

		_, bsClient, acClient := setupCacheEnv(t)
		server := newTestServerWithCache(t, bsClient, acClient)

		// First fetch - populate cache via FetchBlob
		stream := &mockFetchBlobServer{ctx: context.Background()}
		err = server.FetchBlob(&ofpb.FetchBlobRequest{
			Ref: imageName + "@" + digest.String(),
		}, stream)
		require.NoError(t, err)

		// Second fetch with bypass_registry (admin required) - should serve from cache without registry contact
		counter.Reset()
		ctx := testauth.WithAuthenticatedUserInfo(context.Background(), adminUser)
		resp, err := server.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{
			Ref:            imageName + "@" + digest.String(),
			BypassRegistry: true,
		})
		require.NoError(t, err)
		require.Equal(t, expectedSize, resp.GetSize())
		require.Equal(t, expectedMediaType, resp.GetMediaType())

		// Verify no registry requests were made
		assertRequests(t, counter, map[string]int{})
	})

	// Test that FetchBlobMetadata with bypass_registry for admin returns NotFound and doesn't fallback to registry
	t.Run("FetchBlobMetadata/CacheMiss/Admin_NoRegistryFallback", func(t *testing.T) {
		flags.Set(t, "auth.admin_group_id", adminGroupID)

		// Set up a real registry with a real blob
		reg, counter := setupRegistry(t, nil, nil)
		imageName, img := reg.PushNamedImage(t, "test-image", nil)
		layers, err := img.Layers()
		require.NoError(t, err)
		require.NotEmpty(t, layers)
		layer := layers[0]
		digest, err := layer.Digest()
		require.NoError(t, err)

		// Create server (cache is empty - blob metadata not cached)
		server := newTestServer(t)

		// Admin user context
		ctx := testauth.WithAuthenticatedUserInfo(context.Background(), adminUser)

		// Reset counter before the bypass_registry request
		counter.Reset()

		_, err = server.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{
			Ref:            imageName + "@" + digest.String(),
			BypassRegistry: true,
		})

		// Should return NotFoundError (metadata not in cache)
		require.Error(t, err)
		require.True(t, status.IsNotFoundError(err), "expected NotFoundError, got: %v", err)

		// Verify NO registry requests were made (no fallback to remote)
		assertRequests(t, counter, map[string]int{})
	})

	// Test that FetchManifest returns cached manifest when available (digest ref)
	t.Run("FetchManifest/WithCaching/DigestRef", func(t *testing.T) {
		reg, counter := setupRegistry(t, nil, nil)
		imageName, img := reg.PushNamedImage(t, "test-image", nil)
		digest, _, _ := imageMetadata(t, img)
		expectedManifest, err := img.RawManifest()
		require.NoError(t, err)

		_, bsClient, acClient := setupCacheEnv(t)
		server := newTestServerWithCache(t, bsClient, acClient)

		// First fetch by digest - cache miss, should fetch from registry and cache
		counter.Reset()
		resp, err := server.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{
			Ref: imageName + "@" + digest,
		})
		require.NoError(t, err)
		require.Equal(t, expectedManifest, resp.GetManifest())

		// Verify registry was hit
		require.Greater(t, counter.Snapshot()[http.MethodGet+" /v2/test-image/manifests/"+digest], 0)

		// Second fetch by digest - should serve from cache
		counter.Reset()
		resp, err = server.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{
			Ref: imageName + "@" + digest,
		})
		require.NoError(t, err)
		require.Equal(t, expectedManifest, resp.GetManifest())

		// Verify no registry requests were made (served from cache)
		assertRequests(t, counter, map[string]int{})
	})

	// Test that FetchManifest caching works for tag refs (HEAD to resolve, then cache lookup)
	t.Run("FetchManifest/WithCaching/TagRef", func(t *testing.T) {
		reg, counter := setupRegistry(t, nil, nil)
		imageName, img := reg.PushNamedImage(t, "test-image", nil)
		digest, _, _ := imageMetadata(t, img)
		expectedManifest, err := img.RawManifest()
		require.NoError(t, err)

		_, bsClient, acClient := setupCacheEnv(t)
		server := newTestServerWithCache(t, bsClient, acClient)

		// First fetch by tag - cache miss, should HEAD then GET from registry
		counter.Reset()
		resp, err := server.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{
			Ref: imageName,
		})
		require.NoError(t, err)
		require.Equal(t, expectedManifest, resp.GetManifest())
		require.Equal(t, digest, resp.GetDigest())

		// Verify registry was hit (HEAD + GET)
		require.Greater(t, counter.Snapshot()[http.MethodHead+" /v2/test-image/manifests/latest"], 0)
		require.Greater(t, counter.Snapshot()[http.MethodGet+" /v2/test-image/manifests/latest"], 0)

		// Second fetch by tag - should only HEAD (cache hit avoids GET)
		counter.Reset()
		resp, err = server.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{
			Ref: imageName,
		})
		require.NoError(t, err)
		require.Equal(t, expectedManifest, resp.GetManifest())

		// Verify only HEAD was made (cache hit), no GET
		require.Greater(t, counter.Snapshot()[http.MethodHead+" /v2/test-image/manifests/latest"], 0)
		require.Equal(t, 0, counter.Snapshot()[http.MethodGet+" /v2/test-image/manifests/latest"])
	})

	// Test that FetchManifest with bypass_registry serves from cache when manifest is cached (digest ref)
	t.Run("FetchManifest/CacheHit/DigestRef/ServesFromCache", func(t *testing.T) {
		flags.Set(t, "auth.admin_group_id", adminGroupID)

		reg, counter := setupRegistry(t, nil, nil)
		imageName, img := reg.PushNamedImage(t, "test-image", nil)
		digest, _, _ := imageMetadata(t, img)
		expectedManifest, err := img.RawManifest()
		require.NoError(t, err)

		_, bsClient, acClient := setupCacheEnv(t)
		server := newTestServerWithCache(t, bsClient, acClient)

		// First fetch - populate cache
		resp, err := server.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{
			Ref: imageName + "@" + digest,
		})
		require.NoError(t, err)
		require.Equal(t, expectedManifest, resp.GetManifest())

		// Second fetch with bypass_registry (admin required) - should serve from cache
		counter.Reset()
		ctx := testauth.WithAuthenticatedUserInfo(context.Background(), adminUser)
		resp, err = server.FetchManifest(ctx, &ofpb.FetchManifestRequest{
			Ref:            imageName + "@" + digest,
			BypassRegistry: true,
		})
		require.NoError(t, err)
		require.Equal(t, expectedManifest, resp.GetManifest())

		// Verify no registry requests were made
		assertRequests(t, counter, map[string]int{})
	})

	// Test that FetchManifest with bypass_registry for admin returns NotFound for uncached digest ref
	t.Run("FetchManifest/CacheMiss/DigestRef/Admin_NoRegistryFallback", func(t *testing.T) {
		flags.Set(t, "auth.admin_group_id", adminGroupID)

		reg, counter := setupRegistry(t, nil, nil)
		imageName, img := reg.PushNamedImage(t, "test-image", nil)
		digest, _, _ := imageMetadata(t, img)

		// Create server (cache is empty)
		server := newTestServer(t)

		ctx := testauth.WithAuthenticatedUserInfo(context.Background(), adminUser)
		counter.Reset()

		_, err := server.FetchManifest(ctx, &ofpb.FetchManifestRequest{
			Ref:            imageName + "@" + digest,
			BypassRegistry: true,
		})

		require.Error(t, err)
		require.True(t, status.IsNotFoundError(err), "expected NotFoundError, got: %v", err)

		// Verify NO registry requests were made
		assertRequests(t, counter, map[string]int{})
	})

	// Test that FetchManifest with bypass_registry for admin returns NotFound for tag ref
	t.Run("FetchManifest/TagRef/Admin_NoRegistryFallback", func(t *testing.T) {
		flags.Set(t, "auth.admin_group_id", adminGroupID)

		reg, counter := setupRegistry(t, nil, nil)
		imageName, _ := reg.PushNamedImage(t, "test-image", nil)

		server := newTestServer(t)

		ctx := testauth.WithAuthenticatedUserInfo(context.Background(), adminUser)
		counter.Reset()

		_, err := server.FetchManifest(ctx, &ofpb.FetchManifestRequest{
			Ref:            imageName,
			BypassRegistry: true,
		})

		require.Error(t, err)
		require.True(t, status.IsNotFoundError(err), "expected NotFoundError, got: %v", err)

		// Verify NO registry requests were made (can't resolve tag without registry)
		assertRequests(t, counter, map[string]int{})
	})
}

func TestFetchBlobSingleflight(t *testing.T) {
	var blobRequestCount atomic.Int32
	const numConcurrent = 5

	// Barrier: all goroutines signal when they're about to call FetchBlob
	var allStarting sync.WaitGroup
	allStarting.Add(numConcurrent)
	proceedWithFetch := make(chan struct{})

	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if strings.Contains(r.URL.Path, "/blobs/") && r.Method == http.MethodGet {
			blobRequestCount.Add(1)
			// Wait for all goroutines to be in-flight before proceeding.
			// This ensures they have a chance to join singleflight.
			allStarting.Wait()
			<-proceedWithFetch
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)

	layer := layers[0]
	digest, err := layer.Digest()
	require.NoError(t, err)
	expectedData := layerData(t, layer)

	_, bsClient, acClient := setupCacheEnv(t)
	server := newTestServerWithCache(t, bsClient, acClient)

	var wg sync.WaitGroup
	results := make([][]byte, numConcurrent)
	errs := make([]error, numConcurrent)

	// Launch concurrent requests
	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			stream := &mockFetchBlobServer{ctx: context.Background()}
			// Signal before entering FetchBlob. We can't use defer here because
			// the handler blocks on allStarting.Wait() inside FetchBlob - using
			// defer would cause a deadlock since Done() wouldn't be called until
			// FetchBlob returns.
			allStarting.Done()
			errs[idx] = server.FetchBlob(&ofpb.FetchBlobRequest{
				Ref: imageName + "@" + digest.String(),
			}, stream)
			results[idx] = stream.collectData()
		}(i)
	}

	// Wait for all goroutines to signal, yield to let them enter FetchBlob/singleflight,
	// then release the handler.
	allStarting.Wait()
	for i := 0; i < 10; i++ {
		runtime.Gosched()
	}
	close(proceedWithFetch)
	wg.Wait()

	// All requests should succeed with correct data
	for i := 0; i < numConcurrent; i++ {
		require.NoError(t, errs[i], "request %d failed", i)
		require.Equal(t, expectedData, results[i], "request %d got wrong data", i)
	}

	// Should only hit registry once due to singleflight
	blobRequests := counter.Snapshot()[http.MethodGet+" /v2/test-image/blobs/"+digest.String()]
	require.Equal(t, 1, blobRequests, "expected exactly 1 registry blob request due to singleflight, got %d", blobRequests)
	require.Equal(t, int32(1), blobRequestCount.Load(), "expected exactly 1 blob request")
}

func TestFetchBlobSingleflightDifferentCreds(t *testing.T) {
	// Use a barrier to ensure both requests overlap in the registry handler
	var requestsStarted sync.WaitGroup
	requestsStarted.Add(2)
	releaseRequests := make(chan struct{})

	reg, counter := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if strings.Contains(r.URL.Path, "/blobs/") && r.Method == http.MethodGet {
			requestsStarted.Done()  // Signal this request has started
			<-releaseRequests       // Wait for both requests to start
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)

	layer := layers[0]
	digest, err := layer.Digest()
	require.NoError(t, err)
	expectedData := layerData(t, layer)

	_, bsClient, acClient := setupCacheEnv(t)
	server := newTestServerWithCache(t, bsClient, acClient)

	// Different credentials should NOT be deduplicated
	creds1 := &rgpb.Credentials{Username: "user1", Password: "pass1"}
	creds2 := &rgpb.Credentials{Username: "user2", Password: "pass2"}

	var wg sync.WaitGroup
	var result1, result2 []byte
	var err1, err2 error

	wg.Add(2)
	go func() {
		defer wg.Done()
		stream := &mockFetchBlobServer{ctx: context.Background()}
		err1 = server.FetchBlob(&ofpb.FetchBlobRequest{
			Ref:         imageName + "@" + digest.String(),
			Credentials: creds1,
		}, stream)
		result1 = stream.collectData()
	}()
	go func() {
		defer wg.Done()
		stream := &mockFetchBlobServer{ctx: context.Background()}
		err2 = server.FetchBlob(&ofpb.FetchBlobRequest{
			Ref:         imageName + "@" + digest.String(),
			Credentials: creds2,
		}, stream)
		result2 = stream.collectData()
	}()

	// Wait for both requests to reach the handler, then release them simultaneously
	go func() {
		requestsStarted.Wait()
		close(releaseRequests)
	}()

	wg.Wait()

	require.NoError(t, err1)
	require.NoError(t, err2)
	require.Equal(t, expectedData, result1)
	require.Equal(t, expectedData, result2)

	// Different credentials should result in 2 separate requests
	blobRequests := counter.Snapshot()[http.MethodGet+" /v2/test-image/blobs/"+digest.String()]
	require.Equal(t, 2, blobRequests, "different credentials should make separate requests")
}

func TestFetchBlobSingleflightSharedError(t *testing.T) {
	// Test that concurrent callers share the same upstream error via singleflight
	var blobRequestCount atomic.Int32
	const numConcurrent = 3

	// Barrier: all goroutines signal when they're about to call FetchBlob
	var allStarting sync.WaitGroup
	allStarting.Add(numConcurrent)
	releaseRequest := make(chan struct{})

	reg, _ := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if strings.Contains(r.URL.Path, "/blobs/") && r.Method == http.MethodGet {
			blobRequestCount.Add(1)
			// Wait for all goroutines to be in-flight before returning error
			allStarting.Wait()
			<-releaseRequest
			w.WriteHeader(http.StatusInternalServerError)
			return false // Don't proceed with normal handling
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)

	layer := layers[0]
	digest, err := layer.Digest()
	require.NoError(t, err)

	_, bsClient, acClient := setupCacheEnv(t)
	server := newTestServerWithCache(t, bsClient, acClient)

	var wg sync.WaitGroup
	errs := make([]error, numConcurrent)

	// Launch concurrent requests
	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			stream := &mockFetchBlobServer{ctx: context.Background()}
			allStarting.Done() // Signal before entering FetchBlob
			errs[idx] = server.FetchBlob(&ofpb.FetchBlobRequest{
				Ref: imageName + "@" + digest.String(),
			}, stream)
		}(i)
	}

	// Wait for all goroutines to signal, yield to let them enter FetchBlob/singleflight,
	// then release the handler.
	allStarting.Wait()
	for i := 0; i < 10; i++ {
		runtime.Gosched()
	}
	close(releaseRequest)

	wg.Wait()

	// All callers should receive an error
	for i, err := range errs {
		require.Error(t, err, "caller %d should have received an error", i)
	}

	// The first round of requests should be deduplicated via singleflight
	// After the error, retries may occur, but the initial concurrent requests should coalesce
	initialRequests := blobRequestCount.Load()
	require.GreaterOrEqual(t, initialRequests, int32(1), "expected at least 1 blob request")
	t.Logf("Total blob requests (including potential retries): %d", initialRequests)
}

func TestFetchBlobSingleflightMidStreamError(t *testing.T) {
	// Test that concurrent callers share the same mid-stream failure
	var blobRequestCount atomic.Int32
	const numConcurrent = 3

	// Barrier: all goroutines signal when they're about to call FetchBlob
	var allStarting sync.WaitGroup
	allStarting.Add(numConcurrent)
	releaseRequest := make(chan struct{})

	reg, _ := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if strings.Contains(r.URL.Path, "/blobs/") && r.Method == http.MethodGet {
			blobRequestCount.Add(1)
			// Wait for all goroutines to be in-flight before returning partial response
			allStarting.Wait()
			<-releaseRequest

			// Write partial data then close connection abruptly
			w.Header().Set("Content-Length", "1000") // Claim we'll send 1000 bytes
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("partial")) // Only write 7 bytes
			// Use http.Hijacker to close connection mid-stream
			if hj, ok := w.(http.Hijacker); ok {
				conn, _, _ := hj.Hijack()
				conn.Close() // Abruptly close - simulates network failure
			}
			return false
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)

	layer := layers[0]
	digest, err := layer.Digest()
	require.NoError(t, err)

	_, bsClient, acClient := setupCacheEnv(t)
	server := newTestServerWithCache(t, bsClient, acClient)

	var wg sync.WaitGroup
	errs := make([]error, numConcurrent)

	// Launch concurrent requests
	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			stream := &mockFetchBlobServer{ctx: context.Background()}
			allStarting.Done() // Signal before entering FetchBlob
			errs[idx] = server.FetchBlob(&ofpb.FetchBlobRequest{
				Ref: imageName + "@" + digest.String(),
			}, stream)
		}(i)
	}

	// Wait for all goroutines to signal, yield to let them enter FetchBlob/singleflight,
	// then release the handler.
	allStarting.Wait()
	for i := 0; i < 10; i++ {
		runtime.Gosched()
	}
	close(releaseRequest)

	wg.Wait()

	// All callers should receive an error (incomplete read)
	for i, err := range errs {
		require.Error(t, err, "caller %d should have received an error", i)
	}

	// The first round of requests should be deduplicated via singleflight
	// After the error, retries may occur, but the initial concurrent requests should coalesce
	initialRequests := blobRequestCount.Load()
	require.GreaterOrEqual(t, initialRequests, int32(1), "expected at least 1 blob request")
	t.Logf("Total blob requests (including potential retries): %d", initialRequests)
}

func TestFetchBlobSingleflightFirstCallerDisconnect(t *testing.T) {
	// Use barriers to control timing precisely
	var firstRequestOnce sync.Once
	firstRequestStarted := make(chan struct{})
	waiterStarting := make(chan struct{})
	proceedWithResponse := make(chan struct{})

	reg, _ := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if strings.Contains(r.URL.Path, "/blobs/") && r.Method == http.MethodGet {
			firstRequestOnce.Do(func() {
				close(firstRequestStarted)
			})
			// Wait for the waiter to signal it's about to call FetchBlob
			<-waiterStarting
			// Wait for test to signal we can complete
			<-proceedWithResponse
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)

	layer := layers[0]
	digest, err := layer.Digest()
	require.NoError(t, err)
	expectedData := layerData(t, layer)

	_, bsClient, acClient := setupCacheEnv(t)
	server := newTestServerWithCache(t, bsClient, acClient)

	ctx1, cancel1 := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	var waiterErr error
	var waiterData []byte

	// Start first caller (will be cancelled)
	wg.Add(1)
	go func() {
		defer wg.Done()
		stream := &mockFetchBlobServer{ctx: ctx1}
		// We don't care about first caller's error - it will be cancelled
		_ = server.FetchBlob(&ofpb.FetchBlobRequest{
			Ref: imageName + "@" + digest.String(),
		}, stream)
	}()

	// Wait for first request to reach handler
	<-firstRequestStarted

	// Start waiter - it signals before calling FetchBlob
	wg.Add(1)
	go func() {
		defer wg.Done()
		stream := &mockFetchBlobServer{ctx: context.Background()}
		close(waiterStarting) // Signal before entering FetchBlob
		waiterErr = server.FetchBlob(&ofpb.FetchBlobRequest{
			Ref: imageName + "@" + digest.String(),
		}, stream)
		waiterData = stream.collectData()
	}()

	// Wait for waiter to signal, yield to let it enter FetchBlob/singleflight,
	// then cancel first caller while request is in progress.
	<-waiterStarting
	for i := 0; i < 10; i++ {
		runtime.Gosched()
	}
	cancel1()

	// Let handler complete - waiter should get the data
	close(proceedWithResponse)

	wg.Wait()

	// Waiter should succeed with correct data (from cache after fetch completes)
	require.NoError(t, waiterErr, "waiter should succeed even when first caller disconnects")
	require.Equal(t, expectedData, waiterData, "waiter should get correct data")
}

func TestFetchBlobSingleflightAllCallersDisconnect(t *testing.T) {
	// Use barriers and fail-fast assertions instead of sleeps
	requestStarted := make(chan struct{})
	requestCancelled := make(chan struct{})

	reg, _ := setupRegistry(t, nil, func(w http.ResponseWriter, r *http.Request) bool {
		if strings.Contains(r.URL.Path, "/blobs/") && r.Method == http.MethodGet {
			close(requestStarted) // Signal request has started
			select {
			case <-r.Context().Done():
				close(requestCancelled) // Signal cancellation was received
			case <-time.After(5 * time.Second):
				// Should never reach - test will fail via timeout assertion
			}
		}
		return true
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)

	layer := layers[0]
	digest, err := layer.Digest()
	require.NoError(t, err)

	_, bsClient, acClient := setupCacheEnv(t)
	server := newTestServerWithCache(t, bsClient, acClient)

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	var err1, err2 error

	// Start two callers
	wg.Add(2)
	go func() {
		defer wg.Done()
		stream := &mockFetchBlobServer{ctx: ctx1}
		err1 = server.FetchBlob(&ofpb.FetchBlobRequest{
			Ref: imageName + "@" + digest.String(),
		}, stream)
	}()
	go func() {
		defer wg.Done()
		stream := &mockFetchBlobServer{ctx: ctx2}
		err2 = server.FetchBlob(&ofpb.FetchBlobRequest{
			Ref: imageName + "@" + digest.String(),
		}, stream)
	}()

	// Wait for request to start (replaces time.Sleep)
	<-requestStarted

	// Cancel all callers
	cancel1()
	cancel2()

	// Assert cancellation propagated with fail-fast timeout
	select {
	case <-requestCancelled:
		// Good - cancellation propagated to handler
	case <-time.After(1 * time.Second):
		t.Fatal("cancellation did not propagate to registry handler within 1s")
	}

	wg.Wait()

	// Both should error (context cancelled)
	require.Error(t, err1)
	require.Error(t, err2)
}
