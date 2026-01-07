package ocifetcher_test

import (
	"context"
	"errors"
	"io"
	"net/http"
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
	// Copy bytes since upstream may reuse buffers.
	cp := make([]byte, len(resp.GetData()))
	copy(cp, resp.GetData())
	m.responses = append(m.responses, &ofpb.FetchBlobResponse{Data: cp})
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

// concurrentMockFetchBlobServer is a thread-safe mock with error injection support.
type concurrentMockFetchBlobServer struct {
	grpc.ServerStream
	ctx           context.Context
	mu            sync.Mutex
	responses     []*ofpb.FetchBlobResponse
	sendErr       error // If set, Send() returns this error
	sendErrAfterN int   // Fail after N successful sends (0 = fail immediately)
	sendCount     int
	// If set, only fail if we successfully CAS this from false to true (i.e., we're the first sender)
	firstSenderFlag *atomic.Bool
	isFirstSender   bool // Set to true if we won the CAS race
}

func (m *concurrentMockFetchBlobServer) Send(resp *ofpb.FetchBlobResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// On first send, determine if we're the first sender (leader)
	if m.sendCount == 0 && m.firstSenderFlag != nil {
		m.isFirstSender = m.firstSenderFlag.CompareAndSwap(false, true)
	}

	// Check if we should fail
	if m.sendErr != nil {
		shouldFail := m.firstSenderFlag == nil || m.isFirstSender
		if shouldFail && m.sendCount >= m.sendErrAfterN {
			return m.sendErr
		}
	}

	m.sendCount++
	// Copy bytes since upstream may reuse buffers.
	cp := make([]byte, len(resp.GetData()))
	copy(cp, resp.GetData())
	m.responses = append(m.responses, &ofpb.FetchBlobResponse{Data: cp})
	return nil
}

func (m *concurrentMockFetchBlobServer) Context() context.Context {
	return m.ctx
}

func (m *concurrentMockFetchBlobServer) collectData() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	var data []byte
	for _, resp := range m.responses {
		data = append(data, resp.GetData()...)
	}
	return data
}

// failingByteStreamClient always errors on Write and reports NotFound on Read/Query.
type failingByteStreamClient struct {
	bspb.ByteStreamClient
}

func (f failingByteStreamClient) Write(context.Context, ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
	return nil, status.InternalError("cache write failed")
}

func (f failingByteStreamClient) Read(context.Context, *bspb.ReadRequest, ...grpc.CallOption) (bspb.ByteStream_ReadClient, error) {
	return nil, status.NotFoundError("not found")
}

func (f failingByteStreamClient) QueryWriteStatus(context.Context, *bspb.QueryWriteStatusRequest, ...grpc.CallOption) (*bspb.QueryWriteStatusResponse, error) {
	return nil, status.NotFoundError("not found")
}

// notFoundActionCacheClient always returns NotFound.
type notFoundActionCacheClient struct {
	repb.ActionCacheClient
}

func (n notFoundActionCacheClient) GetActionResult(context.Context, *repb.GetActionResultRequest, ...grpc.CallOption) (*repb.ActionResult, error) {
	return nil, status.NotFoundError("not found")
}

func (n notFoundActionCacheClient) UpdateActionResult(context.Context, *repb.UpdateActionResultRequest, ...grpc.CallOption) (*repb.ActionResult, error) {
	return nil, status.NotFoundError("not found")
}

// testBarrier synchronizes N goroutines to start concurrently.
type testBarrier struct {
	count   int
	waiting int
	mu      sync.Mutex
	cond    *sync.Cond
}

func newTestBarrier(count int) *testBarrier {
	b := &testBarrier{count: count}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *testBarrier) Wait() {
	b.mu.Lock()
	b.waiting++
	if b.waiting == b.count {
		b.cond.Broadcast()
	}
	for b.waiting < b.count {
		b.cond.Wait()
	}
	b.mu.Unlock()
}

// blockingInterceptor blocks HTTP requests until signaled.
type blockingInterceptor struct {
	enabled      atomic.Bool   // Must be enabled before blocking starts
	blockUntil   chan struct{}
	requestCount atomic.Int32
	failWithCode atomic.Int32 // 0 = don't fail, >0 = return this HTTP status for blob requests
}

func newBlockingInterceptor() *blockingInterceptor {
	return &blockingInterceptor{
		blockUntil: make(chan struct{}),
	}
}

func (bi *blockingInterceptor) enable() {
	bi.enabled.Store(true)
}

func (bi *blockingInterceptor) intercept(w http.ResponseWriter, r *http.Request) bool {
	if !bi.enabled.Load() {
		return true // Not enabled yet, let requests through (e.g., during image push)
	}
	if strings.Contains(r.URL.Path, "/blobs/") {
		bi.requestCount.Add(1)
		<-bi.blockUntil // Block until released

		if code := bi.failWithCode.Load(); code > 0 {
			w.WriteHeader(int(code))
			return false
		}
	}
	return true
}

func (bi *blockingInterceptor) release() {
	close(bi.blockUntil)
}

// fetchResult holds the result of a concurrent FetchBlob call.
type fetchResult struct {
	idx  int
	data []byte
	err  error
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

// runConcurrentFetchBlob runs numRequests concurrent FetchBlob calls and returns results.
func runConcurrentFetchBlob(
	t *testing.T,
	server ofpb.OCIFetcherServer,
	req *ofpb.FetchBlobRequest,
	numRequests int,
	streamFactory func(idx int) *concurrentMockFetchBlobServer,
) []*fetchResult {
	barrier := newTestBarrier(numRequests)
	results := make([]*fetchResult, numRequests)
	var wg sync.WaitGroup

	for i := 0; i < numRequests; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			stream := streamFactory(i)

			barrier.Wait() // All goroutines start together

			err := server.FetchBlob(req, stream)

			results[i] = &fetchResult{
				idx:  i,
				data: stream.collectData(),
				err:  err,
			}
		}()
	}

	wg.Wait()
	return results
}

func TestFetchBlob_Singleflight(t *testing.T) {
	const numRequests = 10

	t.Run("HappyPath", func(t *testing.T) {
		blocker := newBlockingInterceptor()
		reg, counter := setupRegistry(t, nil, blocker.intercept)
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

		req := &ofpb.FetchBlobRequest{
			Ref: imageName + "@" + digest.String(),
		}

		// Enable blocking now that setup is complete
		blocker.enable()

		// Release the blocker after all requests have arrived
		go func() {
			require.Eventually(t, func() bool {
				return blocker.requestCount.Load() >= 1
			}, 5*time.Second, 10*time.Millisecond, "expected at least 1 HTTP request to arrive")
			// Small delay to let any additional requests arrive at singleflight
			time.Sleep(50 * time.Millisecond)
			blocker.release()
		}()

		results := runConcurrentFetchBlob(t, server, req, numRequests,
			func(idx int) *concurrentMockFetchBlobServer {
				return &concurrentMockFetchBlobServer{ctx: context.Background()}
			},
		)

		// 1. Only 1 HTTP request to registry (singleflight deduplication)
		blobPath := http.MethodGet + " /v2/test-image/blobs/" + digest.String()
		require.Equal(t, 1, counter.Snapshot()[blobPath], "expected exactly 1 HTTP request due to singleflight")

		// 2. All 10 streams receive correct data
		for i, r := range results {
			require.NoError(t, r.err, "request %d should succeed", i)
			require.Equal(t, expectedData, r.data, "request %d should have correct data", i)
		}
	})

	t.Run("CacheHitBeforeSingleflight", func(t *testing.T) {
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

		req := &ofpb.FetchBlobRequest{
			Ref: imageName + "@" + digest.String(),
		}

		// Pre-populate cache with a single request
		stream := &mockFetchBlobServer{ctx: context.Background()}
		err = server.FetchBlob(req, stream)
		require.NoError(t, err)

		// Reset counter before concurrent requests
		counter.Reset()

		results := runConcurrentFetchBlob(t, server, req, numRequests,
			func(idx int) *concurrentMockFetchBlobServer {
				return &concurrentMockFetchBlobServer{ctx: context.Background()}
			},
		)

		// 1. No HTTP requests (all served from cache)
		assertRequests(t, counter, map[string]int{})

		// 2. All requests succeed with correct data
		for i, r := range results {
			require.NoError(t, r.err, "request %d should succeed", i)
			require.Equal(t, expectedData, r.data, "request %d should have correct data", i)
		}
	})

	t.Run("LeaderFailsUpstream", func(t *testing.T) {
		blocker := newBlockingInterceptor()
		blocker.failWithCode.Store(http.StatusInternalServerError)

		reg, counter := setupRegistry(t, nil, blocker.intercept)
		imageName, img := reg.PushNamedImage(t, "test-image", nil)

		layers, err := img.Layers()
		require.NoError(t, err)
		require.NotEmpty(t, layers)

		layer := layers[0]
		digest, err := layer.Digest()
		require.NoError(t, err)

		server := newTestServer(t)

		req := &ofpb.FetchBlobRequest{
			Ref: imageName + "@" + digest.String(),
		}

		// Enable blocking now that setup is complete
		blocker.enable()

		// Release the blocker after requests arrive
		go func() {
			require.Eventually(t, func() bool {
				return blocker.requestCount.Load() >= 1
			}, 5*time.Second, 10*time.Millisecond)
			time.Sleep(50 * time.Millisecond)
			blocker.release()
		}()

		results := runConcurrentFetchBlob(t, server, req, numRequests,
			func(idx int) *concurrentMockFetchBlobServer {
				return &concurrentMockFetchBlobServer{ctx: context.Background()}
			},
		)

		// 1. Singleflight deduplication: fewer HTTP requests than concurrent callers.
		// Note: retries on HTTP 500 mean we may see >1 request, but should be << numRequests.
		blobPath := http.MethodGet + " /v2/test-image/blobs/" + digest.String()
		httpRequestCount := counter.Snapshot()[blobPath]
		require.Less(t, httpRequestCount, numRequests, "singleflight should deduplicate (got %d requests for %d callers)", httpRequestCount, numRequests)

		// 2. All 10 callers should get an error
		for i, r := range results {
			require.Error(t, r.err, "request %d should fail", i)
		}
	})

	t.Run("LeaderFailsStreamResponse", func(t *testing.T) {
		blocker := newBlockingInterceptor()
		reg, counter := setupRegistry(t, nil, blocker.intercept)
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

		req := &ofpb.FetchBlobRequest{
			Ref: imageName + "@" + digest.String(),
		}

		// Shared flag to track which stream is the first to send (the leader)
		var firstSenderFlag atomic.Bool
		streams := make([]*concurrentMockFetchBlobServer, numRequests)

		// Enable blocking now that setup is complete
		blocker.enable()

		// Release the blocker after requests arrive
		go func() {
			require.Eventually(t, func() bool {
				return blocker.requestCount.Load() >= 1
			}, 5*time.Second, 10*time.Millisecond)
			blocker.release()
		}()

		results := runConcurrentFetchBlob(t, server, req, numRequests,
			func(idx int) *concurrentMockFetchBlobServer {
				stream := &concurrentMockFetchBlobServer{
					ctx:             context.Background(),
					sendErr:         status.InternalError("simulated Send failure"),
					sendErrAfterN:   0, // Fail immediately on first send
					firstSenderFlag: &firstSenderFlag,
				}
				streams[idx] = stream
				return stream
			},
			)

		// 1. Verify singleflight deduplication (only 1 HTTP request)
		blobPath := http.MethodGet + " /v2/test-image/blobs/" + digest.String()
		require.Equal(t, 1, counter.Snapshot()[blobPath], "singleflight should deduplicate to 1 HTTP request")

		// 2. All requests should fail (leader failed on first Send = no cache data written)
		for i, r := range results {
			require.Error(t, r.err, "request %d should fail (leader failed before writing to cache)", i)
		}

		// 3. Subsequent fetch should succeed by refetching from registry (cache was not populated).
		counter.Reset()
		stream := &mockFetchBlobServer{ctx: context.Background()}
		err = server.FetchBlob(req, stream)
		require.NoError(t, err, "subsequent fetch should succeed")
		require.Equal(t, expectedData, stream.collectData())
		require.LessOrEqual(t, counter.Snapshot()[blobPath], 1, "subsequent fetch should be served from cache or a single upstream retry")
	})

	t.Run("CacheSetupFailureLeaderStreamsFollowersCacheMiss", func(t *testing.T) {
		reg, counter := setupRegistry(t, nil, nil)
		imageName, img := reg.PushNamedImage(t, "test-image", nil)

		layers, err := img.Layers()
		require.NoError(t, err)
		require.NotEmpty(t, layers)

		layer := layers[0]
		digest, err := layer.Digest()
		require.NoError(t, err)
		expectedData := layerData(t, layer)

		// Use cache clients that force caching to fail so the leader streams without caching.
		bsClient := failingByteStreamClient{}
		acClient := notFoundActionCacheClient{}
		server := newTestServerWithCache(t, bsClient, acClient)

		req := &ofpb.FetchBlobRequest{
			Ref: imageName + "@" + digest.String(),
		}

		const numRequests = 4
		results := runConcurrentFetchBlob(t, server, req, numRequests,
			func(idx int) *concurrentMockFetchBlobServer {
				return &concurrentMockFetchBlobServer{ctx: context.Background()}
			},
		)

		blobPath := http.MethodGet + " /v2/test-image/blobs/" + digest.String()
		require.Equal(t, 1, counter.Snapshot()[blobPath], "requests should be deduped even when caching fails")

		successes := 0
		for i, r := range results {
			if r.err == nil {
				successes++
				require.Equal(t, expectedData, r.data, "request %d should stream data", i)
			} else {
				require.True(t, status.IsNotFoundError(r.err), "request %d should fail with cache miss, got %v", i, r.err)
			}
		}
		require.Equal(t, 1, successes, "only the leader should succeed when caching fails")
	})

	t.Run("OneFollowerCancelled", func(t *testing.T) {
		blocker := newBlockingInterceptor()
		reg, _ := setupRegistry(t, nil, blocker.intercept)
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

		req := &ofpb.FetchBlobRequest{
			Ref: imageName + "@" + digest.String(),
		}

		cancelFuncs := make([]context.CancelFunc, numRequests)
		var cancelFuncsMu sync.Mutex
		var cancelFuncsReady sync.WaitGroup
		cancelFuncsReady.Add(numRequests)

		// Track which stream is the leader (first to send)
		var firstSenderFlag atomic.Bool
		streams := make([]*concurrentMockFetchBlobServer, numRequests)

		// Enable blocking now that setup is complete
		blocker.enable()

		// Cancel one confirmed follower after requests enter singleflight but before completion
		go func() {
			// Wait for all cancel funcs to be populated
			cancelFuncsReady.Wait()

			require.Eventually(t, func() bool {
				return blocker.requestCount.Load() >= 1
			}, 5*time.Second, 10*time.Millisecond)

			// Find a non-leader to cancel. Since we're blocking before data is sent,
			// we can't know who the leader is yet. Cancel index 0 - if it happens to
			// be the leader, the leader continues anyway (WithoutCancel), so we check
			// the result based on whether it was actually the leader.
			cancelFuncsMu.Lock()
			cancelFuncs[0]()
			cancelFuncsMu.Unlock()

			blocker.release()
		}()

		results := runConcurrentFetchBlob(t, server, req, numRequests,
			func(idx int) *concurrentMockFetchBlobServer {
				ctx, cancel := context.WithCancel(context.Background())
				cancelFuncsMu.Lock()
				cancelFuncs[idx] = cancel
				cancelFuncsMu.Unlock()
				cancelFuncsReady.Done()
				stream := &concurrentMockFetchBlobServer{
					ctx:             ctx,
					firstSenderFlag: &firstSenderFlag,
				}
				streams[idx] = stream
				return stream
			},
		)

		// Determine if request 0 was the leader
		streams[0].mu.Lock()
		request0WasLeader := streams[0].isFirstSender
		streams[0].mu.Unlock()

		// Check results based on whether request 0 was the leader or a follower
		for i, r := range results {
			if i == 0 {
				// The cancelled request may be a follower (expected context.Canceled) or the leader
				// (singleflight waiter can return context.Canceled even though the goroutine completes).
				if r.err != nil {
					require.True(t, errors.Is(r.err, context.Canceled), "cancelled request 0 should get context.Canceled, got: %v", r.err)
					continue
				}
				require.Equal(t, expectedData, r.data, "request 0 should have correct data if not cancelled")
			} else {
				require.NoError(t, r.err, "request %d should succeed", i)
				require.Equal(t, expectedData, r.data, "request %d should have correct data", i)
			}
		}

		// Ensure at least one request was cancelled (we cancelled request 0's context)
		require.Error(t, results[0].err, "cancelled request should return an error")
	})

	t.Run("DifferentCredentialsNotDeduplicated", func(t *testing.T) {
		// Requests with different credentials should NOT be deduplicated
		// because the singleflight key includes a credentials hash.
		blocker := newBlockingInterceptor()
		creds := &testregistry.BasicAuthCreds{Username: "user", Password: "pass"}
		reg, counter := setupRegistry(t, creds, blocker.intercept)
		imageName, img := reg.PushNamedImage(t, "test-image", creds)

		layers, err := img.Layers()
		require.NoError(t, err)
		require.NotEmpty(t, layers)

		layer := layers[0]
		digest, err := layer.Digest()
		require.NoError(t, err)
		expectedData := layerData(t, layer)

		server := newTestServer(t)

		// Two different credential sets (both valid for this registry)
		creds1 := &rgpb.Credentials{Username: "user", Password: "pass"}
		creds2 := &rgpb.Credentials{Username: "user", Password: "pass2"} // Different password

		req1 := &ofpb.FetchBlobRequest{
			Ref:         imageName + "@" + digest.String(),
			Credentials: creds1,
		}
		req2 := &ofpb.FetchBlobRequest{
			Ref:         imageName + "@" + digest.String(),
			Credentials: creds2,
		}

		blocker.enable()

		var wg sync.WaitGroup
		var result1Err, result2Err error
		var result1Data []byte

		// Launch both requests concurrently
		wg.Add(2)
		go func() {
			defer wg.Done()
			stream := &mockFetchBlobServer{ctx: context.Background()}
			result1Err = server.FetchBlob(req1, stream)
			result1Data = stream.collectData()
		}()
		go func() {
			defer wg.Done()
			stream := &mockFetchBlobServer{ctx: context.Background()}
			result2Err = server.FetchBlob(req2, stream)
		}()

		// Wait for both requests to arrive at the blocker
		require.Eventually(t, func() bool {
			return blocker.requestCount.Load() >= 2
		}, 5*time.Second, 10*time.Millisecond, "expected 2 HTTP requests (not deduplicated)")

		blocker.release()
		wg.Wait()

		// First request (valid creds) should succeed
		require.NoError(t, result1Err, "request with valid creds should succeed")
		require.Equal(t, expectedData, result1Data)

		// Second request (invalid creds) should fail with auth error
		require.Error(t, result2Err, "request with invalid creds should fail")

		// Key assertion: 2 separate HTTP requests were made (not deduplicated)
		blobPath := http.MethodGet + " /v2/test-image/blobs/" + digest.String()
		require.Equal(t, 2, counter.Snapshot()[blobPath], "different credentials should result in separate requests")
	})
}
