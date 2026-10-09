package ocifetcher_server_proxy

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
)

// fakeAppsFetcher stands in for the apps' OCI fetcher. It serves a canned
// manifest and blob, and counts calls.
type fakeAppsFetcher struct {
	calls    atomic.Int32
	manifest []byte
	blob     []byte
}

func (f *fakeAppsFetcher) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest, opts ...grpc.CallOption) (*ofpb.FetchManifestResponse, error) {
	f.calls.Add(1)
	return &ofpb.FetchManifestResponse{Manifest: f.manifest, Size: int64(len(f.manifest))}, nil
}

func (f *fakeAppsFetcher) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest, opts ...grpc.CallOption) (*ofpb.FetchManifestMetadataResponse, error) {
	f.calls.Add(1)
	return &ofpb.FetchManifestMetadataResponse{Size: int64(len(f.manifest))}, nil
}

func (f *fakeAppsFetcher) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest, opts ...grpc.CallOption) (*ofpb.FetchBlobMetadataResponse, error) {
	f.calls.Add(1)
	return &ofpb.FetchBlobMetadataResponse{Size: int64(len(f.blob))}, nil
}

func (f *fakeAppsFetcher) FetchBlob(ctx context.Context, req *ofpb.FetchBlobRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ofpb.FetchBlobResponse], error) {
	f.calls.Add(1)
	return &oneShotBlobStream{data: f.blob}, nil
}

type oneShotBlobStream struct {
	grpc.ClientStream
	data []byte
	sent bool
}

func (s *oneShotBlobStream) Recv() (*ofpb.FetchBlobResponse, error) {
	if s.sent {
		return nil, io.EOF
	}
	s.sent = true
	return &ofpb.FetchBlobResponse{Data: s.data}, nil
}

// setFetchFromRegistry sets the cache_proxy.oci_fetch_from_registry
// experiment for every group.
func setFetchFromRegistry(t *testing.T, env *testenv.TestEnv, enabled bool) {
	variant := "off"
	if enabled {
		variant = "on"
	}
	provider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		fetchFromRegistryExperiment: {
			State:          memprovider.Enabled,
			DefaultVariant: variant,
			Variants:       map[string]any{"on": true, "off": false},
		},
	})
	require.NoError(t, openfeature.SetProviderAndWait(provider))
	fp, err := experiments.NewFlagProvider("test")
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)
	t.Cleanup(func() {
		require.NoError(t, openfeature.SetProviderAndWait(openfeature.NoopProvider{}))
	})
}

// newRegistryFetchingProxy returns a client for a proxy whose upstream is
// apps, with fetching from the registry enabled or not.
func newRegistryFetchingProxy(t *testing.T, apps ofpb.OCIFetcherClient, fetchFromRegistry bool) ofpb.OCIFetcherClient {
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.0/8", "::1/128"})

	env := newProxyEnv(t, apps, fetchFromRegistry)
	proxy, err := New(env)
	require.NoError(t, err)
	return serveProxy(t, env, proxy)
}

// newProxyEnv returns an environment for a proxy with a local cache.
func newProxyEnv(t *testing.T, apps ofpb.OCIFetcherClient, fetchFromRegistry bool) *testenv.TestEnv {
	cacheEnv := testenv.GetTestEnv(t)
	enterprise_testenv.AddClientIdentity(t, cacheEnv, interfaces.ClientIdentityCacheProxy)
	_, runCache, cacheLis := testenv.RegisterLocalGRPCServer(t, cacheEnv)
	testcache.Setup(t, cacheEnv, cacheLis)
	go runCache()

	env := testenv.GetTestEnv(t)
	env.SetOCIFetcherClient(apps)
	env.SetLocalByteStreamClient(cacheEnv.GetByteStreamClient())
	env.SetLocalActionCacheClient(cacheEnv.GetActionCacheClient())
	setFetchFromRegistry(t, env, fetchFromRegistry)
	return env
}

func serveProxy(t *testing.T, env *testenv.TestEnv, proxy ofpb.OCIFetcherServer) ofpb.OCIFetcherClient {
	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	ofpb.RegisterOCIFetcherServer(grpcServer, proxy)
	go runFunc()
	t.Cleanup(func() { grpcServer.GracefulStop() })
	conn, err := testenv.LocalGRPCConn(t, context.Background(), lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return ofpb.NewOCIFetcherClient(conn)
}

func runCountingRegistry(t *testing.T, interceptor func(http.ResponseWriter, *http.Request) bool) (*testregistry.Registry, *testhttp.RequestCounter) {
	counter := testhttp.NewRequestCounter()
	reg := testregistry.Run(t, testregistry.Opts{
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

// pushImage pushes an image and returns its name, its only layer's digest
// ref, and that layer's compressed bytes.
func pushImage(t *testing.T, reg *testregistry.Registry) (imageName, layerRef string, layer []byte) {
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	layers, err := img.Layers()
	require.NoError(t, err)
	require.Len(t, layers, 1)
	digest, err := layers[0].Digest()
	require.NoError(t, err)
	rc, err := layers[0].Compressed()
	require.NoError(t, err)
	defer rc.Close()
	layer, err = io.ReadAll(rc)
	require.NoError(t, err)
	return imageName, reg.ImageAddress("test-image@" + digest.String()), layer
}

func readBlob(t *testing.T, client ofpb.OCIFetcherClient, req *ofpb.FetchBlobRequest) ([]byte, error) {
	stream, err := client.FetchBlob(context.Background(), req)
	require.NoError(t, err)
	var data []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return data, nil
		}
		if err != nil {
			return data, err
		}
		data = append(data, resp.GetData()...)
	}
}

func fallbackCount(t *testing.T, method, ref string, err error) float64 {
	return testutil.ToFloat64(metrics.OCIFetcherProxyFallbackCount.With(prometheus.Labels{
		metrics.OCIFetcherMethodLabel:    method,
		metrics.GroupID:                  interfaces.AuthAnonymousUser,
		metrics.ImageFetchRegistryLabel:  registryLabel(ref),
		metrics.StatusHumanReadableLabel: status.MetricsLabel(err),
	}))
}

func TestFetchFromRegistry(t *testing.T) {
	reg, counter := runCountingRegistry(t, nil)
	imageName, layerRef, want := pushImage(t, reg)
	counter.Reset()
	apps := &fakeAppsFetcher{}
	proxy := newRegistryFetchingProxy(t, apps, true)
	ctx := context.Background()

	for range 2 {
		resp, err := proxy.FetchManifest(ctx, &ofpb.FetchManifestRequest{Ref: imageName})
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetManifest())

		_, err = proxy.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{Ref: layerRef})
		require.NoError(t, err)

		got, err := readBlob(t, proxy, &ofpb.FetchBlobRequest{Ref: layerRef})
		require.NoError(t, err)
		require.Equal(t, want, got)
	}

	require.Zero(t, apps.calls.Load(), "the proxy shouldn't call the apps")
	// The second round is served from the proxy's cache.
	_, digest, _ := strings.Cut(layerRef, "@")
	snapshot := counter.Snapshot()
	require.Equal(t, 1, snapshot[http.MethodGet+" /v2/test-image/manifests/latest"], "requests: %v", snapshot)
	require.Equal(t, 1, snapshot[http.MethodGet+" /v2/test-image/blobs/"+digest], "requests: %v", snapshot)
}

func TestFetchFromRegistryFallsBackToAppsWhenRejected(t *testing.T) {
	// After the image is pushed, the registry rejects requests, as if it only
	// accepted requests from the apps.
	var reject atomic.Bool
	reg, _ := runCountingRegistry(t, func(w http.ResponseWriter, r *http.Request) bool {
		if reject.Load() {
			w.WriteHeader(http.StatusForbidden)
			return false
		}
		return true
	})
	imageName, layerRef, want := pushImage(t, reg)
	reject.Store(true)
	manifest := []byte(`{"from": "the apps"}`)
	apps := &fakeAppsFetcher{manifest: manifest, blob: want}
	proxy := newRegistryFetchingProxy(t, apps, true)
	ctx := context.Background()
	rejected := status.PermissionDeniedError("")
	manifestFallbacks := fallbackCount(t, "FetchManifest", imageName, rejected)
	blobFallbacks := fallbackCount(t, "FetchBlob", layerRef, rejected)

	resp, err := proxy.FetchManifest(ctx, &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, manifest, resp.GetManifest())

	got, err := readBlob(t, proxy, &ofpb.FetchBlobRequest{Ref: layerRef})
	require.NoError(t, err)
	require.Equal(t, want, got)

	require.NotZero(t, apps.calls.Load())
	require.Equal(t, manifestFallbacks+1, fallbackCount(t, "FetchManifest", imageName, rejected))
	require.Equal(t, blobFallbacks+1, fallbackCount(t, "FetchBlob", layerRef, rejected))
}

func TestFetchFromRegistryDoesNotFallBackOnNotFound(t *testing.T) {
	reg, _ := runCountingRegistry(t, nil)
	apps := &fakeAppsFetcher{}
	proxy := newRegistryFetchingProxy(t, apps, true)

	_, err := proxy.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: reg.ImageAddress("missing:latest")})
	require.True(t, status.IsNotFoundError(err), "unexpected error: %v", err)
	require.Zero(t, apps.calls.Load())
}

func TestFetchFromRegistryExperimentOff(t *testing.T) {
	reg, counter := runCountingRegistry(t, nil)
	imageName, _, _ := pushImage(t, reg)
	counter.Reset()
	apps := &fakeAppsFetcher{manifest: []byte("from the apps")}
	proxy := newRegistryFetchingProxy(t, apps, false)

	resp, err := proxy.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName})
	require.NoError(t, err)
	require.Equal(t, []byte("from the apps"), resp.GetManifest())
	require.Equal(t, int32(1), apps.calls.Load())
	require.Empty(t, counter.Snapshot())
}

func TestFetchFromRegistryBypassRegistryGoesToApps(t *testing.T) {
	reg, counter := runCountingRegistry(t, nil)
	imageName, _, _ := pushImage(t, reg)
	counter.Reset()
	apps := &fakeAppsFetcher{manifest: []byte("from the apps")}
	proxy := newRegistryFetchingProxy(t, apps, true)

	resp, err := proxy.FetchManifest(context.Background(), &ofpb.FetchManifestRequest{Ref: imageName, BypassRegistry: true})
	require.NoError(t, err)
	require.Equal(t, []byte("from the apps"), resp.GetManifest())
	require.Equal(t, int32(1), apps.calls.Load())
	require.Empty(t, counter.Snapshot())
}

// partialBlobFetcher sends part of a blob, then fails with an error that
// would otherwise cause a fallback to the apps.
type partialBlobFetcher struct {
	ofpb.UnimplementedOCIFetcherServer
}

func (partialBlobFetcher) FetchBlob(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	if err := stream.Send(&ofpb.FetchBlobResponse{Data: []byte("partial")}); err != nil {
		return err
	}
	return status.UnavailableError("registry connection reset")
}

func TestFetchFromRegistryDoesNotFallBackAfterSendingData(t *testing.T) {
	apps := &fakeAppsFetcher{blob: []byte("from the apps")}
	env := newProxyEnv(t, apps, true)
	proxy, err := New(env)
	require.NoError(t, err)
	proxy.registryFetcher = partialBlobFetcher{}
	client := serveProxy(t, env, proxy)

	got, err := readBlob(t, client, &ofpb.FetchBlobRequest{Ref: "localhost/repo@sha256:" + strings.Repeat("a", 64)})
	require.True(t, status.IsUnavailableError(err), "unexpected error: %v", err)
	require.Equal(t, []byte("partial"), got)
	require.Zero(t, apps.calls.Load())
}

func TestFetchFromRegistryFallsBackWhenBlobBodyFails(t *testing.T) {
	// After the image is pushed, the registry drops the connection after
	// sending the headers for a blob, before any of its bytes.
	var dropBlobs atomic.Bool
	reg, _ := runCountingRegistry(t, func(w http.ResponseWriter, r *http.Request) bool {
		if dropBlobs.Load() && r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/blobs/") {
			// The server closes the connection after the headers, since
			// the body is shorter than the declared length.
			w.Header().Set("Content-Length", "1000")
			w.WriteHeader(http.StatusOK)
			return false
		}
		return true
	})
	_, layerRef, want := pushImage(t, reg)
	dropBlobs.Store(true)
	apps := &fakeAppsFetcher{blob: want}
	proxy := newRegistryFetchingProxy(t, apps, true)
	// The fetcher reports failing to read the blob as Internal.
	readFailed := status.InternalError("")
	fallbacks := fallbackCount(t, "FetchBlob", layerRef, readFailed)

	got, err := readBlob(t, proxy, &ofpb.FetchBlobRequest{Ref: layerRef})
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.NotZero(t, apps.calls.Load())
	require.Equal(t, fallbacks+1, fallbackCount(t, "FetchBlob", layerRef, readFailed))
}
