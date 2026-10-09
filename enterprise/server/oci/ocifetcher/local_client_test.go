package ocifetcher_test

import (
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	ctr "github.com/google/go-containerregistry/pkg/v1"
	"github.com/stretchr/testify/require"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func runRegistry(t *testing.T) (*testregistry.Registry, *testhttp.RequestCounter) {
	counter := testhttp.NewRequestCounter()
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	return reg, counter
}

func cacheClients(t *testing.T) (bspb.ByteStreamClient, repb.ActionCacheClient) {
	te := testenv.GetTestEnv(t)
	enterprise_testenv.AddClientIdentity(t, te, interfaces.ClientIdentityApp)
	_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, localGRPClis)
	go runServer()
	return te.GetByteStreamClient(), te.GetActionCacheClient()
}

func newLocalClient(t *testing.T, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient) ofpb.OCIFetcherClient {
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.0/8", "::1/128"})
	server, err := ocifetcher.NewLocalServer(bsClient, acClient)
	require.NoError(t, err)
	return ocifetcher.NewLocalClient(server)
}

// pushLargeImage pushes an image with a single layer that's big enough to be
// sent in several FetchBlob responses. It returns the image name, the layer's
// digest ref, and the layer's compressed bytes.
func pushLargeImage(t *testing.T, reg *testregistry.Registry) (imageName, layerRef string, layerData []byte) {
	contents := make([]byte, 4<<20)
	_, err := rand.Read(contents)
	require.NoError(t, err)
	imageName, img := reg.PushNamedImageWithFiles(t, "large", map[string][]byte{"/data": contents}, nil)
	layers, err := img.Layers()
	require.NoError(t, err)
	require.Len(t, layers, 1)
	digest, err := layers[0].Digest()
	require.NoError(t, err)
	return imageName, reg.ImageAddress("large@" + digest.String()), compressedLayerData(t, layers[0])
}

func compressedLayerData(t *testing.T, layer ctr.Layer) []byte {
	rc, err := layer.Compressed()
	require.NoError(t, err)
	defer rc.Close()
	b, err := io.ReadAll(rc)
	require.NoError(t, err)
	return b
}

func fetchBlob(ctx context.Context, client ofpb.OCIFetcherClient, ref string) ([]byte, error) {
	return fetchBlobWithRequest(ctx, client, &ofpb.FetchBlobRequest{Ref: ref})
}

func fetchBlobWithRequest(ctx context.Context, client ofpb.OCIFetcherClient, req *ofpb.FetchBlobRequest) ([]byte, error) {
	stream, err := client.FetchBlob(ctx, req)
	if err != nil {
		return nil, err
	}
	var data []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return data, nil
		}
		if err != nil {
			return nil, err
		}
		data = append(data, resp.GetData()...)
	}
}

// blobGETs returns how many times the registry served the blob at layerRef.
func blobGETs(t *testing.T, counter *testhttp.RequestCounter, layerRef string) int {
	_, digest, ok := strings.Cut(layerRef, "@")
	require.True(t, ok)
	return counter.Snapshot()[http.MethodGet+" /v2/large/blobs/"+digest]
}

func TestLocalClientWithoutCache(t *testing.T) {
	reg, counter := runRegistry(t)
	imageName, layerRef, want := pushLargeImage(t, reg)
	counter.Reset()
	client := newLocalClient(t, nil, nil)
	ctx := context.Background()

	for range 2 {
		resp, err := client.FetchManifest(ctx, &ofpb.FetchManifestRequest{Ref: imageName})
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetManifest())

		got, err := fetchBlob(ctx, client, layerRef)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	// Without a cache, every request goes to the registry, and fetching the
	// manifest doesn't need a separate HEAD to prove access.
	snapshot := counter.Snapshot()
	require.Equal(t, 2, snapshot[http.MethodGet+" /v2/large/manifests/latest"], "requests: %v", snapshot)
	require.Zero(t, snapshot[http.MethodHead+" /v2/large/manifests/latest"], "requests: %v", snapshot)
	require.Equal(t, 2, blobGETs(t, counter, layerRef))
}

func TestLocalClientWithCache(t *testing.T) {
	reg, counter := runRegistry(t)
	_, layerRef, want := pushLargeImage(t, reg)
	bsClient, acClient := cacheClients(t)
	client := newLocalClient(t, bsClient, acClient)
	ctx := context.Background()

	for range 2 {
		got, err := fetchBlob(ctx, client, layerRef)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	require.Equal(t, 1, blobGETs(t, counter, layerRef))
}

func TestLocalClientWithoutCacheContext(t *testing.T) {
	reg, counter := runRegistry(t)
	_, layerRef, want := pushLargeImage(t, reg)
	bsClient, acClient := cacheClients(t)
	client := newLocalClient(t, bsClient, acClient)

	// Requests that skip the cache neither read nor write it.
	for range 2 {
		got, err := fetchBlob(ocifetcher.WithoutCache(context.Background()), client, layerRef)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	require.Equal(t, 2, blobGETs(t, counter, layerRef))

	// So a cached request still has to go to the registry once.
	for range 2 {
		got, err := fetchBlob(context.Background(), client, layerRef)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	require.Equal(t, 3, blobGETs(t, counter, layerRef))
}

func TestLocalServerTrustsBypassRegistry(t *testing.T) {
	reg, counter := runRegistry(t)
	_, layerRef, want := pushLargeImage(t, reg)
	bsClient, acClient := cacheClients(t)
	client := newLocalClient(t, bsClient, acClient)
	ctx := context.Background()

	got, err := fetchBlob(ctx, client, layerRef)
	require.NoError(t, err)
	require.Equal(t, want, got)
	counter.Reset()

	// The context has no server admin claims, but the local server trusts
	// bypass_registry because the execution server already checked it.
	data, err := fetchBlobWithRequest(ctx, client, &ofpb.FetchBlobRequest{Ref: layerRef, BypassRegistry: true})
	require.NoError(t, err)
	require.Equal(t, want, data)
	require.Empty(t, counter.Snapshot())
}

func TestLocalServerIgnoresBypassRegistryWithoutCache(t *testing.T) {
	reg, _ := runRegistry(t)
	_, layerRef, want := pushLargeImage(t, reg)
	client := newLocalClient(t, nil, nil)

	data, err := fetchBlobWithRequest(context.Background(), client, &ofpb.FetchBlobRequest{Ref: layerRef, BypassRegistry: true})
	require.NoError(t, err)
	require.Equal(t, want, data)
}

func TestLocalClientFetchBlobCanceled(t *testing.T) {
	reg, _ := runRegistry(t)
	_, layerRef, _ := pushLargeImage(t, reg)
	client := newLocalClient(t, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.FetchBlob(ctx, &ofpb.FetchBlobRequest{Ref: layerRef})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)

	cancel()
	for {
		_, err = stream.Recv()
		if err != nil {
			break
		}
	}
	require.NotEqual(t, io.EOF, err)
	require.True(t, status.IsCanceledError(err) || status.IsUnavailableError(err), "unexpected error: %v", err)
}

func TestNewLocalServerRequiresBothCacheClients(t *testing.T) {
	bsClient, _ := cacheClients(t)
	_, err := ocifetcher.NewLocalServer(bsClient, nil)
	require.True(t, status.IsFailedPreconditionError(err), "unexpected error: %v", err)
}

func TestLocalClientUsesBlobMetadataFromRequest(t *testing.T) {
	reg, counter := runRegistry(t)
	_, layerRef, want := pushLargeImage(t, reg)
	counter.Reset()
	bsClient, acClient := cacheClients(t)
	client := newLocalClient(t, bsClient, acClient)
	_, digest, _ := strings.Cut(layerRef, "@")

	req := &ofpb.FetchBlobRequest{
		Ref:       layerRef,
		Size:      new(int64(len(want))),
		MediaType: new("application/vnd.oci.image.layer.v1.tar+gzip"),
	}
	for range 2 {
		got, err := fetchBlobWithRequest(context.Background(), client, req)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	// The size and media type came from the request, so there's no HEAD,
	// and the blob was still cached.
	snapshot := counter.Snapshot()
	require.Zero(t, snapshot[http.MethodHead+" /v2/large/blobs/"+digest], "requests: %v", snapshot)
	require.Equal(t, 1, blobGETs(t, counter, layerRef))
}

func TestRemoteServerIgnoresBlobMetadataFromRequest(t *testing.T) {
	reg, counter := runRegistry(t)
	_, layerRef, want := pushLargeImage(t, reg)
	counter.Reset()
	bsClient, acClient := cacheClients(t)
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.0/8", "::1/128"})
	server, err := ocifetcher.NewServer(bsClient, acClient)
	require.NoError(t, err)
	client := ocifetcher.NewLocalClient(server)
	_, digest, _ := strings.Cut(layerRef, "@")

	// A remote server must not trust caller-supplied metadata, so it makes
	// its own HEAD request, and the wrong size doesn't break caching.
	req := &ofpb.FetchBlobRequest{
		Ref:       layerRef,
		Size:      new(int64(1)),
		MediaType: new("text/plain"),
	}
	for range 2 {
		got, err := fetchBlobWithRequest(context.Background(), client, req)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	snapshot := counter.Snapshot()
	require.Equal(t, 1, snapshot[http.MethodHead+" /v2/large/blobs/"+digest], "requests: %v", snapshot)
	require.Equal(t, 1, blobGETs(t, counter, layerRef))
}

func TestLocalServerBypassRegistryFallsBackToRegistry(t *testing.T) {
	reg, _ := runRegistry(t)
	imageName, layerRef, want := pushLargeImage(t, reg)
	bsClient, acClient := cacheClients(t)
	client := newLocalClient(t, bsClient, acClient)
	ctx := context.Background()

	// Nothing is cached, and the manifest ref is a tag, but an in-process
	// server still resolves and fetches from the registry.
	resp, err := client.FetchManifest(ctx, &ofpb.FetchManifestRequest{Ref: imageName, BypassRegistry: true})
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetManifest())

	got, err := fetchBlobWithRequest(ctx, client, &ofpb.FetchBlobRequest{Ref: layerRef, BypassRegistry: true})
	require.NoError(t, err)
	require.Equal(t, want, got)

	_, err = client.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{Ref: layerRef, BypassRegistry: true})
	require.NoError(t, err)
}

// blockingFetchBlobServer sends one response from FetchBlob, then blocks
// until released, ignoring cancellation.
type blockingFetchBlobServer struct {
	ofpb.UnimplementedOCIFetcherServer
	release chan struct{}
}

func (s *blockingFetchBlobServer) FetchBlob(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	if err := stream.Send(&ofpb.FetchBlobResponse{Data: []byte("x")}); err != nil {
		return err
	}
	<-s.release
	return nil
}

func TestLocalClientRecvReturnsWhenCanceled(t *testing.T) {
	server := &blockingFetchBlobServer{release: make(chan struct{})}
	defer close(server.release)
	client := ocifetcher.NewLocalClient(server)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.FetchBlob(ctx, &ofpb.FetchBlobRequest{})
	require.NoError(t, err)
	resp, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []byte("x"), resp.GetData())

	cancel()
	_, err = stream.Recv()
	require.True(t, status.IsCanceledError(err), "unexpected error: %v", err)
}

func TestLocalClientRecvReturnsEOFAfterServerReturns(t *testing.T) {
	server := &blockingFetchBlobServer{release: make(chan struct{})}
	close(server.release)
	client := ocifetcher.NewLocalClient(server)

	for range 100 {
		stream, err := client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{})
		require.NoError(t, err)
		_, err = stream.Recv()
		require.NoError(t, err)
		_, err = stream.Recv()
		require.Equal(t, io.EOF, err)
	}
}
