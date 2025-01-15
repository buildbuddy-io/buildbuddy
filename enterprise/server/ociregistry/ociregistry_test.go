package ociregistry_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/ociregistry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
)

type requestCounter struct {
	count int32
	mu    sync.Mutex
}

func (c *requestCounter) Inc() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count++
}

func withCounter(counter *requestCounter, handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counter.Inc()
		handler.ServeHTTP(w, r)
	})
}

func runMirrorRegistry(t *testing.T, env environment.Env, counter *requestCounter) string {
	t.Helper()
	ocireg, err := ociregistry.New(env)
	require.Nil(t, err)
	port := testport.FindFree(t)

	mux := http.NewServeMux()
	mux.Handle("/", ocireg)

	listenAddr := fmt.Sprintf("localhost:%d", port)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counter.Inc()
		ocireg.ServeHTTP(w, r)
	})}
	lis, err := net.Listen("tcp", listenAddr)
	require.NoError(t, err)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Shutdown(context.TODO())
	})
	return listenAddr
}

func TestResolve(t *testing.T) {
	te := testenv.GetTestEnv(t)

	testregCounter := &requestCounter{}
	testreg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			testregCounter.Inc()
			return true
		},
	})
	imageName, randomImage := testreg.PushRandomImage(t)

	mirrorCounter := requestCounter{}
	mirrorAddr := runMirrorRegistry(t, te, &mirrorCounter)

	flags.Set(t, "executor.container_registry_mirrors", []oci.MirrorConfig{{
		OriginalURL: "http://" + testreg.Address(),
		MirrorURL:   "http://" + mirrorAddr,
	}})

	resolvedImage, err := oci.Resolve(
		context.Background(),
		imageName,
		&rgpb.Platform{
			Arch: runtime.GOARCH,
			Os:   runtime.GOOS,
		},
		oci.Credentials{})
	require.NoError(t, err)
	assertSameImages(t, randomImage, resolvedImage)
	// One request to the mirror can generate multiple requests to the test registry.
	// So at least make sure the mirror received some requests, and that the test registry
	// received at least as many requests.
	assert.Greater(t, mirrorCounter.count, int32(0))
	assert.GreaterOrEqual(t, testregCounter.count, mirrorCounter.count)
}

func assertSameImages(t *testing.T, original, resolved v1.Image) {
	originalImageDigest, err := original.Digest()
	require.NoError(t, err)

	resolvedImageDigest, err := resolved.Digest()
	require.NoError(t, err)

	assert.Equal(t, originalImageDigest, resolvedImageDigest)

	originalLayers, err := original.Layers()
	require.NoError(t, err)

	for _, originalLayer := range originalLayers {
		originalDigest, err := originalLayer.Digest()
		require.NoError(t, err)
		resolvedLayer, err := resolved.LayerByDigest(originalDigest)
		require.NoError(t, err)

		originalMediaType, err := originalLayer.MediaType()
		require.NoError(t, err)
		resolvedMediaType, err := resolvedLayer.MediaType()
		require.NoError(t, err)
		assert.Equal(t, originalMediaType, resolvedMediaType)

		originalSize, err := originalLayer.Size()
		require.NoError(t, err)
		resolvedSize, err := resolvedLayer.Size()
		require.NoError(t, err)
		assert.Equal(t, originalSize, resolvedSize)

		originalCompressed, err := originalLayer.Compressed()
		require.NoError(t, err)
		originalBytes, err := io.ReadAll(originalCompressed)
		require.NoError(t, err)
		resolvedCompressed, err := resolvedLayer.Compressed()
		require.NoError(t, err)
		resolvedBytes, err := io.ReadAll(resolvedCompressed)
		require.NoError(t, err)
		assert.Equal(t, originalBytes, resolvedBytes)

		originalDiffID, err := originalLayer.DiffID()
		require.NoError(t, err)
		resolvedDiffID, err := resolvedLayer.DiffID()
		require.NoError(t, err)
		assert.Equal(t, originalDiffID, resolvedDiffID)

		originalUncompressed, err := originalLayer.Uncompressed()
		require.NoError(t, err)
		originalUncompressedBytes, err := io.ReadAll(originalUncompressed)
		require.NoError(t, err)
		resolvedUncompressed, err := resolvedLayer.Uncompressed()
		require.NoError(t, err)
		resolvedUncompressedBytes, err := io.ReadAll(resolvedUncompressed)
		require.NoError(t, err)
		assert.Equal(t, originalUncompressedBytes, resolvedUncompressedBytes)
	}

	resolvedLayers, err := resolved.Layers()
	require.NoError(t, err)
	assert.Equal(t, len(originalLayers), len(resolvedLayers))
}
