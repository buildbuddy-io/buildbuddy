package ociregistry_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
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

func runTestProxy(t *testing.T, env environment.Env) string {
	t.Helper()
	r, err := ociregistry.New(env)
	require.Nil(t, err)
	port := testport.FindFree(t)

	mux := http.NewServeMux()
	mux.Handle("/", r)

	listenAddr := fmt.Sprintf("localhost:%d", port)
	server := &http.Server{Handler: r}
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

	registry := testregistry.Run(t, testregistry.Opts{})
	imageName, randomImage := registry.PushRandomImage(t)
	proxyAddr := runTestProxy(t, te)

	flags.Set(t, "executor.container_registry_mirrors", []oci.MirrorConfig{{
		OriginalURL: "http://" + registry.Address(),
		MirrorURL:   "http://" + proxyAddr,
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
}

func assertSameImages(t *testing.T, original, resolved v1.Image) {
	originalImageDigest, err := original.Digest()
	require.NoError(t, err)

	resolvedImageDigest, err := resolved.Digest()
	require.NoError(t, err)

	assert.Equal(t, originalImageDigest, resolvedImageDigest)

	originalLayers, err := original.Layers()
	require.NoError(t, err)

	resolvedLayers, err := resolved.Layers()
	require.NoError(t, err)

	originalDigests := make(map[v1.Hash][]byte)
	for _, layer := range originalLayers {
		digest, err := layer.Digest()
		require.NoError(t, err)
		reader, err := layer.Compressed()
		require.NoError(t, err)
		compressedBytes, err := io.ReadAll(reader)
		require.NoError(t, err)
		originalDigests[digest] = compressedBytes
	}

	for _, layer := range resolvedLayers {
		digest, err := layer.Digest()
		require.NoError(t, err)
		originalBytes, ok := originalDigests[digest]
		assert.True(t, ok, "unexpected layer in resolved image: %s", digest)
		delete(originalDigests, digest)

		reader, err := layer.Compressed()
		require.NoError(t, err)
		compressedBytes, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, originalBytes, compressedBytes)
	}

	if len(originalDigests) > 0 {
		t.Errorf("%d layers from original image missing in resolved", len(originalDigests))
	}
}
