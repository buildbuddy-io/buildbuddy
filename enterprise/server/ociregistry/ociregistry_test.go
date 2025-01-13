package ociregistry_test

import (
	"context"
	"fmt"
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
	randomImageDigest, err := randomImage.Digest()
	require.NoError(t, err)

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

	resolvedImageDigest, err := resolvedImage.Digest()
	require.NoError(t, err)
	assert.Equal(t, randomImageDigest, resolvedImageDigest)
}
