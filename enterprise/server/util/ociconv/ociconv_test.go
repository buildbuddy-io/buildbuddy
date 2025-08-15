package ociconv_test

import (
	"archive/tar"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ociconv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOciconv(t *testing.T) {
	ctx := context.Background()
	root := testfs.MakeTempDir(t)
	os.Setenv("REGISTRY_AUTH_FILE", "_null")

	for _, img := range []string{
		// TODO: use testregistry instead of these images,
		// and remove network dependency.
		"gcr.io/flame-public/test-alpine@sha256:6457d53fb065d6f250e1504b9bc42d5b6c65941d57532c072d929dd0628977d0",
		"mirror.gcr.io/ubuntu:22.04",
	} {
		t.Run("image="+img, func(t *testing.T) {
			te := testenv.GetTestEnv(t)
			resolver, err := oci.NewResolver(te)
			require.NoError(t, err)
			require.NotNil(t, resolver)
			_, err = ociconv.CreateDiskImage(ctx, resolver, root, img, oci.Credentials{})
			require.NoError(t, err)
		})
	}
}

func TestOciconv_TestRegistry(t *testing.T) {
	te := testenv.GetTestEnv(t)
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})

	ctx := context.Background()
	root := testfs.MakeTempDir(t)

	reg := testregistry.Run(t, testregistry.Opts{})

	ref, img := reg.PushNamedImage(t, "ociconv-test-image:latest")

	resolver, err := oci.NewResolver(te)
	require.NoError(t, err)
	require.NotNil(t, resolver)

	path, err := ociconv.CreateDiskImage(ctx, resolver, root, ref, oci.Credentials{})
	require.NoError(t, err)

	fi, err := os.Stat(path)
	require.NoError(t, err)
	require.False(t, fi.IsDir())
	require.Greater(t, fi.Size(), int64(0))

	// Extract ext4 image and verify that all file paths from the image are present.
	outDir := testfs.MakeTempDir(t)
	err = ext4.ImageToDirectory(ctx, path, outDir, []string{"/"})
	require.NoError(t, err)


	rc := mutate.Extract(img)
	defer rc.Close()

	tr := tar.NewReader(rc)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if hdr == nil {
			continue
		}
		switch hdr.Typeflag {
		case tar.TypeReg, tar.TypeRegA, tar.TypeSymlink:
			name := strings.TrimPrefix(hdr.Name, "./")
			name = strings.TrimPrefix(name, "/")
			if name == "" {
				continue
			}
			_, err := os.Lstat(filepath.Join(outDir, name))
			require.NoErrorf(t, err, "expected path to exist in ext4 output: %q", name)
		}
	}
}

func TestOciconv_ChecksCredentials(t *testing.T) {
	te := testenv.GetTestEnv(t)
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})

	ctx := context.Background()
	root := testfs.MakeTempDir(t)
	os.Setenv("REGISTRY_AUTH_FILE", "_null")

	authEnabled := false
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			if !authEnabled {
				return true
			}
			user, pass, ok := r.BasicAuth()
			if !ok {
				// Respond with WWW-Authenticate header to trigger basic auth.
				w.Header().Set("WWW-Authenticate", "Basic")
				w.WriteHeader(http.StatusUnauthorized)
				return false
			}
			if user != "test" || pass != "test" {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return false
			}
			return true
		},
	})
	t.Cleanup(func() { reg.Shutdown(ctx) })

	// Bypass auth while pushing the image.
	authEnabled = false
	ref, _ := reg.PushNamedImage(t, "test-empty-image:latest")
	authEnabled = true

	resolver, err := oci.NewResolver(te)
	require.NoError(t, err)
	require.NotNil(t, resolver)
	// This should fail because the credentials are invalid.
	_, err = ociconv.CreateDiskImage(ctx, resolver, root, ref, oci.Credentials{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401 Unauthorized")

	// This should succeed because the credentials are valid.
	_, err = ociconv.CreateDiskImage(ctx, resolver, root, ref, oci.Credentials{
		Username: "test",
		Password: "test",
	})
	require.NoError(t, err)

	// Now that the image is cached, try pulling again with invalid credentials.
	// This should still fail.
	_, err = ociconv.CreateDiskImage(ctx, resolver, root, ref, oci.Credentials{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401 Unauthorized")

	// Try a successful pull again with valid credentials now that the image
	// is cached; this should succeed.
	_, err = ociconv.CreateDiskImage(ctx, resolver, root, ref, oci.Credentials{
		Username: "test",
		Password: "test",
	})
	require.NoError(t, err)
}
