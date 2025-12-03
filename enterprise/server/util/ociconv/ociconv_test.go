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

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ociconv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOciconv(t *testing.T) {
	te := testenv.GetTestEnv(t)
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})

	ctx := context.Background()
	root := testfs.MakeTempDir(t)

	reg := testregistry.Run(t, testregistry.Opts{})

	ref, img := reg.PushNamedImage(t, "ociconv-test-image:latest", nil)

	resolver, err := oci.NewResolver(te)
	require.NoError(t, err)
	require.NotNil(t, resolver)

	path, err := ociconv.CreateDiskImage(ctx, resolver, root, ref, oci.Credentials{})
	require.NoError(t, err)

	fi, err := os.Stat(path)
	require.NoError(t, err)
	require.False(t, fi.IsDir())
	require.Greater(t, fi.Size(), int64(0))

	// Extract ext4 image.
	outDir := testfs.MakeTempDir(t)
	err = ext4.ImageToDirectory(ctx, path, outDir, []string{"/"})
	require.NoError(t, err)

	// Build path->size map from the container image (regular files only).
	rc := mutate.Extract(img)
	defer rc.Close()
	tr := tar.NewReader(rc)
	imageFiles := make(map[string]int64)
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
		case tar.TypeReg, tar.TypeRegA:
			name := strings.TrimPrefix(hdr.Name, "./")
			name = strings.TrimPrefix(name, "/")
			if name == "" {
				continue
			}
			imageFiles[name] = hdr.Size
		}
	}

	// Build path->size map from the extracted ext4 directory (regular files only).
	extractedFiles := make(map[string]int64)
	err = filepath.Walk(outDir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			rel, err := filepath.Rel(outDir, p)
			if err != nil {
				return err
			}
			if rel == "." {
				return nil
			}
			extractedFiles[rel] = info.Size()
		}
		return nil
	})
	require.NoError(t, err)

	require.Empty(t, cmp.Diff(imageFiles, extractedFiles))
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
	ref, _ := reg.PushNamedImage(t, "test-empty-image:latest", nil)
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
