package oci_test

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenviron"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

func TestCredentialsFromProto(t *testing.T) {
	creds, err := oci.CredentialsFromProto(&rgpb.Credentials{
		Username: "",
		Password: "",
	})
	require.NoError(t, err)
	assert.True(t, creds.IsEmpty())

	_, err = oci.CredentialsFromProto(&rgpb.Credentials{
		Username: "foo",
		Password: "",
	})
	require.Error(t, err)

	_, err = oci.CredentialsFromProto(&rgpb.Credentials{
		Username: "",
		Password: "bar",
	})
	require.Error(t, err)

	creds, err = oci.CredentialsFromProto(&rgpb.Credentials{
		Username: "foo",
		Password: "bar",
	})
	require.NoError(t, err)
	assert.False(t, creds.IsEmpty())
	assert.Equal(t, "foo:bar", creds.String())
}

func TestCredentialsFromProperties(t *testing.T) {
	registries := []oci.Registry{
		{
			Hostnames: []string{"gcr.io", "us.gcr.io", "eu.gcr.io", "asia.gcr.io", "marketplace.gcr.io"},
			Username:  "gcruser",
			Password:  "gcrpass",
		},
		{
			Hostnames: []string{"docker.io"},
			Username:  "dockeruser",
			Password:  "dockerpass",
		},
	}
	flags.Set(t, "executor.container_registries", registries)

	props := &platform.Properties{}
	c, err := oci.CredentialsFromProperties(props)
	require.NoError(t, err)
	assert.True(t, c.IsEmpty())

	props = &platform.Properties{
		ContainerImage:            "missing-password.io",
		ContainerRegistryUsername: "username",
		ContainerRegistryPassword: "",
	}
	_, err = oci.CredentialsFromProperties(props)
	assert.True(t, status.IsInvalidArgumentError(err))

	props = &platform.Properties{
		ContainerImage:            "missing-username.io",
		ContainerRegistryUsername: "",
		ContainerRegistryPassword: "password",
	}
	_, err = oci.CredentialsFromProperties(props)
	assert.True(t, status.IsInvalidArgumentError(err))

	for _, testCase := range []struct {
		imageRef            string
		expectedCredentials oci.Credentials
	}{
		// Creds shouldn't be returned if there's no container image requested
		{"", oci.Credentials{}},
		// Creds shouldn't be returned if the registry is unrecognized
		{"unrecognized-registry.io/foo/bar", oci.Credentials{}},
		{"unrecognized-registry.io/foo/bar:latest", oci.Credentials{}},
		{"unrecognized-registry.io/foo/bar@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", oci.Credentials{}},
		// Images with no domain should get defaulted to docker.io (Docker and
		// other tools like `skopeo` assume docker.io as the default registry)
		{"alpine", creds("dockeruser", "dockerpass")},
		{"alpine:latest", creds("dockeruser", "dockerpass")},
		{"alpine@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", creds("dockeruser", "dockerpass")},
		// docker.io supports both `/image` and `/library/image` -- make sure we
		// handle both
		{"docker.io/alpine", creds("dockeruser", "dockerpass")},
		{"docker.io/alpine:latest", creds("dockeruser", "dockerpass")},
		{"docker.io/alpine@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", creds("dockeruser", "dockerpass")},
		{"docker.io/library/alpine:latest", creds("dockeruser", "dockerpass")},
		{"docker.io/library/alpine@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", creds("dockeruser", "dockerpass")},
		// Non-docker registries should work as well
		{"gcr.io/foo/bar", creds("gcruser", "gcrpass")},
		{"gcr.io/foo/bar:latest", creds("gcruser", "gcrpass")},
		{"gcr.io/foo/bar@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", creds("gcruser", "gcrpass")},
		// Subdomains should work too
		{"marketplace.gcr.io/foo/bar", creds("gcruser", "gcrpass")},
		{"marketplace.gcr.io/foo/bar:latest", creds("gcruser", "gcrpass")},
		{"marketplace.gcr.io/foo/bar@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", creds("gcruser", "gcrpass")},
	} {
		props = &platform.Properties{ContainerImage: testCase.imageRef}

		creds, err := oci.CredentialsFromProperties(props)

		assert.NoError(t, err)
		assert.Equal(
			t, testCase.expectedCredentials, creds,
			"unexpected credentials for image ref %q: %q",
			testCase.imageRef, testCase.expectedCredentials,
		)
	}
}

func TestCredentialsFromProperties_DefaultKeychain(t *testing.T) {
	tmp := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, tmp, map[string]string{
		// Write a credential helper that always returns the creds
		// test-user:test-pass
		"bin/docker-credential-test": `#!/usr/bin/env sh
			if [ "$1" = "get" ]; then
				echo '{"ServerURL":"","Username":"test-user","Secret":"test-pass"}'
				exit 0
			else
				echo "Not implemented"
				exit 1
			fi
		`,
		// Set up docker-credential-test as the credential helper for
		// "testregistry.io". Note the "docker-credential-" prefix is omitted.
		".docker/config.json": `{"credHelpers": {"testregistry.io": "test"}}`,
	})
	testfs.MakeExecutable(t, tmp, "bin/docker-credential-test")
	// The default keychain reads config.json from the path specified in the
	// DOCKER_CONFIG environment variable, if set. Point that to our directory.
	testenviron.Set(t, "DOCKER_CONFIG", filepath.Join(tmp, ".docker"))
	// Add our directory to PATH so the default keychain can find our credential
	// helper binary.
	testenviron.Set(t, "PATH", filepath.Join(tmp, "bin")+":"+os.Getenv("PATH"))

	for _, test := range []struct {
		Name                   string
		DefaultKeychainEnabled bool
		ExpectedCredentials    oci.Credentials
	}{
		{
			Name:                   "invokes credential helper if default keychain enabled",
			DefaultKeychainEnabled: true,
			ExpectedCredentials:    oci.Credentials{Username: "test-user", Password: "test-pass"},
		},
		{
			Name:                   "returns empty credentials if default keychain disabled",
			DefaultKeychainEnabled: false,
			ExpectedCredentials:    oci.Credentials{},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			flags.Set(t, "executor.container_registry_default_keychain_enabled", test.DefaultKeychainEnabled)

			creds, err := oci.CredentialsFromProperties(&platform.Properties{
				ContainerImage: "testregistry.io/test-repo/test-img",
			})
			require.NoError(t, err)

			require.Equal(t, test.ExpectedCredentials, creds)
		})
	}

}

func creds(username, password string) oci.Credentials {
	return oci.Credentials{Username: username, Password: password}
}

func TestToProto(t *testing.T) {
	assert.True(t,
		proto.Equal(
			&rgpb.Credentials{Username: "foo", Password: "bar"},
			oci.Credentials{Username: "foo", Password: "bar"}.ToProto()))
}

func TestResolve(t *testing.T) {
	flags.Set(t, "http.client.allow_localhost", true)
	registry := testregistry.Run(t, testregistry.Opts{})
	imageName, _ := registry.PushRandomImage(t)
	_, err := oci.Resolve(
		context.Background(),
		imageName,
		&rgpb.Platform{
			Arch: runtime.GOARCH,
			Os:   runtime.GOOS,
		},
		oci.Credentials{})
	require.NoError(t, err)
}

func TestResolve_InvalidImage(t *testing.T) {
	_, err := oci.Resolve(
		context.Background(),
		":invalid",
		&rgpb.Platform{
			Arch: runtime.GOARCH,
			Os:   runtime.GOOS,
		},
		oci.Credentials{})
	assert.True(t, status.IsInvalidArgumentError(err))
}

func TestResolve_Unauthorized(t *testing.T) {
	flags.Set(t, "http.client.allow_localhost", true)
	registry := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == "GET" {
				matches, err := regexp.MatchString("/v2/.*/manifests/.*", r.URL.Path)
				require.NoError(t, err)
				if matches {
					w.WriteHeader(401)
					return false
				}
			}
			return true
		},
	})

	imageName, _ := registry.PushRandomImage(t)
	_, err := oci.Resolve(
		context.Background(),
		imageName,
		&rgpb.Platform{
			Arch: runtime.GOARCH,
			Os:   runtime.GOOS,
		},
		oci.Credentials{})
	require.True(t, status.IsPermissionDeniedError(err))
}

func TestResolve_Arm64VariantIsOptional(t *testing.T) {
	for _, test := range []struct {
		name     string
		platform v1.Platform
	}{
		{name: "linux/arm64/v8", platform: v1.Platform{Architecture: "arm64", OS: "linux", Variant: "v8"}},
		{name: "linux/arm64", platform: v1.Platform{Architecture: "arm64", OS: "linux"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			flags.Set(t, "http.client.allow_localhost", true)
			ctx := context.Background()

			registry := testregistry.Run(t, testregistry.Opts{})

			img, err := crane.Image(map[string][]byte{
				"/variant.txt": []byte(test.platform.Variant)},
			)
			require.NoError(t, err)

			_ = registry.Push(t, img, test.platform.Architecture+test.platform.Variant)

			index := mutate.AppendManifests(empty.Index, mutate.IndexAddendum{
				Add: img,
				Descriptor: v1.Descriptor{
					Platform: &test.platform,
				},
			})

			ref := registry.PushIndex(t, index, "test-multiplatform-image")

			pulledImg, err := oci.Resolve(ctx, ref, &rgpb.Platform{
				Arch: "arm64",
				Os:   "linux",
			}, oci.Credentials{})
			require.NoError(t, err)
			layers, err := pulledImg.Layers()
			require.NoError(t, err)
			contents := layerContents(t, layers[0])
			require.Equal(t, map[string]string{
				"/variant.txt": test.platform.Variant,
			}, contents)
		})
	}
}

func layerContents(t *testing.T, layer v1.Layer) map[string]string {
	rc, err := layer.Uncompressed()
	require.NoError(t, err)
	defer rc.Close()
	tr := tar.NewReader(rc)
	contents := map[string]string{}
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if header.Typeflag == tar.TypeReg {
			var buf bytes.Buffer
			_, err := io.Copy(&buf, tr)
			require.NoError(t, err)
			contents[header.Name] = buf.String()
		}
	}
	return contents
}

func TestResolve_FallsBackToOriginalWhenMirrorFails(t *testing.T) {
	flags.Set(t, "http.client.allow_localhost", true)
	// Track requests to original and mirror registries.
	var originalReqCount, mirrorReqCount atomic.Int32

	// Original registry serves the image.
	originalRegistry := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == "GET" {
				if matched, _ := regexp.MatchString("/v2/.*/manifests/.*", r.URL.Path); matched {
					originalReqCount.Add(1)
				}
			}
			return true
		},
	})
	imageName, image := originalRegistry.PushRandomImage(t)
	imageDigest, err := image.Digest()
	require.NoError(t, err)

	// Mirror registry does not have the image and returns 404.
	mirrorRegistry := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == "GET" {
				if matched, _ := regexp.MatchString("/v2/.*/manifests/.*", r.URL.Path); matched {
					mirrorReqCount.Add(1)
					w.WriteHeader(http.StatusNotFound)
					return false
				}
			}
			return true
		},
	})

	// Configure the resolver to use the mirror as a mirror for the original registry.
	flags.Set(t, "executor.container_registry_mirrors", []oci.MirrorConfig{
		{
			OriginalURL: "http://" + originalRegistry.Address(),
			MirrorURL:   "http://" + mirrorRegistry.Address(),
		},
	})

	// Resolve the image, which should fall back to the original after mirror fails.
	img, err := oci.Resolve(
		context.Background(),
		imageName,
		&rgpb.Platform{
			Arch: runtime.GOARCH,
			Os:   runtime.GOOS,
		},
		oci.Credentials{},
	)
	require.NoError(t, err)

	// Verify the image digest matches the original.
	digest, err := img.Digest()
	require.NoError(t, err)
	assert.Equal(t, imageDigest.String(), digest.String())

	// Ensure the mirror was attempted first, then the original.
	assert.Equal(t, int32(1), mirrorReqCount.Load(), "mirror should have been queried once")
	assert.Equal(t, int32(1), originalReqCount.Load(), "original registry should have been queried after mirror failed")
}
