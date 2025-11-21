package oci_test

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/fetcher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/jonboulle/clockwork"
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
	t.Setenv("DOCKER_CONFIG", filepath.Join(tmp, ".docker"))
	// Add our directory to PATH so the default keychain can find our credential
	// helper binary.
	t.Setenv("PATH", filepath.Join(tmp, "bin")+":"+os.Getenv("PATH"))

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

func TestCredentialsToProto(t *testing.T) {
	assert.True(t,
		proto.Equal(
			&rgpb.Credentials{Username: "foo", Password: "bar"},
			oci.Credentials{Username: "foo", Password: "bar"}.ToProto()))
}

func newResolver(t *testing.T, te *testenv.TestEnv) *oci.Resolver {
	r, err := oci.NewResolver(te)
	require.NoError(t, err)
	return r
}

type resolveArgs struct {
	imageName   string
	platform    *rgpb.Platform
	credentials oci.Credentials
}

type resolveTestCase struct {
	name string

	imageName     string
	imageFiles    map[string][]byte
	imagePlatform v1.Platform

	args       resolveArgs
	checkError func(error) bool
	opts       testregistry.Opts
}

func TestResolve(t *testing.T) {
	for _, tc := range []resolveTestCase{
		{
			name: "resolving an existing image without credentials succeeds",

			imageName: "resolve_existing",
			imageFiles: map[string][]byte{
				"/name": []byte("resolving an existing image without credentials succeeds"),
			},
			imagePlatform: v1.Platform{
				Architecture: runtime.GOARCH,
				OS:           runtime.GOOS,
			},

			args: resolveArgs{
				imageName: "resolve_existing",
				platform: &rgpb.Platform{
					Arch: runtime.GOARCH,
					Os:   runtime.GOOS,
				},
			},
		},
		{
			name: "resolving an invalid image name fails with invalid argument error",

			imageName: "resolve_invalid",
			imageFiles: map[string][]byte{
				"/name": []byte("resolving an invalid image name fails with invalid argument error"),
			},
			imagePlatform: v1.Platform{
				Architecture: runtime.GOARCH,
				OS:           runtime.GOOS,
			},

			args: resolveArgs{
				imageName: ":invalid",
				platform: &rgpb.Platform{
					Arch: runtime.GOARCH,
					Os:   runtime.GOOS,
				},
			},
			checkError: status.IsInvalidArgumentError,
		},
		{
			name: "resolving an existing image without authorization fails",

			imageName: "resolve_unauthed",
			imageFiles: map[string][]byte{
				"/name": []byte("resolving an existing image without authorization fails"),
			},
			imagePlatform: v1.Platform{
				Architecture: runtime.GOARCH,
				OS:           runtime.GOOS,
			},

			args: resolveArgs{
				imageName: "resolve_unauthed",
				platform: &rgpb.Platform{
					Arch: runtime.GOARCH,
					Os:   runtime.GOOS,
				},
			},
			checkError: status.IsPermissionDeniedError,
			opts: testregistry.Opts{
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
			},
		},
		{
			name: "resolving a platform-specific image without including the variant succeeds",

			imageName: "resolve_platform_variant",
			imageFiles: map[string][]byte{
				"/name":    []byte("resolving a platform-specific image without including the variant succeeds"),
				"/variant": []byte("v8"),
			},
			imagePlatform: v1.Platform{
				Architecture: "arm64",
				OS:           "linux",
				Variant:      "v8",
			},

			args: resolveArgs{
				imageName: "resolve_platform_variant",
				platform: &rgpb.Platform{
					Arch: "arm64",
					Os:   "linux",
				},
			},
		},
	} {
		for _, useCachePercent := range []int{0, 100} {
			t.Run(tc.name+fmt.Sprintf("/use_cache_percent_%d", useCachePercent), func(t *testing.T) {
				te := testenv.GetTestEnv(t)
				require.NoError(t, fetcher.Register(te))
				flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})
				flags.Set(t, "executor.container_registry.use_cache_percent", useCachePercent)
				registry := testregistry.Run(t, tc.opts)
				_, pushedImage := registry.PushNamedImageWithFiles(t, tc.imageName+"_image", tc.imageFiles)

				index := mutate.AppendManifests(empty.Index, mutate.IndexAddendum{
					Add: pushedImage,
					Descriptor: v1.Descriptor{
						Platform: &tc.imagePlatform,
					},
				})
				registry.PushIndex(t, index, tc.imageName+"_index")

				for _, nameToResolve := range []string{tc.args.imageName + "_image", tc.args.imageName + "_index"} {
					pulledImage, err := newResolver(t, te).Resolve(
						context.Background(),
						registry.ImageAddress(nameToResolve),
						tc.args.platform,
						tc.args.credentials,
					)
					if tc.checkError != nil {
						require.True(t, tc.checkError(err))
						continue
					}
					require.NoError(t, err)
					layers, err := pulledImage.Layers()
					require.NoError(t, err)
					require.Equal(t, 1, len(layers))
					require.Empty(t, cmp.Diff(tc.imageFiles, layerFiles(t, layers[0])))
				}
			})
		}
	}
}

// TestResolve_Layers_DiffIDs tests for a specific regression that led to an incident:
//
// Layer.DiffID() returns the SHA256 of the uncompressed bytes for the layer.
// The go-containerregistry library can retrieve this information from the image's config file.
// If it is not present, however, it will fetch the entire uncompresed layer from the upstream
// server and compute the SHA256.
//
// This test ensures that calling Image.Layers() or Layer.DiffID() does not result in
// HTTP requests to the server.
func TestResolve_Layers_DiffIDs(t *testing.T) {
	for _, tc := range []resolveTestCase{
		{
			name: "resolving an existing image without credentials succeeds",

			imageName: "resolve_existing",
			imageFiles: map[string][]byte{
				"/name": []byte("resolving an existing image without credentials succeeds"),
			},
			imagePlatform: v1.Platform{
				Architecture: runtime.GOARCH,
				OS:           runtime.GOOS,
			},

			args: resolveArgs{
				imageName: "resolve_existing",
				platform: &rgpb.Platform{
					Arch: runtime.GOARCH,
					Os:   runtime.GOOS,
				},
			},
		},
		{
			name: "resolving a platform-specific image without including the variant succeeds",

			imageName: "resolve_platform_variant",
			imageFiles: map[string][]byte{
				"/name":    []byte("resolving a platform-specific image without including the variant succeeds"),
				"/variant": []byte("v8"),
			},
			imagePlatform: v1.Platform{
				Architecture: "arm64",
				OS:           "linux",
				Variant:      "v8",
			},

			args: resolveArgs{
				imageName: "resolve_platform_variant",
				platform: &rgpb.Platform{
					Arch: "arm64",
					Os:   "linux",
				},
			},
		},
	} {
		for _, useCachePercent := range []int{0, 100} {
			name := tc.name + "/use_cache_percent_" + strconv.Itoa(useCachePercent)
			t.Run(name, func(t *testing.T) {
				te := testenv.GetTestEnv(t)
				require.NoError(t, fetcher.Register(te))
				flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})
				flags.Set(t, "executor.container_registry.use_cache_percent", useCachePercent)
				counter := newRequestCounter()
				registry := testregistry.Run(t, testregistry.Opts{
					HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
						counter.Inc(r)
						return true
					},
				})
				_, pushedImage := registry.PushNamedImageWithMultipleLayers(t, tc.imageName+"_image")

				index := mutate.AppendManifests(empty.Index, mutate.IndexAddendum{
					Add: pushedImage,
					Descriptor: v1.Descriptor{
						Platform: &tc.imagePlatform,
					},
				})
				registry.PushIndex(t, index, tc.imageName+"_index")

				for _, nameToResolve := range []string{tc.args.imageName + "_image", tc.args.imageName + "_index"} {
					pulledImage, err := newResolver(t, te).Resolve(
						context.Background(),
						registry.ImageAddress(nameToResolve),
						tc.args.platform,
						tc.args.credentials,
					)
					require.NoError(t, err)

					counter.reset()

					layers, err := pulledImage.Layers()
					require.NoError(t, err)

					expected := map[string]int{}
					require.Empty(t, cmp.Diff(expected, counter.snapshot()))

					configDigest, err := pulledImage.ConfigName()
					require.NoError(t, err)
					expected = map[string]int{
						http.MethodGet + " /v2/" + nameToResolve + "/blobs/" + configDigest.String(): 1,
					}

					// To make the DiffID() request counts always be zero,
					// fetch the config file here. Otherwise the first
					// Layer.DiffID() call will make a request to fetch the config file.
					_, err = pulledImage.ConfigFile()
					require.NoError(t, err)
					require.Empty(t, cmp.Diff(expected, counter.snapshot()))

					for _, layer := range layers {
						_, err := layer.DiffID()
						require.NoError(t, err)
						require.Empty(t, cmp.Diff(expected, counter.snapshot()))
					}
				}
			})
		}
	}
}

func layerFiles(t *testing.T, layer v1.Layer) map[string][]byte {
	rc, err := layer.Uncompressed()
	require.NoError(t, err)
	defer func() { err := rc.Close(); require.NoError(t, err) }()
	tr := tar.NewReader(rc)
	contents := map[string][]byte{}
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if header.Typeflag == tar.TypeReg {
			var buf bytes.Buffer
			_, err := io.Copy(&buf, tr)
			require.NoError(t, err)
			contents[header.Name] = buf.Bytes()
		}
	}
	return contents
}

func TestResolve_FallsBackToOriginalWhenMirrorFails(t *testing.T) {
	te := testenv.GetTestEnv(t)
	require.NoError(t, fetcher.Register(te))
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})
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
	img, err := newResolver(t, te).Resolve(
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

func pushAndFetchRandomImage(t *testing.T, te *testenv.TestEnv, registry *testregistry.Registry) error {
	imageName, _ := registry.PushRandomImage(t)
	_, err := newResolver(t, te).Resolve(
		context.Background(),
		imageName,
		&rgpb.Platform{
			Arch: runtime.GOARCH,
			Os:   runtime.GOOS,
		},
		oci.Credentials{})
	return err
}

func TestAllowPrivateIPs(t *testing.T) {
	for _, tc := range []struct {
		name          string
		allowedIPs    []string
		expectError   bool
		errorContains string
	}{
		{
			name:          "private IPs not allowed",
			allowedIPs:    []string{},
			expectError:   true,
			errorContains: "not allowed",
		},
		{
			name:        "localhost allowed",
			allowedIPs:  []string{"127.0.0.1/32"},
			expectError: false,
		},
		{
			name:        "all IPv4 addresses allowed",
			allowedIPs:  []string{"0.0.0.0/0"},
			expectError: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := testenv.GetTestEnv(t)
			require.NoError(t, fetcher.Register(te))
			flags.Set(t, "http.client.allow_localhost", false)
			flags.Set(t, "executor.container_registry_allowed_private_ips", tc.allowedIPs)
			registry := testregistry.Run(t, testregistry.Opts{})
			err := pushAndFetchRandomImage(t, te, registry)
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tc.errorContains != "" {
				require.ErrorContains(t, err, tc.errorContains)
			}
		})
	}
}

// TestResolve_WithCache enumerates all the different combinations of read/write flags for manifests and layers,
// resolves images and indexes with those flags set, and validates the number of requests made to the upstream registry.
//
// Currently caching is not implemented end-to-end, so each Resolve(...) call will make the same number of upstream requests.
func TestResolve_WithCache(t *testing.T) {
	for _, tc := range []resolveTestCase{
		{
			name: "resolving an existing image without credentials succeeds",

			imageName: "resolve_existing",
			imageFiles: map[string][]byte{
				"/name": []byte("resolving an existing image without credentials succeeds"),
			},
			imagePlatform: v1.Platform{
				Architecture: runtime.GOARCH,
				OS:           runtime.GOOS,
			},

			args: resolveArgs{
				imageName: "resolve_existing",
				platform: &rgpb.Platform{
					Arch: runtime.GOARCH,
					Os:   runtime.GOOS,
				},
			},
		},
		{
			name: "resolving a platform-specific image without including the variant succeeds",

			imageName: "resolve_platform_variant",
			imageFiles: map[string][]byte{
				"/name":    []byte("resolving a platform-specific image without including the variant succeeds"),
				"/variant": []byte("v8"),
			},
			imagePlatform: v1.Platform{
				Architecture: "arm64",
				OS:           "linux",
				Variant:      "v8",
			},

			args: resolveArgs{
				imageName: "resolve_platform_variant",
				platform: &rgpb.Platform{
					Arch: "arm64",
					Os:   "linux",
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := setupTestEnvWithCache(t)
			flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})
			flags.Set(t, "executor.container_registry.use_cache_percent", 100)
			counter := newRequestCounter()
			registry := testregistry.Run(t, testregistry.Opts{
				HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
					counter.Inc(r)
					return true
				},
			})
			_, pushedImage := registry.PushNamedImageWithFiles(t, tc.imageName+"_image", tc.imageFiles)

			index := mutate.AppendManifests(empty.Index, mutate.IndexAddendum{
				Add: pushedImage,
				Descriptor: v1.Descriptor{
					Platform: &tc.imagePlatform,
				},
			})
			registry.PushIndex(t, index, tc.imageName+"_index")

			// Test with normal image manifest
			{
				imageAddress := registry.ImageAddress(tc.args.imageName + "_image")
				imageDigest, err := pushedImage.Digest()
				require.NoError(t, err)
				pushedLayers, err := pushedImage.Layers()
				require.NoError(t, err)
				require.Len(t, pushedLayers, 1)
				layerDigest, err := pushedLayers[0].Digest()
				require.NoError(t, err)

				// Initially, nothing is cached, and we expect to make requests
				// to resolve the manifest, as well as to fetch the manifest and
				// layer contents.
				expected := map[string]int{
					http.MethodGet + " /v2/": 1,
					http.MethodHead + " /v2/" + tc.args.imageName + "_image/manifests/latest":             1,
					http.MethodGet + " /v2/" + tc.args.imageName + "_image/manifests/latest":              1,
					http.MethodGet + " /v2/" + tc.args.imageName + "_image/blobs/" + layerDigest.String(): 1,
				}
				resolveAndCheck(t, tc, te, imageAddress, expected, counter)

				// Try resolving again - the image should now be cached and we
				// should be able to avoid GET requests for manifests and blobs,
				// but we still expect some requests to resolve the tag to a
				// digest.
				expected = map[string]int{
					http.MethodGet + " /v2/": 1,
					http.MethodHead + " /v2/" + tc.args.imageName + "_image/manifests/latest": 1,
				}
				resolveAndCheck(t, tc, te, imageAddress, expected, counter)

				// Try resolving again but fetch using a digest ref - we should
				// still do a HEAD request for auth purposes, even though we
				// don't need to resolve the tag to a digest.
				imageAddressWithDigest := imageAddress + "@" + imageDigest.String()
				expected = map[string]int{
					http.MethodGet + " /v2/": 1,
					http.MethodHead + " /v2/" + tc.args.imageName + "_image/manifests/" + imageDigest.String(): 1,
				}
				resolveAndCheck(t, tc, te, imageAddressWithDigest, expected, counter)

				// Try resolving again but enable registry bypass (server admins
				// can use this to bypass the registry for images that are
				// cached). Should be able to avoid contacting the registry
				// entirely, since we don't need to resolve the tag to a digest.
				tcWithCreds := tc // copy
				tcWithCreds.args.credentials, err = oci.CredentialsFromProperties(&platform.Properties{
					ContainerImage:          imageAddressWithDigest,
					ContainerRegistryBypass: true,
				})
				require.NoError(t, err)
				expected = map[string]int{}
				resolveAndCheck(t, tcWithCreds, te, imageAddressWithDigest, expected, counter)
			}

			// Test with index manifest
			{
				indexAddress := registry.ImageAddress(tc.args.imageName + "_index")
				imageDigest, err := pushedImage.Digest()
				require.NoError(t, err)
				pushedLayers, err := pushedImage.Layers()
				require.NoError(t, err)
				require.Len(t, pushedLayers, 1)
				layerDigest, err := pushedLayers[0].Digest()
				require.NoError(t, err)

				// Initially, nothing is cached, and we expect to make requests
				// to resolve the manifest, as well as to fetch the manifest and
				// layer contents. Note that we have one more GET request here
				// compared to the non-index manifest case, since the index
				// manifest points to the platform-specific image manifest.
				expected := map[string]int{
					http.MethodGet + " /v2/": 1,
					http.MethodHead + " /v2/" + tc.args.imageName + "_index/manifests/latest":                  1,
					http.MethodGet + " /v2/" + tc.args.imageName + "_index/manifests/latest":                   1,
					http.MethodHead + " /v2/" + tc.args.imageName + "_index/manifests/" + imageDigest.String(): 1,
					http.MethodGet + " /v2/" + tc.args.imageName + "_index/manifests/" + imageDigest.String():  1,
					http.MethodGet + " /v2/" + tc.args.imageName + "_index/blobs/" + layerDigest.String():      1,
				}
				resolveAndCheck(t, tc, te, indexAddress, expected, counter)

				// Try resolving again - the image should now be cached and we
				// should be able to avoid GET requests for manifests and blobs,
				// but we still expect some requests to resolve the tag to a
				// digest.
				expected = map[string]int{
					http.MethodGet + " /v2/": 1,
					http.MethodHead + " /v2/" + tc.args.imageName + "_index/manifests/latest":                  1,
					http.MethodHead + " /v2/" + tc.args.imageName + "_index/manifests/" + imageDigest.String(): 1,
				}
				resolveAndCheck(t, tc, te, indexAddress, expected, counter)

				// Try resolving again but fetch using a digest ref - should be
				// able to avoid contacting the registry entirely, since we
				// don't need to resolve the tag to a digest.
				imageAddressWithDigest := indexAddress + "@" + imageDigest.String()
				expected = map[string]int{
					http.MethodGet + " /v2/": 1,
					http.MethodHead + " /v2/" + tc.args.imageName + "_index/manifests/" + imageDigest.String(): 1,
				}
				resolveAndCheck(t, tc, te, imageAddressWithDigest, expected, counter)
			}
		})
	}
}

// contextWithUnverifiedJWT creates a JWT with the given claims
// and attaches it to the returned context.
// Necessary now that we do not allow anonymous requests to access
// the OCI cache.
func contextWithUnverifiedJWT(c *claims.Claims) context.Context {
	authCtx := claims.AuthContextWithJWT(context.Background(), c, nil)
	jwt := authCtx.Value(authutil.ContextTokenStringKey).(string)
	return context.WithValue(context.Background(), authutil.ContextTokenStringKey, jwt)
}

// TestResolve_Concurrency fetches layer contents from multiple goroutines
// to make sure doing so does not make unnecessary requests to the remote registry.
func TestResolve_Concurrency(t *testing.T) {
	te := setupTestEnvWithCache(t)
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})
	flags.Set(t, "executor.container_registry.use_cache_percent", 100)
	counter := newRequestCounter()
	registry := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	imageName := "test_resolve_concurrency"
	_, pushedImage := registry.PushNamedImageWithMultipleLayers(t, imageName+"_image")
	pushedLayers, err := pushedImage.Layers()
	require.NoError(t, err)
	pushedDigestToFiles := make(map[v1.Hash]map[string][]byte, len(pushedLayers))
	pushedDigestToDiffID := make(map[v1.Hash]v1.Hash, len(pushedLayers))
	for _, pushedLayer := range pushedLayers {
		digest, err := pushedLayer.Digest()
		require.NoError(t, err)
		files := layerFiles(t, pushedLayer)
		pushedDigestToFiles[digest] = files
		diffID, err := pushedLayer.DiffID()
		require.NoError(t, err)
		pushedDigestToDiffID[digest] = diffID
	}

	configDigest, err := pushedImage.ConfigName()
	require.NoError(t, err)

	imageAddress := registry.ImageAddress(imageName + "_image")
	expected := map[string]int{
		http.MethodGet + " /v2/": 1,
		http.MethodHead + " /v2/" + imageName + "_image/manifests/latest":               1,
		http.MethodGet + " /v2/" + imageName + "_image/manifests/latest":                1,
		http.MethodHead + " /v2/" + imageName + "_image/blobs/" + configDigest.String(): 1,
		http.MethodGet + " /v2/" + imageName + "_image/blobs/" + configDigest.String():  1,
	}
	for digest, _ := range pushedDigestToFiles {
		expected[http.MethodGet+" /v2/"+imageName+"_image/blobs/"+digest.String()] = 1
	}
	counter.reset()
	c := &claims.Claims{UserID: "US123"}
	testContext := contextWithUnverifiedJWT(c)
	pulledImage, err := newResolver(t, te).Resolve(
		testContext,
		imageAddress,
		&rgpb.Platform{
			Arch: runtime.GOARCH,
			Os:   runtime.GOOS,
		},
		oci.Credentials{},
	)
	require.NoError(t, err)

	layers, err := pulledImage.Layers()
	require.NoError(t, err)
	require.Len(t, layers, len(pushedLayers))

	var layerWG sync.WaitGroup
	layerChan := make(chan layerResult, len(layers))
	for _, layer := range layers {
		layerWG.Add(1)
		go func(layer v1.Layer) {
			defer layerWG.Done()
			digest, digestErr := layer.Digest()
			diffID, diffIDErr := layer.DiffID()
			rc, err := layer.Compressed()
			if err != nil {
				layerChan <- layerResult{
					digest:        digest,
					digestErr:     digestErr,
					diffID:        diffID,
					diffIDErr:     diffIDErr,
					compressedErr: err,
				}
				return
			}
			defer rc.Close()
			compressed, err := io.ReadAll(rc)
			layerChan <- layerResult{
				digest:        digest,
				digestErr:     digestErr,
				diffID:        diffID,
				diffIDErr:     diffIDErr,
				compressed:    compressed,
				compressedErr: err,
			}
		}(layer)
	}
	layerWG.Wait()
	close(layerChan)
	require.Empty(t, cmp.Diff(expected, counter.snapshot()))

	for result := range layerChan {
		require.NoError(t, result.digestErr)
		require.NoError(t, result.diffIDErr)
		require.NoError(t, result.compressedErr)
		pushedDiffID := pushedDigestToDiffID[result.digest]
		require.Equal(t, pushedDiffID, result.diffID)
	}
}

type layerResult struct {
	digest        v1.Hash
	digestErr     error
	diffID        v1.Hash
	diffIDErr     error
	compressed    []byte
	compressedErr error
}

func setupTestEnvWithCache(t *testing.T) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	flags.Set(t, "app.client_identity.client", interfaces.ClientIdentityApp)
	key, err := random.RandomString(16)
	require.NoError(t, err)
	flags.Set(t, "app.client_identity.key", string(key))
	require.NoError(t, err)
	err = clientidentity.Register(te)
	require.NoError(t, err)
	require.NotNil(t, te.GetClientIdentityService())

	// Register OCIFetcherServer for tests
	err = fetcher.Register(te)
	require.NoError(t, err)
	require.NotNil(t, te.GetOCIFetcherServer())

	_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, localGRPClis)
	go runServer()
	return te
}

func resolveAndCheck(t *testing.T, tc resolveTestCase, te *testenv.TestEnv, imageAddress string, expected map[string]int, counter *requestCounter) {
	counter.reset()
	c := &claims.Claims{UserID: "US123"}
	testContext := contextWithUnverifiedJWT(c)
	pulledImage, err := newResolver(t, te).Resolve(
		testContext,
		imageAddress,
		tc.args.platform,
		tc.args.credentials,
	)
	require.NoError(t, err)

	layers, err := pulledImage.Layers()
	require.NoError(t, err)
	require.Len(t, layers, 1)

	files := layerFiles(t, layers[0])
	require.Empty(t, cmp.Diff(tc.imageFiles, files))

	require.Empty(t, cmp.Diff(expected, counter.snapshot()))
}

type requestCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func newRequestCounter() *requestCounter {
	return &requestCounter{counts: make(map[string]int)}
}

func (c *requestCounter) Inc(r *http.Request) {
	c.mu.Lock()
	c.counts[r.Method+" "+r.URL.Path]++
	c.mu.Unlock()
}

func (c *requestCounter) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	snap := make(map[string]int, len(c.counts))
	for k, v := range c.counts {
		snap[k] = v
	}
	return snap
}

func (c *requestCounter) reset() {
	c.mu.Lock()
	c.counts = map[string]int{}
	c.mu.Unlock()
}

func TestResolveImageDigest_TagExists(t *testing.T) {
	te := testenv.GetTestEnv(t)
	require.NoError(t, fetcher.Register(te))
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})
	registry := testregistry.Run(t, testregistry.Opts{})

	imageName := "image_tag_exists"
	_, img := registry.PushNamedImage(t, imageName)
	pushedDigest, err := img.Digest()
	require.NoError(t, err)
	nameToResolve := registry.ImageAddress(imageName)

	nameWithDigest, err := newResolver(t, te).ResolveImageDigest(
		context.Background(),
		nameToResolve,
		oci.RuntimePlatform(),
		oci.Credentials{},
	)
	require.NoError(t, err)

	resolvedDigest, err := name.NewDigest(nameWithDigest)
	require.NoError(t, err)
	require.Equal(t, pushedDigest.String(), resolvedDigest.DigestStr())
}

func TestResolveImageDigest_TagDoesNotExist(t *testing.T) {
	te := testenv.GetTestEnv(t)
	require.NoError(t, fetcher.Register(te))
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})
	registry := testregistry.Run(t, testregistry.Opts{})

	nonexistent := registry.ImageAddress("does_not_exist")

	_, err := newResolver(t, te).ResolveImageDigest(
		context.Background(),
		nonexistent,
		oci.RuntimePlatform(),
		oci.Credentials{},
	)
	require.Error(t, err)
	require.True(t, status.IsUnavailableError(err), "expected UnavailableError, got: %v", err)
}

func TestResolveImageDigest_AlreadyDigest_NoHTTPRequests(t *testing.T) {
	te := testenv.GetTestEnv(t)
	require.NoError(t, fetcher.Register(te))
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})

	counter := newRequestCounter()
	registry := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})

	imageName := "cache_hit"
	_, img := registry.PushNamedImage(t, imageName)
	pushedDigest, err := img.Digest()
	require.NoError(t, err)
	nameToResolve := registry.ImageAddress(imageName + "@" + pushedDigest.String())

	resolver := newResolver(t, te)

	counter.reset()
	nameWithDigest, err := resolver.ResolveImageDigest(
		context.Background(),
		nameToResolve,
		oci.RuntimePlatform(),
		oci.Credentials{},
	)
	require.NoError(t, err)
	resolvedDigest, err := name.NewDigest(nameWithDigest)
	require.NoError(t, err)
	require.Equal(t, pushedDigest.String(), resolvedDigest.DigestStr())

	require.Empty(t, counter.snapshot())
}

func TestResolveImageDigest_CacheHit_NoHTTPRequests(t *testing.T) {
	te := testenv.GetTestEnv(t)
	require.NoError(t, fetcher.Register(te))
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})

	counter := newRequestCounter()
	registry := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})

	imageName := "cache_hit"
	_, img := registry.PushNamedImage(t, imageName)
	pushedDigest, err := img.Digest()
	require.NoError(t, err)
	nameToResolve := registry.ImageAddress(imageName)

	resolver := newResolver(t, te)

	{
		counter.reset()
		nameWithDigest, err := resolver.ResolveImageDigest(
			context.Background(),
			nameToResolve,
			oci.RuntimePlatform(),
			oci.Credentials{},
		)
		require.NoError(t, err)
		resolvedDigest, err := name.NewDigest(nameWithDigest)
		require.NoError(t, err)
		require.Equal(t, pushedDigest.String(), resolvedDigest.DigestStr())

		expectedRequests := map[string]int{
			http.MethodGet + " /v2/":                                    1,
			http.MethodHead + " /v2/" + imageName + "/manifests/latest": 1,
		}
		require.Empty(t, cmp.Diff(expectedRequests, counter.snapshot()))
	}

	{
		counter.reset()
		nameWithDigest, err := resolver.ResolveImageDigest(
			context.Background(),
			nameToResolve,
			oci.RuntimePlatform(),
			oci.Credentials{},
		)
		require.NoError(t, err)

		resolvedDigest, err := name.NewDigest(nameWithDigest)
		require.NoError(t, err)
		require.Equal(t, pushedDigest.String(), resolvedDigest.DigestStr())

		require.Empty(t, counter.snapshot())
	}

	{
		ref, err := name.ParseReference(nameToResolve)
		require.NoError(t, err)
		registryAndRepoNoTag := ref.Context().String()
		counter.reset()
		nameWithDigest, err := resolver.ResolveImageDigest(
			context.Background(),
			registryAndRepoNoTag,
			oci.RuntimePlatform(),
			oci.Credentials{},
		)
		require.NoError(t, err)

		resolvedDigest, err := name.NewDigest(nameWithDigest)
		require.NoError(t, err)
		require.Equal(t, pushedDigest.String(), resolvedDigest.DigestStr())

		require.Empty(t, counter.snapshot())
	}
}

func TestResolveImageDigest_CacheExpiration(t *testing.T) {
	te := testenv.GetTestEnv(t)
	require.NoError(t, fetcher.Register(te))
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})

	counter := newRequestCounter()
	registry := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})

	imageName := "cache_expiration"
	_, img := registry.PushNamedImage(t, imageName)
	pushedDigest, err := img.Digest()
	require.NoError(t, err)
	nameToResolve := registry.ImageAddress(imageName)

	fakeClock := clockwork.NewFakeClock()
	te.SetClock(fakeClock)
	resolver := newResolver(t, te)

	counter.reset()
	nameWithDigest, err := resolver.ResolveImageDigest(
		context.Background(),
		nameToResolve,
		oci.RuntimePlatform(),
		oci.Credentials{},
	)
	require.NoError(t, err)
	resolvedDigest, err := name.NewDigest(nameWithDigest)
	require.NoError(t, err)
	require.Equal(t, pushedDigest.String(), resolvedDigest.DigestStr())

	expectedFirst := map[string]int{
		http.MethodGet + " /v2/":                                    1,
		http.MethodHead + " /v2/" + imageName + "/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expectedFirst, counter.snapshot()))

	// Immediate resolve should be a cache hit; no HTTP requests.
	counter.reset()
	nameWithDigest, err = resolver.ResolveImageDigest(
		context.Background(),
		nameToResolve,
		oci.RuntimePlatform(),
		oci.Credentials{},
	)
	require.NoError(t, err)
	resolvedDigest, err = name.NewDigest(nameWithDigest)
	require.NoError(t, err)
	require.Equal(t, pushedDigest.String(), resolvedDigest.DigestStr())
	require.Empty(t, counter.snapshot())

	// Advance time to just before TTL expiry; still a cache hit.
	fakeClock.Advance(15*time.Minute - time.Second)
	counter.reset()
	nameWithDigest, err = resolver.ResolveImageDigest(
		context.Background(),
		nameToResolve,
		oci.RuntimePlatform(),
		oci.Credentials{},
	)
	require.NoError(t, err)
	resolvedDigest, err = name.NewDigest(nameWithDigest)
	require.NoError(t, err)
	require.Equal(t, pushedDigest.String(), resolvedDigest.DigestStr())
	require.Empty(t, counter.snapshot())

	// Advance past TTL; expect cache refresh (GET /v2/ and HEAD manifest).
	fakeClock.Advance(2 * time.Second)
	counter.reset()
	nameWithDigest, err = resolver.ResolveImageDigest(
		context.Background(),
		nameToResolve,
		oci.RuntimePlatform(),
		oci.Credentials{},
	)
	require.NoError(t, err)
	resolvedDigest, err = name.NewDigest(nameWithDigest)
	require.NoError(t, err)
	require.Equal(t, pushedDigest.String(), resolvedDigest.DigestStr())

	expectedRefresh := map[string]int{
		http.MethodGet + " /v2/":                                    1,
		http.MethodHead + " /v2/" + imageName + "/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expectedRefresh, counter.snapshot()))
}
