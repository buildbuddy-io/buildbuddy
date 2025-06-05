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
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
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
				flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})
				flags.Set(t, "executor.container_registry.use_cache_percent", useCachePercent)
				upstreamCounter := atomic.Int32{}
				registry := testregistry.Run(t, testregistry.Opts{
					HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
						if r.Method == http.MethodGet && r.URL.Path != "/v2/" {
							upstreamCounter.Add(1)
						}
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

					beforeLayersCount := upstreamCounter.Load()
					layers, err := pulledImage.Layers()
					require.NoError(t, err)

					afterLayersCount := upstreamCounter.Load()
					require.Zero(t, afterLayersCount-beforeLayersCount)

					// To make the DiffID() request counts always be zero,
					// fetch the config file here. Otherwise the first
					// Layer.DiffID() call will make a request to fetch the config file.
					beforeConfigFileCount := upstreamCounter.Load()
					_, err = pulledImage.ConfigFile()
					require.NoError(t, err)
					afterConfigFileCount := upstreamCounter.Load()
					require.Equal(t, int32(1), afterConfigFileCount-beforeConfigFileCount)

					for _, layer := range layers {
						beforeDiffIDCount := upstreamCounter.Load()
						_, err := layer.DiffID()
						require.NoError(t, err)
						afterDiffIDCount := upstreamCounter.Load()
						require.Zero(t, afterDiffIDCount-beforeDiffIDCount)
					}
				}
			})
		}
	}
}

func layerFiles(t *testing.T, layer v1.Layer) map[string][]byte {
	rc, err := layer.Uncompressed()
	require.NoError(t, err)
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
	err = rc.Close()
	require.NoError(t, err)
	return contents
}

func TestResolve_FallsBackToOriginalWhenMirrorFails(t *testing.T) {
	te := testenv.GetTestEnv(t)
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
		readLayers := false
		for _, writeManifests := range []bool{false, true} {
			for _, readManifests := range []bool{false, true} {
				for _, writeLayers := range []bool{false, true} {
					name := tc.name + fmt.Sprintf(
						"/write_manifests_%t_read_manifests_%t_write_layers_%t_read_layers_%t",
						writeManifests,
						readManifests,
						writeLayers,
						readLayers,
					)
					t.Run(name, func(t *testing.T) {
						te := setupTestEnvWithCache(t)
						flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})
						flags.Set(t, "executor.container_registry.use_cache_percent", 100)
						flags.Set(t, "executor.container_registry.write_manifests_to_cache", writeManifests)
						flags.Set(t, "executor.container_registry.read_manifests_from_cache", readManifests)
						flags.Set(t, "executor.container_registry.write_layers_to_cache", writeLayers)
						counter := atomic.Int32{}
						registry := testregistry.Run(t, testregistry.Opts{
							HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
								if r.Method == http.MethodGet && r.URL.Path != "/v2/" {
									counter.Add(1)
								}
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

						{
							imageAddress := registry.ImageAddress(tc.args.imageName + "_image")
							resolveCount := int32(1)
							filesCount := int32(1)
							resolveAndCheck(t, tc, te, imageAddress, &counter, resolveCount, filesCount)

							resolveCount = int32(1)
							if writeManifests && readManifests {
								resolveCount = int32(0)
							}
							filesCount = int32(1)
							if writeLayers && readLayers {
								filesCount = int32(0)
							}
							resolveAndCheck(t, tc, te, imageAddress, &counter, resolveCount, filesCount)
						}

						{
							indexAddress := registry.ImageAddress(tc.args.imageName + "_index")
							resolveCount := int32(2)
							filesCount := int32(1)
							resolveAndCheck(t, tc, te, indexAddress, &counter, resolveCount, filesCount)

							resolveCount = int32(2)
							if writeManifests && readManifests {
								resolveCount = int32(0)
							}
							filesCount = int32(1)
							if writeLayers && readLayers {
								filesCount = int32(0)
							}
							resolveAndCheck(t, tc, te, indexAddress, &counter, resolveCount, filesCount)
						}
					})
				}
			}
		}
	}
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

	_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, localGRPClis)
	go runServer()
	return te
}

func resolveAndCheck(t *testing.T, tc resolveTestCase, te *testenv.TestEnv, imageAddress string, counter *atomic.Int32, resolveCount, filesCount int32) {
	beforeResolve := counter.Load()
	pulledImage, err := newResolver(t, te).Resolve(
		context.Background(),
		imageAddress,
		tc.args.platform,
		tc.args.credentials,
	)
	require.NoError(t, err)
	require.Equal(t, resolveCount, counter.Load()-beforeResolve)

	beforeLayers := counter.Load()
	layers, err := pulledImage.Layers()
	require.NoError(t, err)
	require.Len(t, layers, 1)
	require.Zero(t, beforeLayers-counter.Load())

	beforeFiles := counter.Load()
	files := layerFiles(t, layers[0])
	require.Equal(t, filesCount, counter.Load()-beforeFiles)

	require.Empty(t, cmp.Diff(tc.imageFiles, files))
}
