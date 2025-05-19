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
		t.Run(tc.name, func(t *testing.T) {
			te := testenv.GetTestEnv(t)
			flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})
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
					return
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
		for _, readManifests := range []bool{false, true} {
			for _, writeManifests := range []bool{false, true} {
				name := tc.name + fmt.Sprintf("/read_manifests_%t_write_manifests_%t", readManifests, writeManifests)
				t.Run(name, func(t *testing.T) {
					te := testenv.GetTestEnv(t)
					flags.Set(t, "executor.container_registry.read_manifests_from_cache", readManifests)
					flags.Set(t, "executor.container_registry.write_manifests_to_cache", writeManifests)
					flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})
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

					var counter atomic.Int32
					if tc.opts.HttpInterceptor == nil {
						tc.opts.HttpInterceptor = func(w http.ResponseWriter, r *http.Request) bool {
							if r.Method == http.MethodGet && r.URL.Path != "/v2/" {
								counter.Add(1)
							}
							return true
						}
					} else {
						interceptor := tc.opts.HttpInterceptor
						tc.opts.HttpInterceptor = func(w http.ResponseWriter, r *http.Request) bool {
							if r.Method == http.MethodGet && r.URL.Path != "/v2/" {
								counter.Add(1)
							}
							return interceptor(w, r)
						}
					}
					registry := testregistry.Run(t, tc.opts)
					_, pushedImage := registry.PushNamedImageWithFiles(t, tc.imageName+"_image", tc.imageFiles)

					index := mutate.AppendManifests(empty.Index, mutate.IndexAddendum{
						Add: pushedImage,
						Descriptor: v1.Descriptor{
							Platform: &tc.imagePlatform,
						},
					})
					registry.PushIndex(t, index, tc.imageName+"_index")

					resolveAndCheck := func(imageName string, expectedResolveCount int, expectedLayersCount int) {
						counterBeforeResolve := counter.Load()
						pulledImage, err := newResolver(t, te).Resolve(
							context.Background(),
							registry.ImageAddress(imageName),
							tc.args.platform,
							tc.args.credentials,
						)
						counterAfterResolve := counter.Load()
						if tc.checkError != nil {
							require.True(t, tc.checkError(err))
						} else {
							require.NoError(t, err)

							require.Equal(t, int32(expectedResolveCount), counterAfterResolve-counterBeforeResolve)

							layers, err := pulledImage.Layers()
							require.NoError(t, err)
							require.Equal(t, 1, len(layers))
							require.Empty(t, cmp.Diff(tc.imageFiles, layerFiles(t, layers[0])))

							counterAfterLayers := counter.Load()
							require.Equal(t, int32(expectedLayersCount), counterAfterLayers-counterAfterResolve)
						}
					}

					resolveAndCheck(tc.args.imageName+"_image", 1, 1)
					resolveGets := 1
					if readManifests && writeManifests {
						resolveGets = 0
					}
					resolveAndCheck(tc.args.imageName+"_image", resolveGets, 1)
					resolveAndCheck(tc.args.imageName+"_index", 2, 1)
					resolveGets = 2
					if readManifests && writeManifests {
						resolveGets = 0
					}
					resolveAndCheck(tc.args.imageName+"_index", resolveGets, 1)
				})
			}
		}
	}
}

func layerFiles(t *testing.T, layer v1.Layer) map[string][]byte {
	rc, err := layer.Uncompressed()
	require.NoError(t, err)
	defer rc.Close()
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
