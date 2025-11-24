package ociregistry_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/ociregistry"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

type simplePullTestCase struct {
	name                     string
	method                   string
	headers                  map[string]string
	blobsOrManifests         string
	identifierOverride       string
	expectedStatus           int
	expectedMirrorRequests   int32
	expectedUpstreamRequests int32
	repeatRequestToHitCache  bool
}

func TestPull(t *testing.T) {
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

	upstreamCounter := atomic.Int32{}
	testreg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			upstreamCounter.Add(1)
			return true
		},
	})
	t.Cleanup(func() {
		err := testreg.Shutdown(context.TODO())
		require.NoError(t, err)
	})

	nonExistentDigest := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	nonExistentManifestRef := "nonexistentManifestRef"

	ocireg, err := ociregistry.New(te)
	require.Nil(t, err)
	port := testport.FindFree(t)

	mirrorCounter := atomic.Int32{}
	mirrorHostPort := fmt.Sprintf("localhost:%d", port)
	require.NotEmpty(t, mirrorHostPort)
	mirrorServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mirrorCounter.Add(1)
		ocireg.ServeHTTP(w, r)
	})}
	lis, err := net.Listen("tcp", mirrorHostPort)
	require.NoError(t, err)
	go func() { _ = mirrorServer.Serve(lis) }()
	t.Cleanup(func() {
		err := mirrorServer.Shutdown(context.TODO())
		require.NoError(t, err)
	})

	tests := []simplePullTestCase{
		{
			name:                     "HEAD request for nonexistent blob fails",
			method:                   http.MethodHead,
			blobsOrManifests:         "blobs",
			identifierOverride:       nonExistentDigest,
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 1,
		},
		{
			name:                     "HEAD request for existing blob succeeds",
			method:                   http.MethodHead,
			blobsOrManifests:         "blobs",
			expectedStatus:           http.StatusOK,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 1,
		},
		{
			name:                     "GET request for nonexistent blob fails",
			method:                   http.MethodGet,
			blobsOrManifests:         "blobs",
			identifierOverride:       nonExistentDigest,
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 1,
		},
		{
			name:                     "GET request for existing blob succeeds",
			method:                   http.MethodGet,
			blobsOrManifests:         "blobs",
			expectedStatus:           http.StatusOK,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 1,
		},
		{
			name:                     "HEAD request for nonexistent manifest fails",
			method:                   http.MethodHead,
			blobsOrManifests:         "manifests",
			identifierOverride:       nonExistentManifestRef,
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 1,
		},
		{
			name:                     "HEAD request for existing manifest tag succeeds",
			method:                   http.MethodHead,
			blobsOrManifests:         "manifests",
			identifierOverride:       "latest",
			expectedStatus:           http.StatusOK,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 1,
		},
		{
			name:             "HEAD request for existing manifest digest succeeds",
			method:           http.MethodHead,
			blobsOrManifests: "manifests",
			expectedStatus:   http.StatusOK,
			expectedMirrorRequests: 1,
			// Manifest-by-digest requests now make a HEAD to validate upstream access,
			// then cache lookup (miss), then upstream request.
			expectedUpstreamRequests: 2,
		},
		{
			name:                     "GET request for nonexistent manifest fails",
			method:                   http.MethodGet,
			blobsOrManifests:         "manifests",
			identifierOverride:       nonExistentManifestRef,
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 1,
		},
		{
			name:                   "GET request for existing manifest tag succeeds",
			method:                 http.MethodGet,
			blobsOrManifests:       "manifests",
			identifierOverride:     "latest",
			expectedStatus:         http.StatusOK,
			expectedMirrorRequests: 1,
			// The mirror will first make an upstream HEAD request to convert tag to digest,
			// then make an upstream GET request to fetch the manifest payload.
			expectedUpstreamRequests: 2,
		},
		{
			name:             "GET request for existing manifest digest succeeds",
			method:           http.MethodHead,
			blobsOrManifests: "manifests",
			expectedStatus:   http.StatusOK,
			expectedMirrorRequests: 1,
			// Manifest-by-digest requests now make a HEAD to validate upstream access,
			// then cache lookup (miss), then upstream request.
			expectedUpstreamRequests: 2,
		},
		{
			name:                     "POST request to /blobs/uploads/ fails",
			method:                   http.MethodPost,
			blobsOrManifests:         "blobs",
			identifierOverride:       "uploads/",
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:                     "PUT request for new manifest tag fails",
			method:                   http.MethodPut,
			blobsOrManifests:         "manifests",
			identifierOverride:       "newtag",
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:                     "PUT request for existing manifest tag fails",
			method:                   http.MethodPut,
			blobsOrManifests:         "manifests",
			identifierOverride:       "latest",
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:                     "PUT request for existing manifest digest fails",
			method:                   http.MethodPut,
			blobsOrManifests:         "manifests",
			identifierOverride:       "latest",
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:                     "DELETE request for existing manifest tag fails",
			method:                   http.MethodDelete,
			blobsOrManifests:         "manifests",
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:                     "DELETE request for existing manifest digest fails",
			method:                   http.MethodDelete,
			blobsOrManifests:         "manifests",
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:                     "DELETE request for nonexistent blob fails",
			method:                   http.MethodDelete,
			blobsOrManifests:         "blobs",
			identifierOverride:       nonExistentDigest,
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:                     "DELETE request for existing blob fails",
			method:                   http.MethodDelete,
			blobsOrManifests:         "blobs",
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:             "GET request with Range header for existing blob fails",
			method:           http.MethodGet,
			blobsOrManifests: "blobs",
			headers: map[string]string{
				"Range": "bytes=0-7",
			},
			expectedStatus:           http.StatusNotImplemented,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:                     "repeated GET requests for existing blob use CAS",
			method:                   http.MethodGet,
			blobsOrManifests:         "blobs",
			expectedStatus:           http.StatusOK,
			expectedMirrorRequests:   2,
			expectedUpstreamRequests: 1,
			repeatRequestToHitCache:  true,
		},
		{
			name:                    "repeated GET requests for existing manifest digest use CAS",
			method:                  http.MethodGet,
			blobsOrManifests:        "manifests",
			expectedStatus:          http.StatusOK,
			expectedMirrorRequests:  2,
			repeatRequestToHitCache: true,
			// First request: HEAD (access validation) + cache miss + GET (fetch) = 2 upstream
			// Second request: HEAD (access validation) + cache hit = 1 upstream
			// Total: 3 upstream requests
			expectedUpstreamRequests: 3,
		},
		{
			name:                   "repeated GET requests for existing manifest tag use CAS",
			method:                 http.MethodGet,
			blobsOrManifests:       "manifests",
			identifierOverride:     "latest",
			expectedStatus:         http.StatusOK,
			expectedMirrorRequests: 2,
			// On the first manifest GET, the mirror makes
			//   1. an upstream HEAD request to resolve the manifest digest
			//   2. an upstream GET request to fetch the manifest payload (which is then stored in CAS).
			//
			// On the second manifest GET, the mirror makes
			//   3. an upstream HEAD request to resolve the manifest digest
			//     (which it uses to fetch manifest payload from CAS).
			expectedUpstreamRequests: 3,
			repeatRequestToHitCache:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repository := strings.ToLower(strings.ReplaceAll(tc.name, " ", "_"))
			testImageName, testImage := testreg.PushNamedImage(t, repository)

			var identifier string
			var expectedDockerContentDigest string
			var expectedContentLength int64
			var expectedBody []byte
			switch tc.blobsOrManifests {
			case "blobs":
				testLayers, err := testImage.Layers()
				require.NoError(t, err)
				require.Greater(t, len(testLayers), 0)
				testLayer := testLayers[0]
				testLayerDigest, err := testLayer.Digest()
				require.NoError(t, err)
				identifier = testLayerDigest.String()
				expectedDockerContentDigest = testLayerDigest.String()
				testLayerSize, err := testLayer.Size()
				require.NoError(t, err)
				expectedContentLength = testLayerSize

				rc, err := testLayer.Compressed()
				require.NoError(t, err)
				testLayerBuf, err := io.ReadAll(rc)
				require.NoError(t, err)
				expectedBody = testLayerBuf

			case "manifests":
				headResp, err := http.Head("http://" + testreg.Address() + "/v2/" + repository + "/manifests/latest")
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, headResp.StatusCode)
				testManifestDigest := headResp.Header.Get("Docker-Content-Digest")
				require.NotEmpty(t, testManifestDigest)
				identifier = testManifestDigest
				expectedDockerContentDigest = testManifestDigest
				testManifestSize, err := strconv.ParseInt(headResp.Header.Get("Content-Length"), 10, 64)
				require.NoError(t, err)
				expectedContentLength = testManifestSize
				expectedBody, err = testImage.RawManifest()
				require.NoError(t, err)
			default:
				t.Fatalf("expected blobsOrManifests to be 'blobs' or 'manifests', got '%s'", tc.blobsOrManifests)
			}
			if tc.identifierOverride != "" {
				identifier = tc.identifierOverride
			}
			path := "/v2/" + testImageName + "/" + tc.blobsOrManifests + "/" + identifier

			mirrorRequestsAtStart := mirrorCounter.Load()
			upstreamRequestsAtStart := upstreamCounter.Load()

			loops := 1
			if tc.repeatRequestToHitCache {
				loops = 2
			}
			for i := 0; i < loops; i++ {
				req, err := http.NewRequest(tc.method, "http://"+mirrorHostPort+path, nil)
				require.NoError(t, err)
				for key, value := range tc.headers {
					req.Header.Add(key, value)
				}
				resp, err := http.DefaultClient.Do(req)
				require.NoError(t, err)
				require.Equal(t, tc.expectedStatus, resp.StatusCode)

				if resp.StatusCode == http.StatusOK {
					if tc.blobsOrManifests == "manifests" {
						respDigest := resp.Header.Get("Docker-Content-Digest")
						require.NotEmpty(t, respDigest)
						require.Equal(t, expectedDockerContentDigest, respDigest)
					}

					if tc.method == http.MethodGet {
						contentLength, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
						require.NoError(t, err)
						require.Equalf(t, expectedContentLength, contentLength, "content length %d (expected %d) for %q", contentLength, expectedContentLength, path)

						respBody, err := io.ReadAll(resp.Body)
						require.NoError(t, err)
						require.Equal(t, len(expectedBody), len(respBody))
						require.Equal(t, expectedContentLength, int64(len(expectedBody)))
						require.Equal(t, expectedBody, respBody)
					}
				}
			}

			require.Equal(t, tc.expectedMirrorRequests, mirrorCounter.Load()-mirrorRequestsAtStart)
			require.Equal(t, tc.expectedUpstreamRequests, upstreamCounter.Load()-upstreamRequestsAtStart)
		})
	}
}

// TestManifestAccessValidation tests that cached manifests require upstream validation
// before being served. This prevents unauthorized access to cached private images.
func TestManifestAccessValidation(t *testing.T) {
	te := testenv.GetTestEnv(t)
	flags.Set(t, "app.client_identity.client", interfaces.ClientIdentityApp)
	key, err := random.RandomString(16)
	require.NoError(t, err)
	flags.Set(t, "app.client_identity.key", string(key))
	err = clientidentity.Register(te)
	require.NoError(t, err)

	_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, localGRPClis)
	go runServer()

	tests := []struct {
		name                   string
		upstreamStatusOnSecond int
		expectedSecondStatus   int
	}{
		{
			name:                   "upstream_401_denies_cached_manifest_access",
			upstreamStatusOnSecond: http.StatusUnauthorized,
			expectedSecondStatus:   http.StatusForbidden,
		},
		{
			name:                   "upstream_403_denies_cached_manifest_access",
			upstreamStatusOnSecond: http.StatusForbidden,
			expectedSecondStatus:   http.StatusForbidden,
		},
		{
			name:                   "upstream_404_returns_not_found_for_cached_manifest",
			upstreamStatusOnSecond: http.StatusNotFound,
			expectedSecondStatus:   http.StatusNotFound,
		},
		{
			name:                   "upstream_500_denies_cached_manifest_access",
			upstreamStatusOnSecond: http.StatusInternalServerError,
			expectedSecondStatus:   http.StatusServiceUnavailable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Track the manifest digest we're testing - only intercept requests to this specific digest
			var targetDigest string
			var interceptEnabled atomic.Bool

			testreg := testregistry.Run(t, testregistry.Opts{
				HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
					// Only intercept after we've set up the target and enabled interception
					if !interceptEnabled.Load() {
						return true
					}
					// Only intercept HEAD requests for our specific manifest digest
					if r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/manifests/") && strings.Contains(r.URL.Path, targetDigest) {
						w.WriteHeader(tc.upstreamStatusOnSecond)
						return false
					}
					return true
				},
			})
			t.Cleanup(func() {
				err := testreg.Shutdown(context.TODO())
				require.NoError(t, err)
			})

			ocireg, err := ociregistry.New(te)
			require.NoError(t, err)
			port := testport.FindFree(t)

			mirrorHostPort := fmt.Sprintf("localhost:%d", port)
			mirrorServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ocireg.ServeHTTP(w, r)
			})}
			lis, err := net.Listen("tcp", mirrorHostPort)
			require.NoError(t, err)
			go func() { _ = mirrorServer.Serve(lis) }()
			t.Cleanup(func() {
				err := mirrorServer.Shutdown(context.TODO())
				require.NoError(t, err)
			})

			repository := tc.name
			testImageName, testImage := testreg.PushNamedImage(t, repository)

			// Get the manifest digest
			testManifest, err := testImage.RawManifest()
			require.NoError(t, err)
			testManifestDigest, err := testImage.Digest()
			require.NoError(t, err)

			// Set the target digest for interception
			targetDigest = testManifestDigest.Hex

			path := "/v2/" + testImageName + "/manifests/" + testManifestDigest.String()

			// First request: GET manifest by digest - should succeed and cache
			req, err := http.NewRequest(http.MethodGet, "http://"+mirrorHostPort+path, nil)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode, "first request should succeed")
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, testManifest, body)

			// Enable interception for subsequent requests
			interceptEnabled.Store(true)

			// Second request: GET manifest by digest - should fail because upstream validation fails
			req, err = http.NewRequest(http.MethodGet, "http://"+mirrorHostPort+path, nil)
			require.NoError(t, err)
			resp, err = http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, tc.expectedSecondStatus, resp.StatusCode, "second request should fail with expected status")
		})
	}
}

// TestManifestAccessValidationPassesAuthHeader verifies that the Authorization header
// is passed to upstream during validation.
func TestManifestAccessValidationPassesAuthHeader(t *testing.T) {
	te := testenv.GetTestEnv(t)
	flags.Set(t, "app.client_identity.client", interfaces.ClientIdentityApp)
	key, err := random.RandomString(16)
	require.NoError(t, err)
	flags.Set(t, "app.client_identity.key", string(key))
	err = clientidentity.Register(te)
	require.NoError(t, err)

	_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, localGRPClis)
	go runServer()

	validToken := "Bearer valid-token"
	invalidToken := "Bearer invalid-token"

	// Track target digest and enable auth checking only after initial cache is populated
	var targetDigest string
	var authCheckEnabled atomic.Bool

	testreg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			// Only check auth after we've enabled it and for our specific manifest
			if !authCheckEnabled.Load() {
				return true
			}
			if r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/manifests/") && strings.Contains(r.URL.Path, targetDigest) {
				authHeader := r.Header.Get("Authorization")
				if authHeader != validToken {
					w.WriteHeader(http.StatusUnauthorized)
					return false
				}
			}
			return true
		},
	})
	t.Cleanup(func() {
		err := testreg.Shutdown(context.TODO())
		require.NoError(t, err)
	})

	ocireg, err := ociregistry.New(te)
	require.NoError(t, err)
	port := testport.FindFree(t)

	mirrorHostPort := fmt.Sprintf("localhost:%d", port)
	mirrorServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ocireg.ServeHTTP(w, r)
	})}
	lis, err := net.Listen("tcp", mirrorHostPort)
	require.NoError(t, err)
	go func() { _ = mirrorServer.Serve(lis) }()
	t.Cleanup(func() {
		err := mirrorServer.Shutdown(context.TODO())
		require.NoError(t, err)
	})

	repository := "auth_header_test"
	testImageName, testImage := testreg.PushNamedImage(t, repository)
	testManifestDigest, err := testImage.Digest()
	require.NoError(t, err)

	// Set the target digest for auth checking
	targetDigest = testManifestDigest.Hex

	path := "/v2/" + testImageName + "/manifests/" + testManifestDigest.String()

	// First request with valid token - should succeed and cache
	req, err := http.NewRequest(http.MethodGet, "http://"+mirrorHostPort+path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", validToken)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "first request with valid token should succeed")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// Enable auth checking for subsequent requests
	authCheckEnabled.Store(true)

	// Second request with invalid token - should fail because validation fails
	req, err = http.NewRequest(http.MethodGet, "http://"+mirrorHostPort+path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", invalidToken)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode, "second request with invalid token should fail")

	// Third request with valid token - should succeed (validation passes)
	req, err = http.NewRequest(http.MethodGet, "http://"+mirrorHostPort+path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", validToken)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "third request with valid token should succeed")
}
