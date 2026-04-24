package ociregistry_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/ociregistry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
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
		require.NoError(t, testreg.Shutdown())
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
			name:                     "HEAD request for existing manifest digest succeeds",
			method:                   http.MethodHead,
			blobsOrManifests:         "manifests",
			expectedStatus:           http.StatusOK,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 1,
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
			name:                     "GET request for existing manifest digest succeeds",
			method:                   http.MethodHead,
			blobsOrManifests:         "manifests",
			expectedStatus:           http.StatusOK,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 1,
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
			name:                     "repeated GET requests for existing manifest digest use CAS",
			method:                   http.MethodGet,
			blobsOrManifests:         "manifests",
			expectedStatus:           http.StatusOK,
			expectedMirrorRequests:   2,
			expectedUpstreamRequests: 1,
			repeatRequestToHitCache:  true,
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
			// On the second manifest GET, the tag->digest mapping is served from
			// the in-memory cache (no auth header), so no upstream HEAD is needed.
			// The manifest payload is served from CAS.
			expectedUpstreamRequests: 2,
			repeatRequestToHitCache:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repository := strings.ToLower(strings.ReplaceAll(tc.name, " ", "_"))
			testImageName, testImage := testreg.PushNamedImage(t, repository, nil)

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

func TestTagDigestCacheBypassedWithAuth(t *testing.T) {
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

	upstreamCounter := atomic.Int32{}
	testreg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			upstreamCounter.Add(1)
			return true
		},
	})
	t.Cleanup(func() { testreg.Shutdown() })

	ocireg, err := ociregistry.New(te)
	require.Nil(t, err)
	port := testport.FindFree(t)
	mirrorHostPort := fmt.Sprintf("localhost:%d", port)
	mirrorServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ocireg.ServeHTTP(w, r)
	})}
	lis, err := net.Listen("tcp", mirrorHostPort)
	require.NoError(t, err)
	go func() { _ = mirrorServer.Serve(lis) }()
	t.Cleanup(func() { mirrorServer.Shutdown(context.TODO()) })

	repository := "test_auth_cache_bypass"
	testImageName, _ := testreg.PushNamedImage(t, repository, nil)
	path := "/v2/" + testImageName + "/manifests/latest"

	// First GET without auth: populates the tag->digest cache.
	upstreamBefore := upstreamCounter.Load()
	req, err := http.NewRequest(http.MethodGet, "http://"+mirrorHostPort+path, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	// HEAD to resolve tag + GET to fetch manifest = 2 upstream requests.
	require.Equal(t, int32(2), upstreamCounter.Load()-upstreamBefore)

	// Second GET without auth: should use cache (no HEAD needed).
	upstreamBefore = upstreamCounter.Load()
	req, err = http.NewRequest(http.MethodGet, "http://"+mirrorHostPort+path, nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	// Manifest served from CAS, no upstream requests.
	require.Equal(t, int32(0), upstreamCounter.Load()-upstreamBefore)

	// Third GET with auth header: should bypass cache and make upstream HEAD.
	upstreamBefore = upstreamCounter.Load()
	req, err = http.NewRequest(http.MethodGet, "http://"+mirrorHostPort+path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer some-token")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	// HEAD to resolve tag (cache bypassed) but manifest served from CAS = 1 upstream request.
	require.Equal(t, int32(1), upstreamCounter.Load()-upstreamBefore)
}

func TestMirrorConfig(t *testing.T) {
	te := testenv.GetTestEnv(t)
	flags.Set(t, "app.client_identity.client", interfaces.ClientIdentityApp)
	key, err := random.RandomString(16)
	require.NoError(t, err)
	flags.Set(t, "app.client_identity.key", string(key))
	err = clientidentity.Register(te)
	require.NoError(t, err)
	require.NotNil(t, te.GetClientIdentityService())

	_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, localGRPClis)
	go runServer()

	// Two upstream registries, each with their own request counter.
	upstream1Counter := atomic.Int32{}
	upstream1 := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			upstream1Counter.Add(1)
			return true
		},
	})
	t.Cleanup(func() { upstream1.Shutdown() })

	upstream2Counter := atomic.Int32{}
	upstream2 := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			upstream2Counter.Add(1)
			return true
		},
	})
	t.Cleanup(func() { upstream2.Shutdown() })

	// Configure registry domain and two mirrors on different subdomains.
	flags.Set(t, "ociregistry.domain", "registry.test")
	flags.Set(t, "ociregistry.mirrors", []ociregistry.Mirror{
		{
			SubDomain:        "mirror1",
			Namespace:        "ns1",
			RemoteRegistry:   upstream1.Address(),
			RemoteRepository: "repo1",
		},
		{
			SubDomain:        "mirror2",
			Namespace:        "ns2",
			RemoteRegistry:   upstream2.Address(),
			RemoteRepository: "repo2",
		},
	})

	ocireg, err := ociregistry.New(te)
	require.NoError(t, err)
	port := testport.FindFree(t)
	mirrorHostPort := fmt.Sprintf("localhost:%d", port)
	mirrorServer := &http.Server{Handler: ocireg}
	lis, err := net.Listen("tcp", mirrorHostPort)
	require.NoError(t, err)
	go func() { _ = mirrorServer.Serve(lis) }()
	t.Cleanup(func() { mirrorServer.Shutdown(context.TODO()) })

	// Push an image to upstream1 under repo1/myimage (for mirror1).
	_, image1 := upstream1.PushNamedImage(t, "repo1/myimage", nil)
	layers1, err := image1.Layers()
	require.NoError(t, err)
	require.Greater(t, len(layers1), 0)
	layer1Digest, err := layers1[0].Digest()
	require.NoError(t, err)

	// Push an image to upstream2 under repo2/myimage (for mirror2).
	_, image2 := upstream2.PushNamedImage(t, "repo2/myimage", nil)
	layers2, err := image2.Layers()
	require.NoError(t, err)
	require.Greater(t, len(layers2), 0)
	layer2Digest, err := layers2[0].Digest()
	require.NoError(t, err)

	// Also push an image to upstream1 under a plain name for the legacy
	// (non-registry-domain) path.
	legacyImageName, legacyImage := upstream1.PushNamedImage(t, "legacyimage", nil)
	legacyLayers, err := legacyImage.Layers()
	require.NoError(t, err)
	require.Greater(t, len(legacyLayers), 0)
	legacyLayerDigest, err := legacyLayers[0].Digest()
	require.NoError(t, err)

	t.Run("v2 check on mirror subdomain returns OK", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, "http://"+mirrorHostPort+"/v2/", nil)
		require.NoError(t, err)
		req.Host = "mirror1.registry.test"
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("pull blob via mirror1 subdomain routes to upstream1", func(t *testing.T) {
		u1Before := upstream1Counter.Load()
		u2Before := upstream2Counter.Load()

		// Path uses the mirror namespace: ns1/myimage
		path := "/v2/ns1/myimage/blobs/" + layer1Digest.String()
		req, err := http.NewRequest(http.MethodGet, "http://"+mirrorHostPort+path, nil)
		require.NoError(t, err)
		req.Host = "mirror1.registry.test"
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		rc, err := layers1[0].Compressed()
		require.NoError(t, err)
		expectedBody, err := io.ReadAll(rc)
		require.NoError(t, err)
		require.Equal(t, expectedBody, body)

		// Upstream1 should have received a request, upstream2 should not.
		require.Greater(t, upstream1Counter.Load()-u1Before, int32(0))
		require.Equal(t, int32(0), upstream2Counter.Load()-u2Before)
	})

	t.Run("pull blob via mirror2 subdomain routes to upstream2", func(t *testing.T) {
		u1Before := upstream1Counter.Load()
		u2Before := upstream2Counter.Load()

		path := "/v2/ns2/myimage/blobs/" + layer2Digest.String()
		req, err := http.NewRequest(http.MethodGet, "http://"+mirrorHostPort+path, nil)
		require.NoError(t, err)
		req.Host = "mirror2.registry.test"
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		rc, err := layers2[0].Compressed()
		require.NoError(t, err)
		expectedBody, err := io.ReadAll(rc)
		require.NoError(t, err)
		require.Equal(t, expectedBody, body)

		// Upstream2 should have received a request, upstream1 should not.
		require.Equal(t, int32(0), upstream1Counter.Load()-u1Before)
		require.Greater(t, upstream2Counter.Load()-u2Before, int32(0))
	})

	t.Run("pull manifest via mirror1 subdomain routes to upstream1", func(t *testing.T) {
		u1Before := upstream1Counter.Load()
		u2Before := upstream2Counter.Load()

		path := "/v2/ns1/myimage/manifests/latest"
		req, err := http.NewRequest(http.MethodGet, "http://"+mirrorHostPort+path, nil)
		require.NoError(t, err)
		req.Host = "mirror1.registry.test"
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		expectedManifest, err := image1.RawManifest()
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, expectedManifest, body)

		require.Greater(t, upstream1Counter.Load()-u1Before, int32(0))
		require.Equal(t, int32(0), upstream2Counter.Load()-u2Before)
	})

	t.Run("wrong namespace on mirror subdomain returns not found", func(t *testing.T) {
		// ns2 namespace on mirror1 subdomain should not match.
		path := "/v2/ns2/myimage/blobs/" + layer1Digest.String()
		req, err := http.NewRequest(http.MethodGet, "http://"+mirrorHostPort+path, nil)
		require.NoError(t, err)
		req.Host = "mirror1.registry.test"
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("mirror subdomain with port in Host header routes correctly", func(t *testing.T) {
		path := "/v2/ns1/myimage/blobs/" + layer1Digest.String()
		req, err := http.NewRequest(http.MethodGet, "http://"+mirrorHostPort+path, nil)
		require.NoError(t, err)
		// Include a port in the Host header to verify it's stripped before matching.
		req.Host = "mirror1.registry.test:9999"
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		rc, err := layers1[0].Compressed()
		require.NoError(t, err)
		expectedBody, err := io.ReadAll(rc)
		require.NoError(t, err)
		require.Equal(t, expectedBody, body)
	})

	t.Run("unknown subdomain returns not found", func(t *testing.T) {
		path := "/v2/ns1/myimage/blobs/" + layer1Digest.String()
		req, err := http.NewRequest(http.MethodGet, "http://"+mirrorHostPort+path, nil)
		require.NoError(t, err)
		req.Host = "unknown.registry.test"
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("non-registry-domain request uses legacy routing", func(t *testing.T) {
		u1Before := upstream1Counter.Load()

		// Legacy path: repository includes the upstream host in the URL.
		path := "/v2/" + legacyImageName + "/blobs/" + legacyLayerDigest.String()
		req, err := http.NewRequest(http.MethodGet, "http://"+mirrorHostPort+path, nil)
		require.NoError(t, err)
		// Host defaults to mirrorHostPort (localhost:PORT), which does not
		// match registry.test, so the legacy codepath is used.
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		rc, err := legacyLayers[0].Compressed()
		require.NoError(t, err)
		expectedBody, err := io.ReadAll(rc)
		require.NoError(t, err)
		require.Equal(t, expectedBody, body)

		require.Greater(t, upstream1Counter.Load()-u1Before, int32(0))
	})
}

func TestFetchDeduplication(t *testing.T) {
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
	t.Cleanup(func() { mirrorServer.Shutdown(context.TODO()) })

	t.Run("concurrent unauthenticated blob fetches are deduplicated", func(t *testing.T) {
		var upstreamGetCount atomic.Int32
		var mu sync.Mutex
		var upstreamGetPaths []string
		firstGetReceived := make(chan struct{}, 1)
		releaseGets := make(chan struct{})
		var releaseOnce sync.Once
		closeRelease := func() { releaseOnce.Do(func() { close(releaseGets) }) }
		t.Cleanup(closeRelease)

		testreg := testregistry.Run(t, testregistry.Opts{
			HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/blobs/") {
					upstreamGetCount.Add(1)
					mu.Lock()
					upstreamGetPaths = append(upstreamGetPaths, r.URL.Path)
					mu.Unlock()
					select {
					case firstGetReceived <- struct{}{}:
					default:
					}
					<-releaseGets
				}
				return true
			},
		})
		t.Cleanup(func() { testreg.Shutdown() })

		repository := "test_dedup_unauth"
		testImageName, testImage := testreg.PushNamedImage(t, repository, nil)
		layers, err := testImage.Layers()
		require.NoError(t, err)
		require.Greater(t, len(layers), 0)
		blobDigest, err := layers[0].Digest()
		require.NoError(t, err)

		blobURL := fmt.Sprintf("http://%s/v2/%s/blobs/%s", mirrorHostPort, testImageName, blobDigest.String())

		numConcurrent := 5
		type result struct {
			statusCode int
			bodyLen    int
			err        error
		}
		results := make([]result, numConcurrent)
		var wg sync.WaitGroup
		for i := 0; i < numConcurrent; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				req, err := http.NewRequest(http.MethodGet, blobURL, nil)
				if err != nil {
					results[idx] = result{err: err}
					return
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					results[idx] = result{err: err}
					return
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					results[idx] = result{err: err}
					return
				}
				results[idx] = result{statusCode: resp.StatusCode, bodyLen: len(body)}
			}(i)
		}

		// Wait for the first upstream GET to start.
		select {
		case <-firstGetReceived:
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for first upstream GET")
		}
		// Give other requests time to reach the singleflight.
		time.Sleep(100 * time.Millisecond)
		// Only 1 upstream GET should have been initiated.
		require.Equal(t, int32(1), upstreamGetCount.Load())

		// Release the blocked GET to complete all requests.
		closeRelease()
		wg.Wait()

		for i, r := range results {
			require.NoErrorf(t, r.err, "request %d", i)
			require.Equalf(t, http.StatusOK, r.statusCode, "request %d", i)
			require.Greaterf(t, r.bodyLen, 0, "request %d", i)
		}
		// The single upstream GET should have been for the correct digest.
		require.Len(t, upstreamGetPaths, 1)
		require.Contains(t, upstreamGetPaths[0], blobDigest.String())
	})

	t.Run("concurrent authenticated blob fetches bypass deduplication", func(t *testing.T) {
		numConcurrent := int32(3)
		var upstreamGetCount atomic.Int32
		var upstreamGetPaths []string
		var mu sync.Mutex
		allGetsReceived := make(chan struct{})
		releaseGets := make(chan struct{})
		var releaseOnce sync.Once
		closeRelease := func() { releaseOnce.Do(func() { close(releaseGets) }) }
		t.Cleanup(closeRelease)

		testreg := testregistry.Run(t, testregistry.Opts{
			HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/blobs/") {
					mu.Lock()
					upstreamGetPaths = append(upstreamGetPaths, r.URL.Path)
					mu.Unlock()
					if upstreamGetCount.Add(1) == numConcurrent {
						close(allGetsReceived)
					}
					<-releaseGets
				}
				return true
			},
		})
		t.Cleanup(func() { testreg.Shutdown() })

		repository := "test_dedup_auth"
		testImageName, testImage := testreg.PushNamedImage(t, repository, nil)
		layers, err := testImage.Layers()
		require.NoError(t, err)
		require.Greater(t, len(layers), 0)
		blobDigest, err := layers[0].Digest()
		require.NoError(t, err)

		blobURL := fmt.Sprintf("http://%s/v2/%s/blobs/%s", mirrorHostPort, testImageName, blobDigest.String())

		type result struct {
			statusCode int
			err        error
		}
		results := make([]result, numConcurrent)
		var wg sync.WaitGroup
		for i := int32(0); i < numConcurrent; i++ {
			wg.Add(1)
			go func(idx int32) {
				defer wg.Done()
				req, err := http.NewRequest(http.MethodGet, blobURL, nil)
				if err != nil {
					results[idx] = result{err: err}
					return
				}
				req.Header.Set("Authorization", "Bearer some-token")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					results[idx] = result{err: err}
					return
				}
				io.ReadAll(resp.Body)
				resp.Body.Close()
				results[idx] = result{statusCode: resp.StatusCode}
			}(i)
		}

		// All requests should reach upstream independently (no dedup with auth).
		select {
		case <-allGetsReceived:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for %d upstream GETs, got %d", numConcurrent, upstreamGetCount.Load())
		}
		require.Equal(t, numConcurrent, upstreamGetCount.Load())

		closeRelease()
		wg.Wait()

		for i := int32(0); i < numConcurrent; i++ {
			require.NoErrorf(t, results[i].err, "request %d", i)
			require.Equalf(t, http.StatusOK, results[i].statusCode, "request %d", i)
		}
		// All upstream GETs should have been for the correct digest.
		require.Len(t, upstreamGetPaths, int(numConcurrent))
		for i, p := range upstreamGetPaths {
			require.Containsf(t, p, blobDigest.String(), "upstream GET %d", i)
		}
	})
}
