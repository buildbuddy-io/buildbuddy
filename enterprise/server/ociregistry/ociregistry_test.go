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

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/ociregistry"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testregistry"
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
					respDigest := resp.Header.Get("Docker-Content-Digest")
					require.NotEmpty(t, respDigest)
					require.Equal(t, expectedDockerContentDigest, respDigest)

					if tc.method == http.MethodGet {
						contentLength, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
						require.NoError(t, err)
						require.Equal(t, expectedContentLength, contentLength)

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
