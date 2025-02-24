package ociregistry_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/ociregistry"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testregistry"
	"github.com/stretchr/testify/require"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

type pullTestCase struct {
	name                     string
	method                   string
	path                     string
	headers                  map[string]string
	expectedStatus           int
	expectedDigest           string
	expectedContentLength    int64
	expectedBody             []byte
	expectedMirrorRequests   int32
	expectedUpstreamRequests int32
	repeatRequestToHitCache  bool
}

func TestPull(t *testing.T) {
	te := testenv.GetTestEnv(t)

	_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, localGRPClis)
	go runServer()

	conn, err := testenv.LocalGRPCConn(context.TODO(), localGRPClis)
	require.NoError(t, err)
	te.SetByteStreamClient(bspb.NewByteStreamClient(conn))

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
	testImageName, testImage := testreg.PushRandomImage(t)
	require.NotEmpty(t, testImageName)

	testLayers, err := testImage.Layers()
	require.NoError(t, err)
	require.Greater(t, len(testLayers), 0)
	testLayer := testLayers[0]
	testLayerDigest, err := testLayer.Digest()
	require.NoError(t, err)

	nonExistentDigest := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	require.NotEqual(t, testLayerDigest, nonExistentDigest)
	nonExistentManifestRef := "nonexistentManifestRef"

	headResp, err := http.Head("http://" + testreg.Address() + "/v2/test/manifests/latest")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, headResp.StatusCode)
	testManifestDigest := headResp.Header.Get("Docker-Content-Digest")
	require.NotEmpty(t, testManifestDigest)
	testManifestSize, err := strconv.ParseInt(headResp.Header.Get("Content-Length"), 10, 64)
	require.NoError(t, err)

	testLayerSize, err := testLayer.Size()
	require.NoError(t, err)

	rc, err := testLayer.Compressed()
	require.NoError(t, err)
	testLayerBuf, err := io.ReadAll(rc)
	require.NoError(t, err)

	tests := []pullTestCase{
		{
			name:                     "HEAD request for nonexistent blob fails",
			method:                   http.MethodHead,
			path:                     "/v2/" + testImageName + "/blobs/" + nonExistentDigest,
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 1,
		},
		{
			name:                     "HEAD request for existing blob succeeds",
			method:                   http.MethodHead,
			path:                     "/v2/" + testImageName + "/blobs/" + testLayerDigest.String(),
			expectedStatus:           http.StatusOK,
			expectedDigest:           testLayerDigest.String(),
			expectedContentLength:    testLayerSize,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 1,
		},
		{
			name:                     "GET request for nonexistent blob fails",
			method:                   http.MethodGet,
			path:                     "/v2/" + testImageName + "/blobs/" + nonExistentDigest,
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 1,
		},
		{
			name:                     "GET request for existing blob succeeds",
			method:                   http.MethodGet,
			path:                     "/v2/" + testImageName + "/blobs/" + testLayerDigest.String(),
			expectedStatus:           http.StatusOK,
			expectedBody:             testLayerBuf,
			expectedDigest:           testLayerDigest.String(),
			expectedContentLength:    testLayerSize,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 1,
		},
		{
			name:                     "HEAD request for nonexistent manifest fails",
			method:                   http.MethodHead,
			path:                     "/v2/" + testImageName + "/manifests/" + nonExistentManifestRef,
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 1,
		},
		{
			name:                     "HEAD request for existing manifest tag succeeds",
			method:                   http.MethodHead,
			path:                     "/v2/" + testImageName + "/manifests/latest",
			expectedStatus:           http.StatusOK,
			expectedDigest:           testManifestDigest,
			expectedContentLength:    testManifestSize,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 1,
		},
		{
			name:                     "HEAD request for existing manifest digest succeeds",
			method:                   http.MethodHead,
			path:                     "/v2/" + testImageName + "/manifests/" + testManifestDigest,
			expectedStatus:           http.StatusOK,
			expectedDigest:           testManifestDigest,
			expectedContentLength:    testManifestSize,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 1,
		},
		{
			name:                     "POST request to /blobs/uploads/ fails",
			method:                   http.MethodPost,
			path:                     "/v2/" + testImageName + "/blobs/uploads/",
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:                     "PUT request for new manifest tag fails",
			method:                   http.MethodPut,
			path:                     "/v2/" + testImageName + "/manifests/newtag",
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:                     "PUT request for existing manifest tag fails",
			method:                   http.MethodPut,
			path:                     "/v2/" + testImageName + "/manifests/latest",
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:                     "PUT request for existing manifest digest fails",
			method:                   http.MethodPut,
			path:                     "/v2/" + testImageName + "/manifests/latest",
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:                     "DELETE request for existing manifest tag fails",
			method:                   http.MethodDelete,
			path:                     "/v2/" + testImageName + "/manifests/" + testManifestDigest,
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:                     "DELETE request for existing manifest digest fails",
			method:                   http.MethodDelete,
			path:                     "/v2/" + testImageName + "/manifests/" + testManifestDigest,
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:                     "DELETE request for nonexistent blob fails",
			method:                   http.MethodDelete,
			path:                     "/v2/" + testImageName + "/blobs/" + nonExistentDigest,
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:                     "DELETE request for existing blob fails",
			method:                   http.MethodDelete,
			path:                     "/v2/" + testImageName + "/blobs/" + testLayerDigest.String(),
			expectedStatus:           http.StatusNotFound,
			expectedMirrorRequests:   1,
			expectedUpstreamRequests: 0,
		},
		{
			name:   "GET request with Range header for existing blob fails",
			method: http.MethodGet,
			path:   "/v2/" + testImageName + "/blobs/" + testLayerDigest.String(),
			headers: map[string]string{
				"Range": "bytes=0-7",
			},
			expectedStatus: http.StatusNotImplemented,
		},
		{
			name:                     "repeated HEAD requests for existing blob use CAS",
			method:                   http.MethodHead,
			path:                     "/v2/" + testImageName + "/blobs/" + testLayerDigest.String(),
			expectedStatus:           http.StatusOK,
			expectedDigest:           testLayerDigest.String(),
			expectedContentLength:    testLayerSize,
			expectedMirrorRequests:   2,
			expectedUpstreamRequests: 1,
			repeatRequestToHitCache:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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

			mirrorRequestsAtStart := mirrorCounter.Load()
			upstreamRequestsAtStart := upstreamCounter.Load()

			loops := 1
			if tc.repeatRequestToHitCache {
				loops = 2
			}
			for i := 0; i < loops; i++ {
				req, err := http.NewRequest(tc.method, "http://"+mirrorHostPort+tc.path, nil)
				require.NoError(t, err)
				for key, value := range tc.headers {
					req.Header.Add(key, value)
				}
				resp, err := http.DefaultClient.Do(req)
				require.NoError(t, err)
				require.Equal(t, tc.expectedStatus, resp.StatusCode)
				if len(tc.expectedBody) > 0 {
					respBody, err := io.ReadAll(resp.Body)
					require.NoError(t, err)
					require.Equal(t, len(tc.expectedBody), len(respBody))
					require.Equal(t, tc.expectedBody, respBody)
				}
				if len(tc.expectedDigest) > 0 {
					respDigest := resp.Header.Get("Docker-Content-Digest")
					require.NotEmpty(t, respDigest)
					require.Equal(t, tc.expectedDigest, respDigest)
				}
				if resp.StatusCode == http.StatusOK {
					contentLength, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
					require.NoError(t, err)
					require.Equal(t, tc.expectedContentLength, contentLength)
				}
			}

			err = mirrorServer.Shutdown(context.TODO())
			require.NoError(t, err)

			require.Equal(t, tc.expectedMirrorRequests, mirrorCounter.Load()-mirrorRequestsAtStart)
			require.Equal(t, tc.expectedUpstreamRequests, upstreamCounter.Load()-upstreamRequestsAtStart)
		})
	}
}
