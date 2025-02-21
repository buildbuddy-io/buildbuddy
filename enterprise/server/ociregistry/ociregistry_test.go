package ociregistry_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/ociregistry"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testregistry"
	"github.com/stretchr/testify/require"
)

func runMirrorRegistry(t *testing.T, env environment.Env) string {
	t.Helper()
	ocireg, err := ociregistry.New(env)
	require.Nil(t, err)
	port := testport.FindFree(t)

	mux := http.NewServeMux()
	mux.Handle("/", ocireg)

	listenHostPort := fmt.Sprintf("localhost:%d", port)
	server := &http.Server{Handler: ocireg}
	lis, err := net.Listen("tcp", listenHostPort)
	require.NoError(t, err)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Shutdown(context.TODO())
	})
	return listenHostPort
}

type pullTestCase struct {
	name                  string
	method                string
	path                  string
	expectedStatus        int
	expectedDigest        string
	expectedContentLength int64
	expectedBody          []byte
}

func TestPull(t *testing.T) {
	te := testenv.GetTestEnv(t)

	testreg := testregistry.Run(t, testregistry.Opts{})
	testImageName, testImage := testreg.PushRandomImage(t)
	require.NotEmpty(t, testImageName)
	mirrorHostPort := runMirrorRegistry(t, te)
	require.NotEmpty(t, mirrorHostPort)
	mirrorAddr := "http://" + mirrorHostPort

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
			name:           "HEAD request to nonexistent blob",
			method:         http.MethodHead,
			path:           mirrorAddr + "/v2/" + testImageName + "/blobs/" + nonExistentDigest,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:                  "HEAD request to existing blob",
			method:                http.MethodHead,
			path:                  mirrorAddr + "/v2/" + testImageName + "/blobs/" + testLayerDigest.String(),
			expectedStatus:        http.StatusOK,
			expectedDigest:        testLayerDigest.String(),
			expectedContentLength: testLayerSize,
		},
		{
			name:           "GET nonexistent blob",
			method:         http.MethodGet,
			path:           mirrorAddr + "/v2/" + testImageName + "/blobs/" + nonExistentDigest,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:                  "GET request to existing blob",
			method:                http.MethodGet,
			path:                  mirrorAddr + "/v2/" + testImageName + "/blobs/" + testLayerDigest.String(),
			expectedStatus:        http.StatusOK,
			expectedBody:          testLayerBuf,
			expectedDigest:        testLayerDigest.String(),
			expectedContentLength: testLayerSize,
		},
		{
			name:           "HEAD request to nonexistent manifest",
			method:         http.MethodHead,
			path:           mirrorAddr + "/v2/" + testImageName + "/manifests/" + nonExistentManifestRef,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:                  "HEAD request to latest manifest",
			method:                http.MethodHead,
			path:                  mirrorAddr + "/v2/" + testImageName + "/manifests/latest",
			expectedStatus:        http.StatusOK,
			expectedDigest:        testManifestDigest,
			expectedContentLength: testManifestSize,
		},
		{
			name:                  "HEAD request to manifest by digest",
			method:                http.MethodHead,
			path:                  mirrorAddr + "/v2/" + testImageName + "/manifests/" + testManifestDigest,
			expectedStatus:        http.StatusOK,
			expectedDigest:        testManifestDigest,
			expectedContentLength: testManifestSize,
		},
		// {
		// 	name:           "HEAD request to manifest[1] path (digest)",
		// 	method:         http.MethodHead,
		// 	path:           "/v2/repo/manifests/sha256:manifest1",
		// 	expectedStatus: http.StatusOK,
		// 	isManifest:     true,
		// 	exists:         true,
		// },
		// {
		// 	name:           "HEAD request to manifest path (tag)",
		// 	method:         http.MethodHead,
		// 	path:           "/v2/repo/manifests/latest",
		// 	expectedStatus: http.StatusOK,
		// 	isManifest:     true,
		// 	exists:         true,
		// },
		// {
		// 	name:           "GET nonexistent manifest",
		// 	method:         http.MethodGet,
		// 	path:           "/v2/repo/manifests/nonexistent",
		// 	expectedStatus: http.StatusNotFound,
		// 	isManifest:     true,
		// 	exists:         false,
		// },
		// {
		// 	name:           "GET request to manifest[0] path (digest)",
		// 	method:         http.MethodGet,
		// 	path:           "/v2/repo/manifests/sha256:manifest0",
		// 	expectedStatus: http.StatusOK,
		// 	isManifest:     true,
		// 	exists:         true,
		// },
		// {
		// 	name:           "GET request to manifest[1] path (digest)",
		// 	method:         http.MethodGet,
		// 	path:           "/v2/repo/manifests/sha256:manifest1",
		// 	expectedStatus: http.StatusOK,
		// 	isManifest:     true,
		// 	exists:         true,
		// },
		// {
		// 	name:           "GET request to manifest path (tag)",
		// 	method:         http.MethodGet,
		// 	path:           "/v2/repo/manifests/latest",
		// 	expectedStatus: http.StatusOK,
		// 	isManifest:     true,
		// 	exists:         true,
		// },
		// {
		// 	name:           "400 response body contains OCI-conforming JSON",
		// 	method:         http.MethodGet,
		// 	path:           "/v2/repo/manifests/invalid",
		// 	expectedStatus: http.StatusBadRequest,
		// 	isManifest:     true,
		// 	checkOCIJSON:   true,
		// },
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, tc.path, nil)
			require.NoError(t, err)

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
			// Execute test case
			// Add your test implementation here
			// Example:
			// resp, err := client.Do(req)
			// if err != nil {
			//     t.Fatal(err)
			// }
			// if resp.StatusCode != tc.expectedStatus {
			//     t.Errorf("expected status %d, got %d", tc.expectedStatus, resp.StatusCode)
			// }
			//
			// if tc.checkOCIJSON {
			//     // Verify OCI-conforming JSON response
			// }
		})
	}

	// 			g.Specify("HEAD request to nonexistent blob should result in 404 response", func() {
	// 			g.Specify("HEAD request to existing blob should yield 200", func() {
	// 			g.Specify("GET nonexistent blob should result in 404 response", func() {
	// 			g.Specify("GET request to existing blob URL should yield 200", func() {
	// 			g.Specify("HEAD request to nonexistent manifest should return 404", func() {
	// 			g.Specify("HEAD request to manifest[0] path (digest) should yield 200 response", func() {
	// 			g.Specify("HEAD request to manifest[1] path (digest) should yield 200 response", func() {
	// 			g.Specify("HEAD request to manifest path (tag) should yield 200 response", func() {
	// 			g.Specify("GET nonexistent manifest should return 404", func() {
	// 			g.Specify("GET request to manifest[0] path (digest) should yield 200 response", func() {
	// 			g.Specify("GET request to manifest[1] path (digest) should yield 200 response", func() {
	// 			g.Specify("GET request to manifest path (tag) should yield 200 response", func() {
	// 			g.Specify("400 response body should contain OCI-conforming JSON message", func() {
}
