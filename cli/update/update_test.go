package update

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const binaryContents = "fake bb binary"

func shasumLine(contents, filename string) string {
	sum := sha256.Sum256([]byte(contents))
	return hex.EncodeToString(sum[:]) + "  " + filename + "\n"
}

func serve(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	}
}

func TestDownloadAndVerify(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request to http server: %s", r.URL)
	}))
	t.Cleanup(httpServer.Close)

	for _, tc := range []struct {
		name      string
		binary    http.HandlerFunc
		sha256    http.HandlerFunc
		httpStart bool
		wantError string
	}{
		{
			name:   "Success",
			binary: serve(binaryContents),
			sha256: serve(shasumLine(binaryContents, "bb")),
		},
		{
			name:      "ChecksumMismatch",
			binary:    serve(binaryContents),
			sha256:    serve(shasumLine("something else", "bb")),
			wantError: "checksum mismatch",
		},
		{
			name:      "ChecksumNotFound",
			binary:    serve(binaryContents),
			sha256:    http.NotFound,
			wantError: "404",
		},
		{
			name:      "BinaryNotFound",
			binary:    http.NotFound,
			sha256:    serve(shasumLine(binaryContents, "bb")),
			wantError: "404",
		},
		{
			name: "BinaryRedirectedToHTTP",
			binary: func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, httpServer.URL+r.URL.Path, http.StatusFound)
			},
			sha256:    serve(shasumLine(binaryContents, "bb")),
			wantError: "non-https",
		},
		{
			name:   "ChecksumExtraContentIgnored",
			binary: serve(binaryContents),
			sha256: serve(shasumLine(binaryContents, "other") + "extra line\n"),
		},
		{
			name:      "MalformedChecksum",
			binary:    serve(binaryContents),
			sha256:    serve("abc123  bb\n"),
			wantError: "malformed checksum",
		},
		{
			name:      "NonHTTPSStartURL",
			binary:    serve(binaryContents),
			sha256:    serve(shasumLine(binaryContents, "bb")),
			httpStart: true,
			wantError: "non-https",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/bb", tc.binary)
			mux.HandleFunc("/bb.sha256", tc.sha256)
			server := httptest.NewTLSServer(mux)
			t.Cleanup(server.Close)
			client := newDownloadClient()
			roots := x509.NewCertPool()
			roots.AddCert(server.Certificate())
			client.Transport.(*http.Transport).TLSClientConfig = &tls.Config{RootCAs: roots}
			url := server.URL + "/bb"
			if tc.httpStart {
				url = strings.Replace(url, "https://", "http://", 1)
			}
			dest := filepath.Join(t.TempDir(), "bb")

			err := downloadAndVerify(client, url, dest)

			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			b, err := os.ReadFile(dest)
			require.NoError(t, err)
			require.Equal(t, binaryContents, string(b))
		})
	}
}

func TestReleaseAssetURL(t *testing.T) {
	for _, tc := range []struct {
		goos, goarch string
		want         string
	}{
		{"linux", "amd64", releaseDownloadBaseURL + "/5.0.1/bazel-5.0.1-linux-x86_64"},
		{"linux", "arm64", releaseDownloadBaseURL + "/5.0.1/bazel-5.0.1-linux-arm64"},
		{"darwin", "amd64", releaseDownloadBaseURL + "/5.0.1/bazel-5.0.1-darwin-x86_64"},
		{"darwin", "arm64", releaseDownloadBaseURL + "/5.0.1/bazel-5.0.1-darwin-arm64"},
		{"windows", "amd64", ""},
	} {
		t.Run(tc.goos+"_"+tc.goarch, func(t *testing.T) {
			got, err := releaseAssetURL("5.0.1", tc.goos, tc.goarch)
			if tc.want == "" {
				require.ErrorContains(t, err, "Could not find a CLI release")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
