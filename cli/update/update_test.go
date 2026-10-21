package update

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const binaryContents = "fake bb binary"

func shasumLine(contents string) string {
	sum := sha256.Sum256([]byte(contents))
	return hex.EncodeToString(sum[:]) + "  bazel-1.0.0-linux-x86_64\n"
}

// Returns a TLS test server and a download client that trusts it and uses
// the production redirect policy.
func newTestServer(t *testing.T, handler http.Handler) (*httptest.Server, *http.Client) {
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	client := server.Client()
	client.CheckRedirect = newDownloadClient().CheckRedirect
	return server, client
}

func TestDownloadAndVerify(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sha256    string
		wantError string
	}{
		{name: "Success", sha256: shasumLine(binaryContents)},
		{name: "ChecksumMismatch", sha256: shasumLine("something else"), wantError: "checksum mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/bb", func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(binaryContents))
			})
			mux.HandleFunc("/bb.sha256", func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(tc.sha256))
			})
			server, client := newTestServer(t, mux)
			dest := filepath.Join(t.TempDir(), "bb")

			err := downloadAndVerify(client, server.URL+"/bb", dest)

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

func TestDownloadAndVerify_Non2xx(t *testing.T) {
	server, client := newTestServer(t, http.NotFoundHandler())

	err := downloadAndVerify(client, server.URL+"/bb", filepath.Join(t.TempDir(), "bb"))

	require.ErrorContains(t, err, "404")
}

func TestDownloadAndVerify_RejectsRedirectToHTTP(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request to http server: %s", r.URL)
	}))
	t.Cleanup(httpServer.Close)
	server, client := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, httpServer.URL+r.URL.Path, http.StatusFound)
	}))

	err := downloadAndVerify(client, server.URL+"/bb", filepath.Join(t.TempDir(), "bb"))

	require.ErrorContains(t, err, "non-https")
}
