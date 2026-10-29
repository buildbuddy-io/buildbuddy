package signedrelease

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	assetName      = "bazel-5.0.499-linux-x86_64"
	binaryContents = "fake bb binary"
)

// useTestKey makes verification use a freshly generated key pair for the
// duration of the test, and returns its private key.
func useTestKey(t *testing.T) ed25519.PrivateKey {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	der, err := x509.MarshalPKIXPublicKey(pub)
	require.NoError(t, err)
	orig := publicKeyPEM
	publicKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	t.Cleanup(func() { publicKeyPEM = orig })
	return priv
}

func shasumLine(contents, filename string) string {
	sum := sha256.Sum256([]byte(contents))
	return hex.EncodeToString(sum[:]) + "  " + filename + "\n"
}

func serve(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	}
}

func sign(priv ed25519.PrivateKey, s string) string {
	return string(ed25519.Sign(priv, []byte(s)))
}

func TestDownload(t *testing.T) {
	priv := useTestKey(t)
	_, otherPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request to http server: %s", r.URL)
	}))
	t.Cleanup(httpServer.Close)

	goodSum := shasumLine(binaryContents, assetName)
	for _, tc := range []struct {
		name      string
		binary    http.HandlerFunc
		sha256    http.HandlerFunc
		sig       http.HandlerFunc
		httpStart bool
		wantError string
	}{
		{
			name:   "Success",
			binary: serve(binaryContents),
			sha256: serve(goodSum),
			sig:    serve(sign(priv, goodSum)),
		},
		{
			name:   "BinaryModeShasum",
			binary: serve(binaryContents),
			sha256: serve(strings.Replace(goodSum, "  ", " *", 1)),
			sig:    serve(sign(priv, strings.Replace(goodSum, "  ", " *", 1))),
		},
		{
			name:      "TamperedBinary",
			binary:    serve("evil bb binary"),
			sha256:    serve(goodSum),
			sig:       serve(sign(priv, goodSum)),
			wantError: "checksum mismatch",
		},
		{
			name:      "TamperedChecksum",
			binary:    serve("evil bb binary"),
			sha256:    serve(shasumLine("evil bb binary", assetName)),
			sig:       serve(sign(priv, goodSum)),
			wantError: "invalid signature",
		},
		{
			name:      "SignedByOtherKey",
			binary:    serve(binaryContents),
			sha256:    serve(goodSum),
			sig:       serve(sign(otherPriv, goodSum)),
			wantError: "invalid signature",
		},
		{
			name:      "TruncatedSignature",
			binary:    serve(binaryContents),
			sha256:    serve(goodSum),
			sig:       serve(sign(priv, goodSum)[:63]),
			wantError: "invalid signature",
		},
		{
			name:      "MissingSignature",
			binary:    serve(binaryContents),
			sha256:    serve(goodSum),
			sig:       http.NotFound,
			wantError: "releases before 5.0.499 are unsigned",
		},
		{
			// A validly signed checksum for some other version or platform.
			name:      "FilenameMismatch",
			binary:    serve(binaryContents),
			sha256:    serve(shasumLine(binaryContents, "bazel-5.0.499-darwin-arm64")),
			sig:       serve(sign(priv, shasumLine(binaryContents, "bazel-5.0.499-darwin-arm64"))),
			wantError: "signed checksum is for",
		},
		{
			name:      "ExtraEntries",
			binary:    serve(binaryContents),
			sha256:    serve(goodSum + shasumLine(binaryContents, "other")),
			sig:       serve(sign(priv, goodSum+shasumLine(binaryContents, "other"))),
			wantError: "has 2 lines",
		},
		{
			name:      "MalformedChecksum",
			binary:    serve(binaryContents),
			sha256:    serve("abc123  " + assetName + "\n"),
			sig:       serve(sign(priv, "abc123  "+assetName+"\n")),
			wantError: "malformed checksum",
		},
		{
			name:      "ChecksumNotFound",
			binary:    serve(binaryContents),
			sha256:    http.NotFound,
			sig:       serve(sign(priv, goodSum)),
			wantError: "404",
		},
		{
			name:      "BinaryNotFound",
			binary:    http.NotFound,
			sha256:    serve(goodSum),
			sig:       serve(sign(priv, goodSum)),
			wantError: "404",
		},
		{
			name: "BinaryRedirectedToHTTP",
			binary: func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, httpServer.URL+r.URL.Path, http.StatusFound)
			},
			sha256:    serve(goodSum),
			sig:       serve(sign(priv, goodSum)),
			wantError: "non-https",
		},
		{
			name:      "NonHTTPSStartURL",
			binary:    serve(binaryContents),
			sha256:    serve(goodSum),
			sig:       serve(sign(priv, goodSum)),
			httpStart: true,
			wantError: "non-https",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url, client := startServer(t, tc.binary, tc.sha256, tc.sig)
			if tc.httpStart {
				url = strings.Replace(url, "https://", "http://", 1)
			}
			dest := filepath.Join(t.TempDir(), "bb")

			err := Download(client, url, dest)

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

func TestVerifyFile(t *testing.T) {
	priv := useTestKey(t)
	goodSum := shasumLine(binaryContents, assetName)
	url, client := startServer(t, serve("unused"), serve(goodSum), serve(sign(priv, goodSum)))

	good := filepath.Join(t.TempDir(), "good")
	require.NoError(t, os.WriteFile(good, []byte(binaryContents), 0600))
	require.NoError(t, VerifyFile(client, url, good))

	bad := filepath.Join(t.TempDir(), "bad")
	require.NoError(t, os.WriteFile(bad, []byte("evil bb binary"), 0600))
	require.ErrorContains(t, VerifyFile(client, url, bad), "checksum mismatch")
}

func TestPlaceholderKeyFailsClosed(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	orig := publicKeyPEM
	publicKeyPEM = []byte("PLACEHOLDER\n")
	t.Cleanup(func() { publicKeyPEM = orig })
	goodSum := shasumLine(binaryContents, assetName)

	_, err = verifyChecksumFile([]byte(goodSum), []byte(sign(priv, goodSum)), assetName)

	require.ErrorContains(t, err, "no release signing public key")
}

func startServer(t *testing.T, binary, sha256, sig http.HandlerFunc) (string, *http.Client) {
	mux := http.NewServeMux()
	mux.HandleFunc("/5.0.499/"+assetName, binary)
	mux.HandleFunc("/5.0.499/"+assetName+".sha256", sha256)
	mux.HandleFunc("/5.0.499/"+assetName+".sha256.sig", sig)
	server := httptest.NewTLSServer(mux)
	t.Cleanup(server.Close)
	client := NewDownloadClient()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	client.Transport.(*http.Transport).TLSClientConfig = &tls.Config{RootCAs: roots}
	return server.URL + "/5.0.499/" + assetName, client
}

func TestAssetURL(t *testing.T) {
	for _, tc := range []struct {
		goos, goarch string
		want         string
	}{
		{"linux", "amd64", ReleaseDownloadBaseURL + "/5.0.1/bazel-5.0.1-linux-x86_64"},
		{"linux", "arm64", ReleaseDownloadBaseURL + "/5.0.1/bazel-5.0.1-linux-arm64"},
		{"darwin", "amd64", ReleaseDownloadBaseURL + "/5.0.1/bazel-5.0.1-darwin-x86_64"},
		{"darwin", "arm64", ReleaseDownloadBaseURL + "/5.0.1/bazel-5.0.1-darwin-arm64"},
		{"windows", "amd64", ""},
	} {
		t.Run(tc.goos+"_"+tc.goarch, func(t *testing.T) {
			got, err := AssetURL("5.0.1", tc.goos, tc.goarch)
			if tc.want == "" {
				require.ErrorContains(t, err, "Could not find a CLI release")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestCompareVersions(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want int
	}{
		{"5.0.499", "5.0.499", 0},
		{"5.0.498", "5.0.499", -1},
		{"5.0.1000", "5.0.999", 1},
		{"6.0.0", "5.9.9", 1},
		{"4.10.0", "5.0.0", -1},
	} {
		got, err := CompareVersions(tc.a, tc.b)
		require.NoError(t, err)
		require.Equal(t, tc.want, got, "CompareVersions(%q, %q)", tc.a, tc.b)
	}
	for _, bad := range []string{"", "5.0", "5.0.1.2", "v5.0.1", "5.0.x", "5.0.+1", "../5.0.1"} {
		_, err := CompareVersions(bad, "5.0.0")
		require.Error(t, err, "CompareVersions(%q, ...)", bad)
	}
}
