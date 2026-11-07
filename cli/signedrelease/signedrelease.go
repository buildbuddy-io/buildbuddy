// Package signedrelease downloads bb CLI release binaries and verifies them
// against a signature made with the release signing key.
//
// Each release asset bazel-$VERSION-$OS-$ARCH is published alongside:
//   - bazel-$VERSION-$OS-$ARCH.sha256: `shasum -a 256` output, i.e. a single
//     line "<hex sha256>  bazel-$VERSION-$OS-$ARCH".
//   - bazel-$VERSION-$OS-$ARCH.sha256.sig: the raw 64-byte ed25519 signature
//     of the .sha256 file's bytes.
//
// The signature is checked against a public key built into bb, so a
// download is trusted even if the host serving it is not. The .sha256 file
// must name exactly the expected asset, so that a signed checksum for one
// version or platform can't be substituted for another.
package signedrelease

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	_ "embed"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
)

const (
	ReleaseDownloadBaseURL = "https://github.com/buildbuddy-io/bazel/releases/download"

	// FirstSignedVersion is the first bb release published with a .sig file.
	// It is set to the release after the latest one (cli-v5.0.498) at the
	// time signing was added. It MUST be the first release built and signed
	// with the real key (i.e. after release_signing_public_key.pem and the
	// CLI_RELEASE_SIGNING_KEY secret are set), so bump it if releases ship
	// before then.
	FirstSignedVersion = "5.0.499"
)

// publicKeyPEM holds the PEM-encoded ed25519 public key that release
// signatures are checked against.
//
// TODO(dan): the checked-in file is a placeholder, so verification fails
// until it is replaced. Generate the key pair with:
//
//	openssl genpkey -algorithm ed25519 -out release_signing_key.pem
//	openssl pkey -in release_signing_key.pem -pubout -out release_signing_public_key.pem
//
// then put release_signing_public_key.pem in this directory and store the
// contents of release_signing_key.pem in the CLI_RELEASE_SIGNING_KEY GitHub
// Actions secret.
//
//go:embed release_signing_public_key.pem
var publicKeyPEM []byte

func publicKey() (ed25519.PublicKey, error) {
	block, _ := pem.Decode(publicKeyPEM)
	if block == nil {
		return nil, errors.New("no release signing public key is built into this version of bb, so the download can't be verified")
	}
	k, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse release signing public key: %w", err)
	}
	pk, ok := k.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("release signing public key has type %T, want ed25519", k)
	}
	return pk, nil
}

// AssetURL returns the download URL for the CLI release binary for the given
// version, OS, and architecture.
func AssetURL(version, goos, goarch string) (string, error) {
	arch := goarch
	if goarch == "amd64" {
		arch = "x86_64"
	}
	if (goos != "linux" && goos != "darwin") || (arch != "x86_64" && arch != "arm64") {
		return "", fmt.Errorf("Could not find a CLI release for os '%s', arch '%s'", goos, goarch)
	}
	return fmt.Sprintf("%s/%s/bazel-%s-%s-%s", ReleaseDownloadBaseURL, version, version, goos, arch), nil
}

// NewDownloadClient returns an HTTP client with timeouts that refuses to
// follow redirects to non-https URLs.
func NewDownloadClient() *http.Client {
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment}
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = t.Clone()
	}
	transport.TLSHandshakeTimeout = 30 * time.Second
	transport.ResponseHeaderTimeout = 30 * time.Second
	return &http.Client{
		Transport:     transport,
		CheckRedirect: checkRedirect,
	}
}

func checkRedirect(req *http.Request, via []*http.Request) error {
	if req.URL.Scheme != "https" {
		return fmt.Errorf("refusing redirect to non-https URL %s", req.URL)
	}
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	return nil
}

// Download downloads the release asset at assetURL to dest, which must not
// exist yet, and verifies it against the signed checksum published next to
// it. On error, dest may be left behind with unverified contents.
func Download(client *http.Client, assetURL, dest string) error {
	if !strings.HasPrefix(assetURL, "https://") {
		return fmt.Errorf("refusing to download from non-https URL %s", assetURL)
	}
	expected, err := fetchVerifiedChecksum(client, assetURL)
	if err != nil {
		return err
	}

	res, err := Get(client, assetURL)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(f, h), res.Body); err != nil {
		f.Close()
		return fmt.Errorf("download %s: %s", assetURL, err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	return checkDigest(assetURL, expected, h.Sum(nil))
}

// VerifyFile verifies the already-downloaded file at path against the signed
// checksum published for the release asset at assetURL.
func VerifyFile(client *http.Client, assetURL, path string) error {
	if !strings.HasPrefix(assetURL, "https://") {
		return fmt.Errorf("refusing to fetch checksum from non-https URL %s", assetURL)
	}
	expected, err := fetchVerifiedChecksum(client, assetURL)
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	return checkDigest(assetURL, expected, h.Sum(nil))
}

func checkDigest(assetURL, expected string, sum []byte) error {
	if actual := hex.EncodeToString(sum); actual != expected {
		return fmt.Errorf("checksum mismatch for %s: expected %s, got %s", assetURL, expected, actual)
	}
	return nil
}

// fetchVerifiedChecksum fetches assetURL + ".sha256" and its signature, and
// returns the sha256 it lists for the asset once the signature and the
// listed filename check out.
func fetchVerifiedChecksum(client *http.Client, assetURL string) (string, error) {
	u, err := url.Parse(assetURL)
	if err != nil {
		return "", err
	}
	assetName := path.Base(u.Path)
	shaURL := assetURL + ".sha256"
	sigURL := shaURL + ".sig"
	sumFile, err := fetchSmall(client, shaURL)
	if err != nil {
		return "", err
	}
	sig, err := fetchSmall(client, sigURL)
	if err != nil {
		return "", fmt.Errorf("fetch release signature (releases before %s are unsigned): %w", FirstSignedVersion, err)
	}
	return verifyChecksumFile(sumFile, sig, assetName)
}

// verifyChecksumFile checks sig over sumFile, then returns the hex sha256
// that sumFile lists, provided it lists exactly one file, named assetName.
func verifyChecksumFile(sumFile, sig []byte, assetName string) (string, error) {
	pk, err := publicKey()
	if err != nil {
		return "", err
	}
	if len(sig) != ed25519.SignatureSize {
		return "", fmt.Errorf("invalid signature for %s.sha256: got %d bytes, want %d", assetName, len(sig), ed25519.SignatureSize)
	}
	if !ed25519.Verify(pk, sumFile, sig) {
		return "", fmt.Errorf("invalid signature for %s.sha256", assetName)
	}
	lines := strings.Split(strings.TrimSuffix(string(sumFile), "\n"), "\n")
	if len(lines) != 1 {
		return "", fmt.Errorf("signed checksum file for %s has %d lines, want 1", assetName, len(lines))
	}
	fields := strings.Fields(lines[0])
	if len(fields) != 2 {
		return "", fmt.Errorf("malformed signed checksum file for %s", assetName)
	}
	sum := strings.ToLower(fields[0])
	if _, err := hex.DecodeString(sum); err != nil || len(sum) != 2*sha256.Size {
		return "", fmt.Errorf("malformed checksum %q for %s", fields[0], assetName)
	}
	// shasum marks binary mode with a '*' before the filename.
	if name := strings.TrimPrefix(fields[1], "*"); name != assetName {
		return "", fmt.Errorf("signed checksum is for %q, not %q", name, assetName)
	}
	return sum, nil
}

func fetchSmall(client *http.Client, u string) ([]byte, error) {
	res, err := Get(client, u)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, 4096))
	if err != nil {
		return nil, fmt.Errorf("read %s: %s", u, err)
	}
	return b, nil
}

// Get fetches url, returning an error for non-2xx responses.
func Get(client *http.Client, url string) (*http.Response, error) {
	res, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		res.Body.Close()
		return nil, fmt.Errorf("GET %s: unexpected status %s", url, res.Status)
	}
	return res, nil
}

// ParseVersion parses a release version, which must be strict semver
// without a "v" prefix (e.g. "5.0.499").
func ParseVersion(v string) (*semver.Version, error) {
	sv, err := semver.StrictNewVersion(v)
	if err != nil {
		return nil, fmt.Errorf("invalid release version %q: %w", v, err)
	}
	return sv, nil
}

// IsSigned reports whether v is a valid release version at or after
// FirstSignedVersion.
func IsSigned(v string) bool {
	sv, err := ParseVersion(v)
	return err == nil && !sv.LessThan(semver.MustParse(FirstSignedVersion))
}
