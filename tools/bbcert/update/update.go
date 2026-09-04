// Package update keeps an installed bbcert current with the latest version.
package update

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// baseURL is where binaries are published.
var baseURL string

// BaseURL returns the stamped manifest location, or "" if this build was
// not stamped with one.
func BaseURL() string {
	if baseURL == "" || strings.HasPrefix(baseURL, "{") {
		return ""
	}
	return strings.TrimRight(baseURL, "/")
}

// publicKeyPEM is the public key used to verify the bbcert binaries.
const publicKeyPEM = `
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAu3vdQ3oRAqhLQf11lmAQlLUn0zI13s7YT2a8alKYpAg=
-----END PUBLIC KEY-----
`

// commitSHA is stamped at link time.
var commitSHA string

// Commit returns the commit this binary was built from, or "" if it was not
// stamped.
func Commit() string {
	if commitSHA == "" || strings.HasPrefix(commitSHA, "{") || commitSHA == "dev" || commitSHA == "unknown" {
		return ""
	}
	return commitSHA
}

// Version describes this binary for `bbcert version`.
func Version() string {
	commit := Commit()
	if commit == "" {
		commit = "unstamped development build"
	}
	return fmt.Sprintf("bbcert %s (%s, %s)", commit, Platform(), runtime.Version())
}

// Platform is the manifest key for the running binary, e.g. "darwin-arm64".
func Platform() string { return runtime.GOOS + "-" + runtime.GOARCH }

// Manifest is what the publish workflow writes.
type Manifest struct {
	Commit      string `json:"commit"`
	PublishedAt string `json:"published_at"`
	// SHA256 is the hex digest of each platform's binary, by platform key.
	SHA256 map[string]string `json:"sha256"`
}

// BinaryURL is the platform's binary URL for this manifest's
// commit: <base>/<commit>/bbcert-<platform>.
func (u *Updater) BinaryURL(m *Manifest, platform string) string {
	return u.BaseURL + "/" + m.Commit + "/bbcert-" + platform
}

// Updater fetches manifests and applies them to an executable.
type Updater struct {
	BaseURL   string
	PublicKey ed25519.PublicKey
	// Executable is the file to replace. Empty means this process's binary.
	Executable string
	Platform   string
	Client     *http.Client
}

// Default returns the Updater for the running binary, or an error if this
// build has no manifest location or signing key configured.
func Default() (*Updater, error) {
	base := BaseURL()
	if base == "" {
		return nil, errors.New("update location is not stamped into this build")
	}
	pub, err := parsePublicKey(publicKeyPEM)
	if err != nil {
		return nil, err
	}
	return &Updater{
		BaseURL:   base,
		PublicKey: pub,
		Platform:  Platform(),
		Client:    &http.Client{Timeout: 2 * time.Minute},
	}, nil
}

func parsePublicKey(pemText string) (ed25519.PublicKey, error) {
	block, _ := pem.Decode([]byte(pemText))
	if block == nil {
		return nil, errors.New("update signing key is not configured in this build")
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("update signing key: %w", err)
	}
	pub, ok := key.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("update signing key is %T, want ed25519", key)
	}
	return pub, nil
}

// Latest fetches the manifest of what is currently published.
func (u *Updater) Latest(ctx context.Context) (*Manifest, error) {
	return u.fetchManifest(ctx, u.BaseURL+"/latest.json")
}

// ForCommit fetches the manifest of a specific published commit, whether or
// not it is what "latest" points at.
func (u *Updater) ForCommit(ctx context.Context, commit string) (*Manifest, error) {
	return u.fetchManifest(ctx, u.BaseURL+"/"+commit+"/manifest.json")
}

func (u *Updater) fetchManifest(ctx context.Context, url string) (*Manifest, error) {
	body, err := u.get(ctx, url)
	if err != nil {
		return nil, err
	}
	sig, err := u.get(ctx, url+".sig")
	if err != nil {
		return nil, err
	}
	if err := verify(u.PublicKey, body, sig); err != nil {
		return nil, fmt.Errorf("%s: %w", url, err)
	}
	m := &Manifest{}
	if err := json.Unmarshal(body, m); err != nil {
		return nil, fmt.Errorf("%s: %w", url, err)
	}
	if m.Commit == "" {
		return nil, fmt.Errorf("%s: manifest names no commit", url)
	}
	return m, nil
}

// verify checks the detached signature over the exact manifest bytes.
func verify(pub ed25519.PublicKey, manifest, sigB64 []byte) error {
	sig, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(sigB64)))
	if err != nil {
		return fmt.Errorf("signature is not base64: %w", err)
	}
	if !ed25519.Verify(pub, manifest, sig) {
		return errors.New("manifest signature does not verify")
	}
	return nil
}

// Apply replaces the executable with the manifest's binary for this platform.
// The download is written beside the executable and renamed over it.
func (u *Updater) Apply(ctx context.Context, m *Manifest) error {
	digest, ok := m.SHA256[u.Platform]
	if !ok {
		return fmt.Errorf("commit %s was not published for %s", m.Commit, u.Platform)
	}
	want, err := hex.DecodeString(digest)
	if err != nil || len(want) != sha256.Size {
		return fmt.Errorf("manifest has a malformed sha256 for %s", u.Platform)
	}
	url := u.BinaryURL(m, u.Platform)

	exe, err := u.executable()
	if err != nil {
		return err
	}
	dir, base := filepath.Split(exe)
	tmp, err := os.CreateTemp(dir, "."+base+".update-*")
	if err != nil {
		return fmt.Errorf("cannot write next to %s (try with sudo): %w", exe, err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // a no-op once renamed

	body, err := u.open(ctx, url)
	if err != nil {
		tmp.Close()
		return err
	}
	defer body.Close()
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(tmp, h), body); err != nil {
		tmp.Close()
		return fmt.Errorf("downloading %s: %w", url, err)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if got := h.Sum(nil); string(got) != string(want) {
		return fmt.Errorf("%s does not match the digest in the manifest (got %x)", url, got)
	}
	if err := os.Chmod(tmpPath, 0o755); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, exe); err != nil {
		return fmt.Errorf("replacing %s (try with sudo): %w", exe, err)
	}
	return nil
}

func (u *Updater) executable() (string, error) {
	if u.Executable != "" {
		return u.Executable, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	// Replace the file, not a symlink to it.
	return filepath.EvalSymlinks(exe)
}

func (u *Updater) get(ctx context.Context, url string) ([]byte, error) {
	body, err := u.open(ctx, url)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	return io.ReadAll(io.LimitReader(body, 1<<20))
}

func (u *Updater) open(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	rsp, err := u.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if rsp.StatusCode != http.StatusOK {
		rsp.Body.Close()
		return nil, fmt.Errorf("GET %s: %s", url, rsp.Status)
	}
	return rsp.Body, nil
}
