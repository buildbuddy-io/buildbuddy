package bazelisk

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/bazelbuild/bazelisk/config"
	"github.com/bazelbuild/bazelisk/core"
	"github.com/bazelbuild/bazelisk/httputil"
	"github.com/bazelbuild/bazelisk/platforms"

	pgperrors "github.com/ProtonMail/go-crypto/openpgp/errors"
)

// releasesBaseURL must match the URL that bazelisk's GCSRepo downloads
// releases from.
const releasesBaseURL = "https://releases.bazel.build"

// bazelReleaseKeyring holds only Bazel's release key.
var bazelReleaseKeyring = mustParseKeyring(bazelReleaseKey)

func mustParseKeyring(armored string) openpgp.EntityList {
	keyring, err := openpgp.ReadArmoredKeyRing(strings.NewReader(armored))
	if err != nil {
		panic(fmt.Sprintf("parse Bazel release key: %s", err))
	}
	return keyring
}

// releaseRepo is the part of bazelisk's GCSRepo that serves official Bazel
// releases, release candidates and rolling releases.
type releaseRepo interface {
	core.LTSRepo
	core.RollingRepo
}

// verifyingReleaseRepo wraps a releaseRepo and checks each downloaded binary
// against its detached GPG signature (<binary URL>.sig), which must be signed
// by Bazel's release key. This protects against a binary being swapped in
// transit, e.g. by an attacker who can intercept TLS.
//
// Known gaps:
//   - BAZELISK_BASE_URL and BAZELISK_FORMAT_URL downloads don't go through the
//     repos, so they are not verified.
//   - Binaries already in the bazelisk cache are reused without verification.
//     Upstream bazelisk shares the same cache, so binaries it downloaded
//     (unverified) are also reused, and cache hits are never re-verified.
//   - Bazel builds at a commit (including last_green) are unsigned, so the
//     commit repo isn't wrapped. Forks aren't wrapped here either.
//   - The signature covers only the binary's contents, not its version or
//     platform, so an attacker could serve a different validly-signed Bazel
//     release (e.g. an older one) in place of the requested one. Once
//     downloaded, that substitute sticks in bazelisk's metadata cache for the
//     requested version.
//   - Revocation checks only see revocations already present in the embedded
//     copy of the key.
//
// BAZELISK_VERIFY_SHA256 is still enforced by bazelisk itself.
type verifyingReleaseRepo struct {
	releaseRepo
	keyring  openpgp.KeyRing
	fetchSig func(url string) ([]byte, error)
}

func newVerifyingReleaseRepo(inner releaseRepo) *verifyingReleaseRepo {
	return &verifyingReleaseRepo{
		releaseRepo: inner,
		keyring:     bazelReleaseKeyring,
		fetchSig: func(url string) ([]byte, error) {
			b, _, err := httputil.ReadRemoteFile(url, "")
			return b, err
		},
	}
}

// ltsURL mirrors the URL that GCSRepo.DownloadLTS downloads from.
func ltsURL(version string, config config.Config) (string, error) {
	srcFile, err := platforms.DetermineBazelFilename(version, true, config)
	if err != nil {
		return "", err
	}
	baseVersion, folder := version, "release"
	if v, rc, ok := strings.Cut(version, "rc"); ok {
		baseVersion, folder = v, "rc"+rc
	}
	return fmt.Sprintf("%s/%s/%s/%s", releasesBaseURL, baseVersion, folder, srcFile), nil
}

// rollingURL mirrors the URL that GCSRepo.DownloadRolling downloads from.
func rollingURL(version string, config config.Config) (string, error) {
	srcFile, err := platforms.DetermineBazelFilename(version, true, config)
	if err != nil {
		return "", err
	}
	releaseVersion, _, _ := strings.Cut(version, "-")
	return fmt.Sprintf("%s/%s/rolling/%s/%s", releasesBaseURL, releaseVersion, version, srcFile), nil
}

func (r *verifyingReleaseRepo) DownloadLTS(version, destDir, destFile string, config config.Config) (string, error) {
	url, err := ltsURL(version, config)
	if err != nil {
		return "", err
	}
	return r.downloadVerified(url, func() (string, error) {
		return r.releaseRepo.DownloadLTS(version, destDir, destFile, config)
	})
}

func (r *verifyingReleaseRepo) DownloadRolling(version, destDir, destFile string, config config.Config) (string, error) {
	url, err := rollingURL(version, config)
	if err != nil {
		return "", err
	}
	return r.downloadVerified(url, func() (string, error) {
		return r.releaseRepo.DownloadRolling(version, destDir, destFile, config)
	})
}

// downloadVerified runs download, which fetches the binary at url, then
// verifies the result and deletes it if verification fails.
func (r *verifyingReleaseRepo) downloadVerified(url string, download func() (string, error)) (string, error) {
	path, err := download()
	if err != nil {
		return "", err
	}
	if err := r.verify(path, url); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("could not verify the signature of %s: %w", url, err)
	}
	return path, nil
}

func (r *verifyingReleaseRepo) verify(path, url string) error {
	sig, err := r.fetchSig(url + ".sig")
	if err != nil {
		return fmt.Errorf("fetch signature: %w", err)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return checkSignature(r.keyring, f, sig)
}

// checkSignature checks that sig is a valid binary detached signature of
// signed, made by a key in keyring.
func checkSignature(keyring openpgp.KeyRing, signed io.Reader, sig []byte) error {
	verified, _, err := openpgp.VerifyDetachedSignature(keyring, signed, bytes.NewReader(sig), nil)
	// A non-nil signature means the cryptographic check passed and err (if
	// any) comes from checking expiry and revocation as of the current time.
	// Ignore expiry: Bazel's release key may carry an expiry date that Bazel
	// extends from time to time, and an embedded copy of the key would then
	// start rejecting every download once its old expiry date passes. Checking
	// validity as of the signature's creation time doesn't work either: the
	// key's current self-signature is newer than old releases' signatures.
	// go-crypto checks revocation before expiry, so revocation is not ignored.
	if verified != nil && (errors.Is(err, pgperrors.ErrKeyExpired) || errors.Is(err, pgperrors.ErrSignatureExpired)) {
		return nil
	}
	return err
}
