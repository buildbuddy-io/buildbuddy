package bazelisk

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ProtonMail/go-crypto/openpgp/packet"
	"github.com/bazelbuild/bazelisk/config"
	"github.com/bazelbuild/bazelisk/core"
	"github.com/stretchr/testify/require"

	pgperrors "github.com/ProtonMail/go-crypto/openpgp/errors"
)

// fakeReleaseRepo "downloads" a fixed binary by writing it to destDir/destFile.
type fakeReleaseRepo struct {
	core.LTSRepo
	core.RollingRepo
	contents []byte
}

func (f *fakeReleaseRepo) download(destDir, destFile string) (string, error) {
	path := filepath.Join(destDir, destFile)
	return path, os.WriteFile(path, f.contents, 0755)
}

func (f *fakeReleaseRepo) DownloadLTS(version, destDir, destFile string, config config.Config) (string, error) {
	return f.download(destDir, destFile)
}

func (f *fakeReleaseRepo) DownloadRolling(version, destDir, destFile string, config config.Config) (string, error) {
	return f.download(destDir, destFile)
}

func newTestEntity(t *testing.T, cfg *packet.Config) *openpgp.Entity {
	e, err := openpgp.NewEntity("Test", "", "test@example.com", cfg)
	require.NoError(t, err)
	return e
}

func sign(t *testing.T, signer *openpgp.Entity, contents []byte, cfg *packet.Config) []byte {
	var buf bytes.Buffer
	require.NoError(t, openpgp.DetachSign(&buf, signer, bytes.NewReader(contents), cfg))
	return buf.Bytes()
}

// newTestRepo returns a repo that serves binary, trusts only trusted, and
// serves sigs[url] as the signature at url.
func newTestRepo(binary []byte, trusted *openpgp.Entity, sigs map[string][]byte) *verifyingReleaseRepo {
	return &verifyingReleaseRepo{
		releaseRepo: &fakeReleaseRepo{contents: binary},
		keyring:     openpgp.EntityList{trusted},
		fetchSig: func(url string) ([]byte, error) {
			sig, ok := sigs[url]
			if !ok {
				return nil, fmt.Errorf("unexpected status code while reading %s: 404", url)
			}
			return sig, nil
		},
	}
}

var testConfig = config.Static(map[string]string{})

// sigURL returns the signature URL for an LTS version on this platform.
func sigURL(t *testing.T, version, folder string) string {
	path, err := ltsURL(version, testConfig)
	require.NoError(t, err)
	require.Contains(t, path, "/"+folder+"/")
	return path + ".sig"
}

func TestDownloadLTS_GoodSignature(t *testing.T) {
	key := newTestEntity(t, nil)
	binary := []byte("bazel binary")
	repo := newTestRepo(binary, key, map[string][]byte{
		sigURL(t, "8.4.2", "release"): sign(t, key, binary, nil),
	})

	dir := t.TempDir()
	path, err := repo.DownloadLTS("8.4.2", dir, "bazel", testConfig)
	require.NoError(t, err)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, binary, got)
}

func TestDownloadLTS_ReleaseCandidateURL(t *testing.T) {
	key := newTestEntity(t, nil)
	binary := []byte("bazel binary")
	url := sigURL(t, "9.0.0rc1", "rc1")
	require.True(t, strings.HasPrefix(url, "https://releases.bazel.build/9.0.0/rc1/bazel-9.0.0rc1-"), url)
	repo := newTestRepo(binary, key, map[string][]byte{url: sign(t, key, binary, nil)})

	_, err := repo.DownloadLTS("9.0.0rc1", t.TempDir(), "bazel", testConfig)
	require.NoError(t, err)
}

func TestDownloadRolling_GoodSignature(t *testing.T) {
	key := newTestEntity(t, nil)
	binary := []byte("bazel binary")
	version := "9.0.0-pre.20250101.1"
	url, err := rollingURL(version, testConfig)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(url, "https://releases.bazel.build/9.0.0/rolling/"+version+"/bazel-"+version+"-"), url)
	repo := newTestRepo(binary, key, map[string][]byte{url + ".sig": sign(t, key, binary, nil)})

	_, err = repo.DownloadRolling(version, t.TempDir(), "bazel", testConfig)
	require.NoError(t, err)
}

func TestDownloadLTS_TamperedBinary(t *testing.T) {
	key := newTestEntity(t, nil)
	repo := newTestRepo([]byte("evil binary"), key, map[string][]byte{
		sigURL(t, "8.4.2", "release"): sign(t, key, []byte("bazel binary"), nil),
	})

	dir := t.TempDir()
	_, err := repo.DownloadLTS("8.4.2", dir, "bazel", testConfig)
	require.Error(t, err)
	require.NoFileExists(t, filepath.Join(dir, "bazel"))
}

func TestDownloadLTS_MissingSignature(t *testing.T) {
	key := newTestEntity(t, nil)
	repo := newTestRepo([]byte("bazel binary"), key, nil)

	dir := t.TempDir()
	_, err := repo.DownloadLTS("8.4.2", dir, "bazel", testConfig)
	require.ErrorContains(t, err, "404")
	require.NoFileExists(t, filepath.Join(dir, "bazel"))
}

func TestDownloadLTS_UntrustedSigner(t *testing.T) {
	trusted := newTestEntity(t, nil)
	other := newTestEntity(t, nil)
	binary := []byte("bazel binary")
	repo := newTestRepo(binary, trusted, map[string][]byte{
		sigURL(t, "8.4.2", "release"): sign(t, other, binary, nil),
	})

	dir := t.TempDir()
	_, err := repo.DownloadLTS("8.4.2", dir, "bazel", testConfig)
	require.Error(t, err)
	require.NoFileExists(t, filepath.Join(dir, "bazel"))
}

func TestDownloadLTS_ExpiredKeyStillVerifies(t *testing.T) {
	past := time.Now().Add(-48 * time.Hour)
	cfg := &packet.Config{
		Time:            func() time.Time { return past },
		KeyLifetimeSecs: 3600,
	}
	key := newTestEntity(t, cfg)
	binary := []byte("bazel binary")
	sig := sign(t, key, binary, cfg)

	// Sanity check: go-crypto itself rejects the key as expired.
	_, _, err := openpgp.VerifyDetachedSignature(openpgp.EntityList{key}, bytes.NewReader(binary), bytes.NewReader(sig), nil)
	require.True(t, errors.Is(err, pgperrors.ErrKeyExpired), "got %v", err)

	repo := newTestRepo(binary, key, map[string][]byte{sigURL(t, "8.4.2", "release"): sig})
	_, err = repo.DownloadLTS("8.4.2", t.TempDir(), "bazel", testConfig)
	require.NoError(t, err)
}

func TestEmbeddedReleaseKey(t *testing.T) {
	require.Len(t, bazelReleaseKeyring, 1)
	require.Equal(t, "71A1D0EFCFEB6281FD0437C93D5919B448457EE0", fmt.Sprintf("%X", bazelReleaseKeyring[0].PrimaryKey.Fingerprint))
}
