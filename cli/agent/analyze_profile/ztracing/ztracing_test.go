package ztracing

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeArchive(t *testing.T) []byte {
	return makeArchiveWithBinary(t, "#!/bin/sh\n")
}

func makeArchiveWithBinary(t *testing.T, binary string) []byte {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	files := []struct {
		name string
		mode int64
		body string
	}{
		{"bin/ztracing", 0755, binary},
		{"skills/trace-analyzer/SKILL.md", 0644, "skill"},
		{"licenses/ztracing-LICENSE", 0644, "license"},
	}
	for _, f := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: f.name, Mode: f.mode, Size: int64(len(f.body)), Typeflag: tar.TypeReg}))
		_, err := tw.Write([]byte(f.body))
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())
	return buf.Bytes()
}

func serve(t *testing.T, body []byte) string {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(body)
	}))
	t.Cleanup(server.Close)
	return server.URL + "/ztracing.tar.gz"
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestDownloadAndExtract_MatchingHash(t *testing.T) {
	archive := makeArchive(t)
	tempDir := t.TempDir()
	dest := t.TempDir()

	err := downloadAndExtract(context.Background(), serve(t, archive), sha256Hex(archive), tempDir, dest)
	require.NoError(t, err)

	assert.True(t, isValidInstallation(installationAt(dest)))
	entries, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "temporary archive should be removed")
}

func TestDownloadAndExtract_TamperedArchive(t *testing.T) {
	archive := makeArchive(t)
	expected := sha256Hex(archive)
	tampered := append(bytes.Clone(archive), 0)
	tempDir := t.TempDir()
	dest := t.TempDir()

	err := downloadAndExtract(context.Background(), serve(t, tampered), expected, tempDir, dest)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sha256")

	entries, err := os.ReadDir(dest)
	require.NoError(t, err)
	assert.Empty(t, entries, "nothing should be extracted from a tampered archive")
	entries, err = os.ReadDir(tempDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "temporary archive should be removed")
}

func TestRemoveOldInstallations_RemovesUnverifiedInstallations(t *testing.T) {
	root := t.TempDir()
	key := installationKey()
	// Installations from before digest verification are keyed by commit only.
	require.NoError(t, os.MkdirAll(filepath.Join(root, ztracingCommitSHA, "linux-amd64"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, key, "linux-amd64"), 0755))

	require.NoError(t, removeOldInstallations(root, key))

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, key, entries[0].Name())
}

func TestSetup_DoesNotReuseLegacyInstallation(t *testing.T) {
	platform := runtime.GOOS + "-" + runtime.GOARCH
	if _, ok := ztracingSHA256[platform]; !ok {
		t.Skipf("ztracing is not available for %s", platform)
	}
	cacheDir := t.TempDir()
	t.Setenv("BUILDBUDDY_CACHE_DIR", cacheDir)
	t.Setenv("PATH", os.Getenv("PATH"))

	archive := makeArchiveWithBinary(t, "downloaded")
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if !strings.HasSuffix(r.URL.Path, "/ztracing-"+platform+".tar.gz") {
			http.NotFound(w, r)
			return
		}
		w.Write(archive)
	}))
	t.Cleanup(server.Close)

	oldURLFormat, oldSHA256 := ztracingURLFormat, ztracingSHA256[platform]
	ztracingURLFormat = server.URL + "/%s/ztracing-%s.tar.gz"
	ztracingSHA256[platform] = sha256Hex(archive)
	t.Cleanup(func() {
		ztracingURLFormat = oldURLFormat
		ztracingSHA256[platform] = oldSHA256
	})

	// A legacy install, keyed by commit only, that would pass validation.
	rootDir := filepath.Join(cacheDir, "tools", "ztracing")
	legacyDir := filepath.Join(rootDir, ztracingCommitSHA, platform)
	legacy := installationAt(legacyDir)
	require.NoError(t, os.MkdirAll(filepath.Dir(legacy.BinaryPath), 0755))
	require.NoError(t, os.WriteFile(legacy.BinaryPath, []byte("legacy"), 0755))
	require.NoError(t, os.MkdirAll(legacy.SkillDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(legacy.SkillDir, "SKILL.md"), []byte("skill"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Dir(legacy.LicensePath), 0755))
	require.NoError(t, os.WriteFile(legacy.LicensePath, []byte("license"), 0644))
	require.True(t, isValidInstallation(legacy))

	installation, err := Setup(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int32(1), requests.Load())
	assert.Equal(t, filepath.Join(rootDir, installationKey(), platform, "bin", "ztracing"), installation.BinaryPath)
	binary, err := os.ReadFile(installation.BinaryPath)
	require.NoError(t, err)
	assert.Equal(t, "downloaded", string(binary))
	_, err = os.Stat(filepath.Join(rootDir, ztracingCommitSHA))
	assert.True(t, os.IsNotExist(err), "legacy installation should be removed")
	entries, err := os.ReadDir(filepath.Join(rootDir, installationKey()))
	require.NoError(t, err)
	require.Len(t, entries, 1, "staging directory should be renamed into place")
	assert.Equal(t, platform, entries[0].Name())
}
