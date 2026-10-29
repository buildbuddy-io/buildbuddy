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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeArchive(t *testing.T) []byte {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	files := []struct {
		name string
		mode int64
		body string
	}{
		{"bin/ztracing", 0755, "#!/bin/sh\n"},
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
	tampered := makeArchive(t)
	tampered = append(tampered, 0)
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
	key := ztracingCommitSHA + "-" + ztracingSHA256["linux-amd64"][:16]
	// Installations from before digest verification are keyed by commit only.
	require.NoError(t, os.MkdirAll(filepath.Join(root, ztracingCommitSHA, "linux-amd64"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, key, "linux-amd64"), 0755))

	require.NoError(t, removeOldInstallations(root, key))

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, key, entries[0].Name())
}
