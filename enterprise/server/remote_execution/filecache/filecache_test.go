package filecache_test

import (
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func writeFile(t *testing.T, base string, path string, executable bool) {
	mod := fs.FileMode(0644)
	suffix := ""
	if executable {
		mod = 0755
		suffix = "-executable"
	}
	fullPath := filepath.Join(base, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0777); err != nil {
		t.Fatal(err)
	}
	log.Printf("Writing file %q", fullPath)
	if err := os.WriteFile(fullPath, []byte(path+suffix), mod); err != nil {
		t.Fatal(err)
	}
}

func TestFilecache(t *testing.T) {
	fcDir := testfs.MakeTempDir(t)
	// Create filecache
	fc, err := filecache.NewFileCache(fcDir, 100000)
	if err != nil {
		t.Fatal(err)
	}

	baseDir := testfs.MakeTempDir(t)
	// Write a non-executable file
	writeFile(t, baseDir, "my/fun/file", false)

	// Add a file and make sure we can hardlink it
	node := nodeFromString("my/fun/file", false)
	fc.AddFile(node, filepath.Join(baseDir, "my/fun/file"))
	linked := fc.FastLinkFile(node, filepath.Join(baseDir, "my/fun/fastlinked-file"))
	assert.True(t, linked, "existing file should link")
	assert.FileExists(t, filepath.Join(baseDir, "my/fun/fastlinked-file"))
	assertFileContents(t, filepath.Join(baseDir, "my/fun/fastlinked-file"), "my/fun/file")

	// Make sure that when we try to hardlink a non-existent file, it doesn't link
	nonexistentNode := nodeFromString("my/fun/nonexistentfile", false)
	notLinked := fc.FastLinkFile(nonexistentNode, filepath.Join(baseDir, "my/fun/fastlinked-nonexistentfile"))
	assert.False(t, notLinked, "nonexistet file should not link")
	assert.NoFileExists(t, filepath.Join(baseDir, "my/fun/fastlinked-nonexistentfile"))

	// Make sure that when we try to link the existing file as an executable file, it doesn't link
	executableNode := nodeFromString("my/fun/file", true)
	notLinkedExecutable := fc.FastLinkFile(executableNode, filepath.Join(baseDir, "my/fun/fastlinked-executablefile"))
	assert.False(t, notLinkedExecutable, "executable file should not link")
	assert.NoFileExists(t, filepath.Join(baseDir, "my/fun/fastlinked-executablefile"))

	// Replace the original file with an executable one
	os.Remove(filepath.Join(baseDir, "my/fun/file"))
	writeFile(t, baseDir, "my/fun/file", true)
	fc.AddFile(executableNode, filepath.Join(baseDir, "my/fun/file"))

	// Make sure the link now works
	linkedExecutable := fc.FastLinkFile(executableNode, filepath.Join(baseDir, "my/fun/fastlinked-executablefile"))
	assert.True(t, linkedExecutable, "executable file should link")
	assert.FileExists(t, filepath.Join(baseDir, "my/fun/fastlinked-executablefile"))
	assertFileContents(t, filepath.Join(baseDir, "my/fun/fastlinked-executablefile"), "my/fun/file-executable")

	// Make sure the original link still works
	secondLink := fc.FastLinkFile(node, filepath.Join(baseDir, "my/fun/second-fastlinkedfile"))
	assert.True(t, secondLink, "original file should still link")
	assert.FileExists(t, filepath.Join(baseDir, "my/fun/second-fastlinkedfile"))
	assertFileContents(t, filepath.Join(baseDir, "my/fun/second-fastlinkedfile"), "my/fun/file")
}

func TestFilecache_Overwrite(t *testing.T) {
	fcDir := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(fcDir, 100000)
	require.NoError(t, err)

	tmpDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, tmpDir, map[string]string{
		"file1.txt": "1",
		"file2.txt": "2",
	})

	key := &repb.FileNode{Digest: &repb.Digest{Hash: hash.String("foo"), SizeBytes: 100}}
	fc.AddFile(key, filepath.Join(tmpDir, "file1.txt"))
	link1 := fc.FastLinkFile(key, filepath.Join(tmpDir, "link1.txt"))
	assertFileContents(t, filepath.Join(tmpDir, "link1.txt"), "1")
	require.True(t, link1, "expected link to be successful")

	// Overwrite the entry with the same key
	fc.AddFile(key, filepath.Join(tmpDir, "file2.txt"))
	link2 := fc.FastLinkFile(key, filepath.Join(tmpDir, "link2.txt"))
	require.True(t, link2, "expected link to be successful")
	assertFileContents(t, filepath.Join(tmpDir, "link1.txt"), "1")
	assertFileContents(t, filepath.Join(tmpDir, "link2.txt"), "2")
	// The overwrite should not count as an eviction
	lastEvictionAge := testmetrics.GaugeValue(t, metrics.FileCacheLastEvictionAgeUsec)
	require.Equal(t, float64(0), lastEvictionAge, "last eviction age should still be 0")
}

func assertFileContents(t *testing.T, path, contents string) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, contents, string(bytes), "file contents should match")
}

func nodeFromString(s string, executable bool) *repb.FileNode {
	return &repb.FileNode{
		Digest: &repb.Digest{
			Hash:      hash.String(s),
			SizeBytes: int64(0),
		},
		IsExecutable: executable,
	}
}
