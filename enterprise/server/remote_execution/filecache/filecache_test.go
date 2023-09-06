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
	content := path
	if executable {
		content = path + "-executable"
	}
	writeFileContent(t, base, path, content, executable)
}

func writeFileContent(t *testing.T, base, path, content string, executable bool) {
	mod := fs.FileMode(0644)
	if executable {
		mod = 0755
	}
	fullPath := filepath.Join(base, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0777); err != nil {
		t.Fatal(err)
	}
	log.Printf("Writing file %q", fullPath)
	if err := os.WriteFile(fullPath, []byte(content), mod); err != nil {
		t.Fatal(err)
	}
}

func TestFilecache(t *testing.T) {
	fcDir := testfs.MakeTempDir(t)
	// Create filecache
	fc, err := filecache.NewFileCache(fcDir, 100000, false)
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

func TestFileCacheOverwrite(t *testing.T) {
	for _, test := range []struct {
		Name       string
		Executable bool
	}{
		{Name: "OverwriteExecutableFile", Executable: true},
		{Name: "OverwriteNormalFile", Executable: false},
	} {
		t.Run(test.Name, func(t *testing.T) {
			filecacheRoot := testfs.MakeTempDir(t)
			fc, err := filecache.NewFileCache(filecacheRoot, 10_000_000, false)
			require.NoError(t, err)
			fc.WaitForDirectoryScanToComplete()
			tempDir := fc.TempDir()

			writeFileContent(t, tempDir, "file1", "file1-content", test.Executable)
			node := nodeFromString("file1-content", test.Executable)
			{
				// Add node for the first time
				fc.AddFile(node, filepath.Join(tempDir, "file1"))
				linked := fc.FastLinkFile(node, filepath.Join(tempDir, "file1-link1"))
				require.True(t, linked, "expected cache hit after writing file")
				// Verify all file contents
				file1Content := testfs.ReadFileAsString(t, tempDir, "file1")
				file1Link1Content := testfs.ReadFileAsString(t, tempDir, "file1-link1")
				require.Equal(t, "file1-content", file1Content)
				require.Equal(t, "file1-content", file1Link1Content)
			}
			{
				// Overwrite existing node entry
				fc.AddFile(node, filepath.Join(tempDir, "file1"))
				linked := fc.FastLinkFile(node, filepath.Join(tempDir, "file1-link2"))
				require.True(t, linked, "expected cache hit after overwriting file")
				// Verify all file contents
				file1Content := testfs.ReadFileAsString(t, tempDir, "file1")
				file1Link1Content := testfs.ReadFileAsString(t, tempDir, "file1-link1")
				file1Link2Content := testfs.ReadFileAsString(t, tempDir, "file1-link2")
				require.Equal(t, "file1-content", file1Content)
				require.Equal(t, "file1-content", file1Link1Content)
				require.Equal(t, "file1-content", file1Link2Content)
			}
			{
				// Overwrite again, this time the contents don't match the
				// digest (filecache supports this, similar to AC).
				writeFileContent(t, tempDir, "file2", "file2-content", test.Executable)
				fc.AddFile(node, filepath.Join(tempDir, "file2"))
				linked := fc.FastLinkFile(node, filepath.Join(tempDir, "file2-link1"))
				require.True(t, linked, "expected cache hit after overwriting file with different digest")
				// Verify all file contents
				file1Content := testfs.ReadFileAsString(t, tempDir, "file1")
				file1Link1Content := testfs.ReadFileAsString(t, tempDir, "file1-link1")
				file1Link2Content := testfs.ReadFileAsString(t, tempDir, "file1-link2")
				file2Content := testfs.ReadFileAsString(t, tempDir, "file2")
				file2Link1Content := testfs.ReadFileAsString(t, tempDir, "file2-link1")
				require.Equal(t, "file1-content", file1Content)
				require.Equal(t, "file1-content", file1Link1Content)
				require.Equal(t, "file1-content", file1Link2Content)
				require.Equal(t, "file2-content", file2Content)
				require.Equal(t, "file2-content", file2Link1Content)
			}

			// No evictions should have occurred.
			lastEvictionAge := testmetrics.GaugeValue(t, metrics.FileCacheLastEvictionAgeUsec)
			require.Equal(t, float64(0), lastEvictionAge, "last eviction age should still be 0 - no evictions should have occurred")
		})
	}
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
