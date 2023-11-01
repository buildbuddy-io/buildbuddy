package filecache_test

import (
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

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
	err = fc.AddFile(node, filepath.Join(baseDir, "my/fun/file"))
	require.NoError(t, err)
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
	err = fc.AddFile(executableNode, filepath.Join(baseDir, "my/fun/file"))
	require.NoError(t, err)

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
				err = fc.AddFile(node, filepath.Join(tempDir, "file1"))
				require.NoError(t, err)
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
				err = fc.AddFile(node, filepath.Join(tempDir, "file1"))
				require.NoError(t, err)
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
				err = fc.AddFile(node, filepath.Join(tempDir, "file2"))
				require.NoError(t, err)
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

func BenchmarkFilecacheLink(b *testing.B) {
	flags.Set(b, "app.log_level", "warn")
	log.Configure()

	for _, test := range []struct {
		Name         string
		Ops          int
		ReadFraction float64
	}{
		{Name: "100%Link/0%Add/5K", Ops: 5000, ReadFraction: 1},
		{Name: "95%Link/5%Add/5K", Ops: 5000, ReadFraction: 0.95},
	} {
		b.Run(test.Name, func(b *testing.B) {
			root := testfs.MakeTempDir(b)
			fc, err := filecache.NewFileCache(root, 1_000_000, false /*=delete*/)
			require.NoError(b, err)
			fc.WaitForDirectoryScanToComplete()
			tmp := fc.TempDir()

			// Create 1K small files
			g := digest.RandomGenerator(0)
			var nodes []*repb.FileNode
			for i := 0; i < test.Ops; i++ {
				d, buf, err := g.RandomDigestBuf(20)
				require.NoError(b, err)
				name := fmt.Sprint(i)
				path := filepath.Join(tmp, name)
				err = os.WriteFile(path, buf, 0644)
				require.NoError(b, err)
				node := &repb.FileNode{
					Name:   name,
					Digest: d,
				}
				nodes = append(nodes, node)
				err = fc.AddFile(node, path)
				require.NoError(b, err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Fast link all of the files we created
				out := testfs.MakeTempDir(b)
				eg := &errgroup.Group{}
				eg.SetLimit(100)
				for _, node := range nodes {
					node := node
					eg.Go(func() error {
						if rand.Float64() > test.ReadFraction {
							err := fc.AddFile(node, filepath.Join(tmp, node.GetName()))
							if err != nil {
								require.FailNowf(b, "fast link failed", "%s", err)
							}
							return nil
						}

						ok := fc.FastLinkFile(node, filepath.Join(out, fmt.Sprint(node.GetName())))
						if !ok {
							require.FailNowf(b, "link failed", "")
						}
						return nil
					})
				}
				eg.Wait()
			}
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
