package filecache_test

import (
	"context"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
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
	ctx := context.TODO()
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
	err = fc.AddFile(ctx, node, filepath.Join(baseDir, "my/fun/file"))
	require.NoError(t, err)
	linked := fc.FastLinkFile(ctx, node, filepath.Join(baseDir, "my/fun/fastlinked-file"))
	assert.True(t, linked, "existing file should link")
	assert.FileExists(t, filepath.Join(baseDir, "my/fun/fastlinked-file"))
	assertFileContents(t, filepath.Join(baseDir, "my/fun/fastlinked-file"), "my/fun/file")

	// Make sure that when we try to hardlink a non-existent file, it doesn't link
	nonexistentNode := nodeFromString("my/fun/nonexistentfile", false)
	notLinked := fc.FastLinkFile(ctx, nonexistentNode, filepath.Join(baseDir, "my/fun/fastlinked-nonexistentfile"))
	assert.False(t, notLinked, "nonexistet file should not link")
	assert.NoFileExists(t, filepath.Join(baseDir, "my/fun/fastlinked-nonexistentfile"))

	// Make sure that when we try to link the existing file as an executable file, it doesn't link
	executableNode := nodeFromString("my/fun/file", true)
	notLinkedExecutable := fc.FastLinkFile(ctx, executableNode, filepath.Join(baseDir, "my/fun/fastlinked-executablefile"))
	assert.False(t, notLinkedExecutable, "executable file should not link")
	assert.NoFileExists(t, filepath.Join(baseDir, "my/fun/fastlinked-executablefile"))

	// Replace the original file with an executable one
	os.Remove(filepath.Join(baseDir, "my/fun/file"))
	writeFile(t, baseDir, "my/fun/file", true)
	err = fc.AddFile(ctx, executableNode, filepath.Join(baseDir, "my/fun/file"))
	require.NoError(t, err)

	// Make sure the link now works
	linkedExecutable := fc.FastLinkFile(ctx, executableNode, filepath.Join(baseDir, "my/fun/fastlinked-executablefile"))
	assert.True(t, linkedExecutable, "executable file should link")
	assert.FileExists(t, filepath.Join(baseDir, "my/fun/fastlinked-executablefile"))
	assertFileContents(t, filepath.Join(baseDir, "my/fun/fastlinked-executablefile"), "my/fun/file-executable")

	// Make sure the original link still works
	secondLink := fc.FastLinkFile(ctx, node, filepath.Join(baseDir, "my/fun/second-fastlinkedfile"))
	assert.True(t, secondLink, "original file should still link")
	assert.FileExists(t, filepath.Join(baseDir, "my/fun/second-fastlinkedfile"))
	assertFileContents(t, filepath.Join(baseDir, "my/fun/second-fastlinkedfile"), "my/fun/file")
}

func TestFileCacheGroupIsolation(t *testing.T) {
	ctx := context.TODO()
	fcDir := testfs.MakeTempDir(t)
	baseDir := testfs.MakeTempDir(t)
	authedCtx := claims.AuthContextFromClaims(ctx, &claims.Claims{GroupID: "GR12345"}, nil)

	{
		// Create a filecache and add a couple files.
		fc, err := filecache.NewFileCache(fcDir, 100000, false)
		if err != nil {
			t.Fatal(err)
		}
		fc.WaitForDirectoryScanToComplete()

		writeFile(t, baseDir, "my/fun/file", false)
		node := nodeFromString("my/fun/file", false)
		err = fc.AddFile(ctx, node, filepath.Join(baseDir, "my/fun/file"))
		require.NoError(t, err)

		writeFile(t, baseDir, "my/evil/file", false)
		node = nodeFromString("my/evil/file", false)
		err = fc.AddFile(authedCtx, node, filepath.Join(baseDir, "my/evil/file"))
		require.NoError(t, err)
	}
	{
		// Recreate filecache and wait for it to scan exsting files.
		// Ensure that they are still present.
		fc, err := filecache.NewFileCache(fcDir, 100000, false)
		if err != nil {
			t.Fatal(err)
		}
		fc.WaitForDirectoryScanToComplete()

		node := nodeFromString("my/fun/file", false)
		linked := fc.FastLinkFile(ctx, node, filepath.Join(baseDir, "my/fun/fastlinked-file"))
		assert.True(t, linked, "existing file should link")
		assert.FileExists(t, filepath.Join(baseDir, "my/fun/fastlinked-file"))
		assertFileContents(t, filepath.Join(baseDir, "my/fun/fastlinked-file"), "my/fun/file")

		evilNode := nodeFromString("my/evil/file", false)
		evilLinked := fc.FastLinkFile(authedCtx, evilNode, filepath.Join(baseDir, "my/evil/fastlinked-file"))
		assert.True(t, evilLinked, "existing file should link")
		assert.FileExists(t, filepath.Join(baseDir, "my/evil/fastlinked-file"))
		assertFileContents(t, filepath.Join(baseDir, "my/evil/fastlinked-file"), "my/evil/file")
	}
	{
		// Recreate filecache and wait for it to scan exsting files.
		// Ensure that permissions are preserved.
		fc, err := filecache.NewFileCache(fcDir, 100000, false)
		if err != nil {
			t.Fatal(err)
		}
		fc.WaitForDirectoryScanToComplete()

		node := nodeFromString("my/fun/file", false)
		linked := fc.FastLinkFile(authedCtx, node, filepath.Join(baseDir, "my/unauthed/fun-file"))
		assert.False(t, linked, "unowned file should not link")
		assert.NoFileExists(t, filepath.Join(baseDir, "my/unauthed/fun-file"))

		evilNode := nodeFromString("my/evil/file", false)
		evilLinked := fc.FastLinkFile(ctx, evilNode, filepath.Join(baseDir, "my/unauthed/evil-file"))
		assert.False(t, evilLinked, "unowned file should not link")
		assert.NoFileExists(t, filepath.Join(baseDir, "my/unauthed/evil-file"))
	}
}

func TestFileCacheOverwrite(t *testing.T) {
	ctx := context.TODO()
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
				err = fc.AddFile(ctx, node, filepath.Join(tempDir, "file1"))
				require.NoError(t, err)
				linked := fc.FastLinkFile(ctx, node, filepath.Join(tempDir, "file1-link1"))
				require.True(t, linked, "expected cache hit after writing file")
				// Verify all file contents
				file1Content := testfs.ReadFileAsString(t, tempDir, "file1")
				file1Link1Content := testfs.ReadFileAsString(t, tempDir, "file1-link1")
				require.Equal(t, "file1-content", file1Content)
				require.Equal(t, "file1-content", file1Link1Content)
			}
			{
				// Overwrite existing node entry
				err = fc.AddFile(ctx, node, filepath.Join(tempDir, "file1"))
				require.NoError(t, err)
				linked := fc.FastLinkFile(ctx, node, filepath.Join(tempDir, "file1-link2"))
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
				err = fc.AddFile(ctx, node, filepath.Join(tempDir, "file2"))
				require.NoError(t, err)
				linked := fc.FastLinkFile(ctx, node, filepath.Join(tempDir, "file2-link1"))
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

func TestFileCacheEviction(t *testing.T) {
	ctx := context.Background()
	// For now just assume the disk block size is 4096
	const fsBlockSize = 4096
	// Create a filecache that can only fit 1 physical block
	filecacheRoot := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(filecacheRoot, fsBlockSize, false)
	require.NoError(t, err)
	fc.WaitForDirectoryScanToComplete()
	tempDir := fc.TempDir()

	node1 := nodeFromString("A", false)
	node2 := nodeFromString("B", false)
	// Write a file that takes up 1 block, containing 1 byte
	{
		writeFileContent(t, tempDir, "file1", "A", false)
		err := fc.AddFile(ctx, node1, filepath.Join(tempDir, "file1"))
		require.NoError(t, err)
		linked := fc.FastLinkFile(ctx, node1, filepath.Join(tempDir, "file1-link1"))
		require.True(t, linked)
	}
	// Write a different file that takes up 1 block, containing 1 byte
	{
		writeFileContent(t, tempDir, "file2", "B", false)
		err := fc.AddFile(ctx, node2, filepath.Join(tempDir, "file2"))
		require.NoError(t, err)
		linked := fc.FastLinkFile(ctx, node2, filepath.Join(tempDir, "file2-link1"))
		require.True(t, linked)
	}
	// The first file should now be evicted, so linking it should fail.
	{
		linked := fc.FastLinkFile(ctx, node1, filepath.Join(tempDir, "file1-link2"))
		require.False(t, linked)
	}
}

func TestFileCacheEvictionAfterStartupScan(t *testing.T) {
	ctx := context.Background()
	// For now just assume the disk block size is 4096
	const fsBlockSize = 4096
	// Create a filecache that can fit 1 physical block plus one byte.
	// Initialize it with a file that takes up 1 block, containing 1 byte.
	filecacheRoot := testfs.MakeTempDir(t)
	writeFileContent(t, filecacheRoot, "ANON/"+hash.String("A"), "A", false)
	fc, err := filecache.NewFileCache(filecacheRoot, fsBlockSize+1, false)
	require.NoError(t, err)
	fc.WaitForDirectoryScanToComplete()
	tempDir := fc.TempDir()

	node1 := nodeFromString("A", false)
	node2 := nodeFromString("B", false)
	// Linking node1 should succeed
	{
		linked := fc.FastLinkFile(ctx, node1, filepath.Join(tempDir, "file1-link1"))
		require.True(t, linked)
	}
	// Write a different file that takes up 1 block, containing 1 byte
	{
		writeFileContent(t, tempDir, "file2", "B", false)
		err := fc.AddFile(ctx, node2, filepath.Join(tempDir, "file2"))
		require.NoError(t, err)
		linked := fc.FastLinkFile(ctx, node2, filepath.Join(tempDir, "file2-link1"))
		require.True(t, linked)
	}
	// The first file should now be evicted, so linking it should fail. If the
	// startup scan incorrectly used content size rather than size on disk, this
	// test would fail, since the cache has 4097 bytes of capacity.
	{
		linked := fc.FastLinkFile(ctx, node1, filepath.Join(tempDir, "file1-link2"))
		require.False(t, linked)
	}
}

func BenchmarkFilecacheLink(b *testing.B) {
	ctx := context.TODO()
	flags.Set(b, "app.log_level", "warn")
	log.Configure()

	for _, test := range []struct {
		Name         string
		Ops          int
		ReadFraction float64
	}{
		{Name: "100%Link/0%Add/1K", Ops: 1000, ReadFraction: 1},
		{Name: "95%Link/5%Add/1K", Ops: 1000, ReadFraction: 0.95},
	} {
		b.Run(test.Name, func(b *testing.B) {
			root := testfs.MakeTempDir(b)
			fc, err := filecache.NewFileCache(testfs.MakeDirAll(b, root, "cache"), 100_000_000, false /*=delete*/)
			require.NoError(b, err)
			fc.WaitForDirectoryScanToComplete()
			tmp := fc.TempDir()

			// Create 1K small files. Use UniformRandomGenerator to ensure
			// uniqueness, otherwise Add() may result in temporary eviction
			// which can cause the test to fail. (In practice, this eviction is
			// fine/expected).
			g := digest.UniformRandomGenerator(0)
			var nodes []*repb.FileNode
			for i := 0; i < test.Ops; i++ {
				d, buf, err := g.RandomDigestBuf(20)
				require.NoError(b, err)
				name := fmt.Sprintf("file_%d", i)
				path := filepath.Join(tmp, name)
				err = os.WriteFile(path, buf, 0644)
				require.NoError(b, err)
				node := &repb.FileNode{
					Name:   name,
					Digest: d,
				}
				nodes = append(nodes, node)
				err = fc.AddFile(ctx, node, path)
				require.NoError(b, err)
			}

			var outDirs []string
			for i := 0; i < b.N; i++ {
				outDirs = append(outDirs, testfs.MakeDirAll(b, root, fmt.Sprintf("outdir_%d", i)))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Fast link all of the files we created
				out := outDirs[i]
				eg := &errgroup.Group{}
				eg.SetLimit(100)
				for _, node := range nodes {
					node := node
					eg.Go(func() error {
						if rand.Float64() > test.ReadFraction {
							err := fc.AddFile(ctx, node, filepath.Join(tmp, node.GetName()))
							if err != nil {
								require.FailNowf(b, "add failed", "%s", err)
							}
							return nil
						}

						ok := fc.FastLinkFile(ctx, node, filepath.Join(out, fmt.Sprint(node.GetName())))
						if !ok {
							require.FailNowf(b, "link failed", "")
						}
						return nil
					})
					if b.Failed() {
						break
					}
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
