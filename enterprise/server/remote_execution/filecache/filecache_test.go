package filecache_test

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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

func writeFileContent(t *testing.T, base, path, content string, executable bool) string {
	mod := fs.FileMode(0644)
	if executable {
		mod = 0755
	}
	fullPath := filepath.Join(base, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0777); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fullPath, []byte(content), mod); err != nil {
		t.Fatal(err)
	}
	return fullPath
}

func TestFilecache(t *testing.T) {
	ctx := context.TODO()
	fcDir := testfs.MakeTempDir(t)
	// Create filecache
	fc, err := filecache.NewFileCache(fcDir, 100000, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fc.Close() })
	fc.WaitForDirectoryScanToComplete()

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
	authedCtx := claims.AuthContextWithJWT(ctx, &claims.Claims{GroupID: "GR12345"}, nil)

	{
		// Create a filecache and add a couple files.
		fc, err := filecache.NewFileCache(fcDir, 100000, false)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { fc.Close() })
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
		t.Cleanup(func() { fc.Close() })
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
		t.Cleanup(func() { fc.Close() })
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
			t.Cleanup(func() { fc.Close() })
			fc.WaitForDirectoryScanToComplete()
			tempDir := testfs.MakeTempDir(t)

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
	t.Cleanup(func() { fc.Close() })
	fc.WaitForDirectoryScanToComplete()
	tempDir := testfs.MakeTempDir(t)

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
	t.Cleanup(func() { fc.Close() })
	fc.WaitForDirectoryScanToComplete()
	tempDir := testfs.MakeTempDir(t)

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

func TestScanWithConcurrentAdd(t *testing.T) {
	for trial := 0; trial < 100; trial++ {
		ctx := context.Background()
		filecacheRoot := testfs.MakeTempDir(t)

		const n = 100
		var nodes [n]*repb.FileNode
		var nodeContents [n]string
		for i := 0; i < n; i++ {
			name := fmt.Sprint(i)
			nodes[i] = nodeFromString(name, false)
			writeFileContent(t, filecacheRoot, "ANON/"+nodes[i].GetDigest().GetHash(), name, false)
			nodeContents[i] = name
		}

		i := rand.Intn(n)
		nodeToAddConcurrently := nodes[i]
		nodeToAddConcurrentlyPath := filepath.Join(testfs.MakeTempDir(t), "action.output")
		err := os.WriteFile(nodeToAddConcurrentlyPath, []byte(nodeContents[i]), 0644)
		require.NoError(t, err)

		fc, err := filecache.NewFileCache(filecacheRoot, 10_000_000, false)
		require.NoError(t, err)
		t.Cleanup(func() { fc.Close() })

		// While the directory scan is in progress, re-add a random file
		// to trigger a race.
		err = fc.AddFile(ctx, nodeToAddConcurrently, nodeToAddConcurrentlyPath)
		require.NoError(t, err)

		fc.WaitForDirectoryScanToComplete()

		// The directory scan should be resilient to this race condition -
		// linking any file should work.
		for i := 0; i < n; i++ {
			ok := fc.FastLinkFile(ctx, nodes[i], filepath.Join(fc.TempDir(), fmt.Sprintf("out-%d", i)))
			require.True(t, ok, "link node %d (test trial %d)", i, trial)
		}
	}
}

func TestFileCacheEvictionAfterSubdirPrefixing(t *testing.T) {
	ctx := context.Background()
	fcDir := testfs.MakeTempDir(t)
	scratchDir := testfs.MakeTempDir(t)

	var unprefixedNodes []*repb.FileNode
	{
		fc, err := filecache.NewFileCache(fcDir, 4096*10, false)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { fc.Close() })
		fc.WaitForDirectoryScanToComplete()

		nodes := make([]*repb.FileNode, 10)
		for i := 0; i < len(nodes); i++ {
			rn, buf := testdigest.RandomCASResourceBuf(t, 4096)
			name := rn.GetDigest().GetHash()
			writeFileContent(t, scratchDir, name, string(buf), false /*executable*/)
			node := &repb.FileNode{Digest: rn.GetDigest()}
			err = fc.AddFile(ctx, node, filepath.Join(scratchDir, name))
			require.NoError(t, err)
			nodes[i] = node
		}
		log.Printf("Done adding initial files")
		unprefixedNodes = nodes
	}

	var prefixLength4Nodes []*repb.FileNode
	{
		// Enable subdir prefixing with prefix length 4.
		flags.Set(t, "executor.include_subdir_prefix", true)
		flags.Set(t, "executor.subdir_prefix_length", 4)
		log.Printf("Scanning old files")
		// Recreate filecache and wait for it to scan existing files.
		fc, err := filecache.NewFileCache(fcDir, 4096*10, false)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { fc.Close() })
		fc.WaitForDirectoryScanToComplete()

		// Ensure that the files from the previous iteration (without subdir
		// prefixing enabled) are still present.
		for _, n := range unprefixedNodes {
			exists := fc.ContainsFile(ctx, n)
			require.True(t, exists, "file %s should still exist", n.GetDigest().GetHash())
			linkPath := filepath.Join(scratchDir, strconv.Itoa(rand.Intn(1e12)))
			ok := fc.FastLinkFile(ctx, n, linkPath)
			require.True(t, ok, "file %s should be linkable", n.GetDigest().GetHash())
			require.FileExists(t, linkPath)
			actualDigest, err := digest.ComputeForFile(linkPath, repb.DigestFunction_SHA256)
			require.NoError(t, err)
			require.Equal(t, n.GetDigest().GetHash(), actualDigest.GetHash())
		}

		// Add new files to the cache.
		nodes := make([]*repb.FileNode, 10)
		for i := 0; i < len(nodes); i++ {
			rn, buf := testdigest.RandomCASResourceBuf(t, 4096)
			name := rn.GetDigest().GetHash()
			writeFileContent(t, scratchDir, name, string(buf), false /*executable*/)
			node := &repb.FileNode{Digest: rn.GetDigest()}
			err = fc.AddFile(ctx, node, filepath.Join(scratchDir, name))
			require.NoError(t, err)
			nodes[i] = node
		}
		prefixLength4Nodes = nodes

		// The old unprefixed files should all have been evicted.
		for _, n := range unprefixedNodes {
			exists := fc.ContainsFile(ctx, n)
			require.False(t, exists, "file %s should still exist", n.GetDigest().GetHash())
			linkPath := filepath.Join(scratchDir, strconv.Itoa(rand.Intn(1e12)))
			ok := fc.FastLinkFile(ctx, n, linkPath)
			require.False(t, ok, "file %s should not be linkable", n.GetDigest().GetHash())
			require.NoFileExists(t, linkPath)
		}

		// The disk usage should be the size of the new files.
		require.Equal(t, int64(4096*10), fileDiskUsageRecursive(t, fcDir))
	}
	{
		// Now *decrease* the subdir prefix length from 4 -> 2; should be able
		// to repopulate the cache as well as evict old files in this situtation
		// as well.
		flags.Set(t, "executor.include_subdir_prefix", true)
		flags.Set(t, "executor.subdir_prefix_length", 2)
		log.Printf("Scanning old files")

		fc, err := filecache.NewFileCache(fcDir, 4096*10, false)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { fc.Close() })
		fc.WaitForDirectoryScanToComplete()

		// Ensure that the files from the previous iteration (prefix length 4)
		// are still present.
		for _, n := range prefixLength4Nodes {
			exists := fc.ContainsFile(ctx, n)
			require.True(t, exists, "file %s should still exist", n.GetDigest().GetHash())
			linkPath := filepath.Join(scratchDir, strconv.Itoa(rand.Intn(1e12)))
			ok := fc.FastLinkFile(ctx, n, linkPath)
			require.True(t, ok, "file %s should be linkable", n.GetDigest().GetHash())
			require.FileExists(t, linkPath)
			actualDigest, err := digest.ComputeForFile(linkPath, repb.DigestFunction_SHA256)
			require.NoError(t, err)
			require.Equal(t, n.GetDigest().GetHash(), actualDigest.GetHash())
		}

		// Add new files to the cache.
		for range 10 {
			rn, buf := testdigest.RandomCASResourceBuf(t, 4096)
			name := rn.GetDigest().GetHash()
			writeFileContent(t, scratchDir, name, string(buf), false /*executable*/)
			node := &repb.FileNode{Digest: rn.GetDigest()}
			err = fc.AddFile(ctx, node, filepath.Join(scratchDir, name))
			require.NoError(t, err)
		}

		// The old files should all have been evicted.
		for _, n := range prefixLength4Nodes {
			exists := fc.ContainsFile(ctx, n)
			require.False(t, exists, "file %s should not exist", n.GetDigest().GetHash())
			linkPath := filepath.Join(scratchDir, strconv.Itoa(rand.Intn(1e12)))
			ok := fc.FastLinkFile(ctx, n, linkPath)
			require.False(t, ok, "file %s should not be linkable", n.GetDigest().GetHash())
			require.NoFileExists(t, linkPath)
		}

		// The disk usage should be the size of the new files.
		require.Equal(t, int64(4096*10), fileDiskUsageRecursive(t, fcDir))
	}
}

func TestFileCacheWriter(t *testing.T) {
	ctx := context.Background()
	fcDir := testfs.MakeTempDir(t)
	// Create filecache
	fc, err := filecache.NewFileCache(fcDir, 100000, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fc.Close() })
	fc.WaitForDirectoryScanToComplete()

	baseDir := testfs.MakeTempDir(t)

	path := "my/fun/file"
	content := "hello"
	fullPath := writeFileContent(t, baseDir, path, content, false)
	d, err := digest.ComputeForFile(fullPath, repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	node := &repb.FileNode{Digest: d}

	w, err := fc.Writer(ctx, node, repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	_, err = w.Write([]byte("bad content"))
	require.NoError(t, err)
	err = w.Commit()
	require.Error(t, err)
	require.True(t, status.IsDataLossError(err))
	err = w.Close()
	require.NoError(t, err)

	w, err = fc.Writer(ctx, node, repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	_, err = w.Write([]byte(content))
	require.NoError(t, err)
	err = w.Commit()
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)

	f, err := fc.Open(ctx, node)
	require.NoError(t, err)
	rc, err := io.ReadAll(f)
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)
	require.Equal(t, content, string(rc))

	cf := filepath.Join(fc.TempDir(), "cached_file")
	require.True(t, fc.FastLinkFile(ctx, node, cf))
	fi, err := os.Stat(cf)
	require.NoError(t, err)
	// Windows doesn't support Unix-style permission bits. Go reports a synthetic mode
	// (typically 0666 for writable files), so assert only the invariants we care about.
	requireOwnerReadableWritable(t, fi)
	requireNotOwnerExecutable(t, fi)

	// Write and read an executable file.
	execNode := &repb.FileNode{Digest: d, IsExecutable: true}
	w, err = fc.Writer(ctx, execNode, repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	_, err = w.Write([]byte(content))
	require.NoError(t, err)
	err = w.Commit()
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)
	cfe := filepath.Join(fc.TempDir(), "cached_file_exec")
	require.True(t, fc.FastLinkFile(ctx, execNode, cfe))
	fi, err = os.Stat(cfe)
	require.NoError(t, err)
	requireOwnerReadableWritable(t, fi)
	requireOwnerExecutable(t, fi)

	// Write with seeking.
	execNode = &repb.FileNode{Digest: d, IsExecutable: true}
	w, err = fc.Writer(ctx, execNode, repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	_, err = w.Write([]byte(content[:1]))
	require.NoError(t, err)
	_, err = w.(io.Seeker).Seek(0, io.SeekStart)
	require.NoError(t, err)
	_, err = w.Write([]byte(content))
	require.NoError(t, err)
	err = w.Commit()
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)
	cfe = filepath.Join(fc.TempDir(), "cached_file_exec")
	require.True(t, fc.FastLinkFile(ctx, execNode, cfe))
	fi, err = os.Stat(cfe)
	require.NoError(t, err)
	requireOwnerReadableWritable(t, fi)
	requireOwnerExecutable(t, fi)
}

func requireOwnerReadableWritable(t *testing.T, fi os.FileInfo) {
	t.Helper()
	perm := fi.Mode().Perm()
	require.NotZero(t, perm&0o400, "expected owner-readable file, got mode=%v", fi.Mode())
	require.NotZero(t, perm&0o200, "expected owner-writable file, got mode=%v", fi.Mode())
}

func requireOwnerExecutable(t *testing.T, fi os.FileInfo) {
	t.Helper()
	if runtime.GOOS == "windows" {
		// Executability is not represented via permission bits on Windows.
		return
	}
	perm := fi.Mode().Perm()
	require.NotZero(t, perm&0o100, "expected owner-executable file, got mode=%v", fi.Mode())
}

func requireNotOwnerExecutable(t *testing.T, fi os.FileInfo) {
	t.Helper()
	if runtime.GOOS == "windows" {
		// Executability is not represented via permission bits on Windows.
		return
	}
	perm := fi.Mode().Perm()
	require.Zero(t, perm&0o100, "expected non-executable file, got mode=%v", fi.Mode())
}

func BenchmarkFilecacheLink(b *testing.B) {
	// Use a simple Claims to speed up filecache.groupIDStringFromContext()
	ctx := claims.AuthContextWithJWT(context.Background(), &claims.Claims{}, nil)
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
			b.Cleanup(func() { fc.Close() })
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

func BenchmarkContainsAdd(b *testing.B) {
	// Use a simple Claims to speed up filecache.groupIDStringFromContext()
	ctx := claims.AuthContextWithJWT(context.Background(), &claims.Claims{}, nil)
	flags.Set(b, "app.log_level", "warn")
	log.Configure()

	for _, test := range []struct {
		Name    string
		Ops     int
		MaxSize int64
	}{
		// Mostly calls just Contains()
		{Name: "1K/10G", Ops: 1000, MaxSize: 10_000_000_000},
		// Small capacity, so every add causes an evict.
		{Name: "1K/1M", Ops: 1000, MaxSize: 1_000_000},
	} {
		b.Run(test.Name, func(b *testing.B) {
			runtime.GC()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			startingHeap := m.HeapAlloc

			root := testfs.MakeTempDir(b)
			fc, err := filecache.NewFileCache(testfs.MakeDirAll(b, root, "cache"), test.MaxSize, false /*=delete*/)
			require.NoError(b, err)
			b.Cleanup(func() { fc.Close() })
			fc.WaitForDirectoryScanToComplete()
			tmp := fc.TempDir()

			// Create 1K small files. Use UniformRandomGenerator to ensure
			// uniqueness, otherwise Add() may result in temporary eviction
			// which can cause the test to fail. (In practice, this eviction is
			// fine/expected).
			g := digest.UniformRandomGenerator(0)
			nodes := make(map[string]*repb.FileNode)
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
				nodes[path] = node
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				eg := &errgroup.Group{}
				eg.SetLimit(100)
				for path, node := range nodes {
					path, node := path, node
					eg.Go(func() error {
						if !fc.ContainsFile(ctx, node) {
							require.NoError(b, fc.AddFile(ctx, node, path))
						}
						return nil
					})
					if b.Failed() {
						break
					}
				}
				eg.Wait()
			}
			nodes = nil
			runtime.GC()
			runtime.ReadMemStats(&m)

			b.ReportMetric(float64(m.HeapAlloc-startingHeap), "retained-heap")

			// Keep a reference to these objects so they're not GCed before we
			// read mem stats.
			runtime.KeepAlive(fc)
			runtime.KeepAlive(g)
			runtime.KeepAlive(tmp)
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

// Sums estimated disk usage from files only.
func fileDiskUsageRecursive(t *testing.T, path string) int64 {
	var sum int64
	walkFn := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		sizeOnDisk, err := disk.EstimatedFileDiskUsage(info)
		if err != nil {
			return err
		}
		sum += sizeOnDisk
		return nil
	}

	err := filepath.WalkDir(path, walkFn)
	require.NoError(t, err)
	return sum
}

func TestFileCacheWriteCleansUpTempFile(t *testing.T) {
	ctx := context.Background()
	fcDir := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(fcDir, 100000, false)
	require.NoError(t, err)
	t.Cleanup(func() { fc.Close() })
	fc.WaitForDirectoryScanToComplete()

	rn, buf := testdigest.RandomCASResourceBuf(t, 1024)
	node := &repb.FileNode{Digest: rn.GetDigest()}

	_, err = fc.Write(ctx, node, buf)
	require.NoError(t, err)

	pattern := filepath.Join(fc.TempDir(), rn.GetDigest().GetHash()+".*.tmp")
	matches, err := filepath.Glob(pattern)
	require.NoError(t, err)
	require.Empty(t, matches, "expected temp file(s) to be deleted: %v", matches)
}

func TestTrackExternalDirectory_Basic(t *testing.T) {
	ctx := context.Background()
	fcDir := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(fcDir, 100_000_000, false)
	require.NoError(t, err)
	t.Cleanup(func() { fc.Close() })
	fc.WaitForDirectoryScanToComplete()

	// Create a directory to track.
	extDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, extDir, map[string]string{
		"file1.txt": "contents1",
		"file2.txt": "contents2",
	})

	// Track the directory.
	unlock, err := fc.TrackExternalDirectory(ctx, extDir, 1000)
	require.NoError(t, err)
	require.NotNil(t, unlock)

	// Directory should still exist.
	require.DirExists(t, extDir)

	// Unlock.
	unlock()

	// Directory should still exist (not evicted yet since cache has room).
	require.DirExists(t, extDir)

	// Should be able to re-track the same directory.
	unlock2, err := fc.TrackExternalDirectory(ctx, extDir, 1000)
	require.NoError(t, err)
	unlock2()
}

func TestTrackExternalDirectory_NotFoundForNonExistentPath(t *testing.T) {
	ctx := context.Background()
	fcDir := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(fcDir, 100_000_000, false)
	require.NoError(t, err)
	t.Cleanup(func() { fc.Close() })
	fc.WaitForDirectoryScanToComplete()

	nonExistentPath := filepath.Join(fcDir, "does-not-exist")
	unlock, err := fc.TrackExternalDirectory(ctx, nonExistentPath, 1000)
	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err), "expected NotFoundError, got: %v", err)
	require.Nil(t, unlock)
}

func TestTrackExternalDirectory_EvictionOnAddWhenUnlocked(t *testing.T) {
	ctx := context.Background()
	fcDir := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(fcDir, 3000, false)
	require.NoError(t, err)
	t.Cleanup(func() { fc.Close() })
	fc.WaitForDirectoryScanToComplete()

	// Create and track a directory, then immediately unlock it.
	extDir1 := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, extDir1, map[string]string{"file.txt": "contents1"})
	unlock1, err := fc.TrackExternalDirectory(ctx, extDir1, 2000)
	require.NoError(t, err)
	unlock1()

	require.DirExists(t, extDir1, "dir should exist before eviction")

	// Track a second directory that causes eviction of the first.
	extDir2 := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, extDir2, map[string]string{"file.txt": "contents2"})
	unlock2, err := fc.TrackExternalDirectory(ctx, extDir2, 2000)
	require.NoError(t, err)
	defer unlock2()

	// dir1 had refCount=0, so eviction moves it to trash synchronously.
	require.NoDirExists(t, extDir1, "unlocked directory should be moved to trash on eviction")
}

func TestTrackExternalDirectory_DeferredDeletion_SingleLock(t *testing.T) {
	ctx := context.Background()
	fcDir := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(fcDir, 3000, false)
	require.NoError(t, err)
	t.Cleanup(func() { fc.Close() })
	fc.WaitForDirectoryScanToComplete()

	// Create and track a directory, keeping it locked.
	extDir1 := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, extDir1, map[string]string{"file.txt": "contents1"})
	unlock1, err := fc.TrackExternalDirectory(ctx, extDir1, 2000)
	require.NoError(t, err)

	// Track a second directory that causes eviction of the first.
	extDir2 := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, extDir2, map[string]string{"file.txt": "contents2"})
	unlock2, err := fc.TrackExternalDirectory(ctx, extDir2, 2000)
	require.NoError(t, err)
	defer unlock2()

	// dir1 is locked, so deletion is deferred.
	require.DirExists(t, extDir1, "locked directory should not be deleted during eviction")

	// Unlock triggers synchronous move to trash.
	unlock1()
	require.NoDirExists(t, extDir1, "directory should be moved to trash after unlocking")
}

func TestTrackExternalDirectory_DeferredDeletion_MultipleLocks(t *testing.T) {
	ctx := context.Background()
	fcDir := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(fcDir, 5000, false)
	require.NoError(t, err)
	t.Cleanup(func() { fc.Close() })
	fc.WaitForDirectoryScanToComplete()

	// Create and track a directory, acquiring multiple locks.
	extDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, extDir, map[string]string{"file.txt": "contents"})
	unlock1, err := fc.TrackExternalDirectory(ctx, extDir, 2000)
	require.NoError(t, err)
	unlock2, err := fc.TrackExternalDirectory(ctx, extDir, 2000)
	require.NoError(t, err)
	unlock3, err := fc.TrackExternalDirectory(ctx, extDir, 2000)
	require.NoError(t, err)

	// Add a file that causes eviction. This also tests mixing files and
	// directories in the same LRU.
	scratchDir := testfs.MakeTempDir(t)
	filePath := filepath.Join(scratchDir, "largefile")
	err = os.WriteFile(filePath, make([]byte, 4000), 0644)
	require.NoError(t, err)
	node := nodeFromString(string(make([]byte, 4000)), false)
	err = fc.AddFile(ctx, node, filePath)
	require.NoError(t, err)

	// Directory is locked, so deletion is deferred.
	require.DirExists(t, extDir, "locked directory should not be deleted during eviction")

	// Release locks one by one - directory should persist until all released.
	unlock1()
	require.DirExists(t, extDir, "dir should exist after releasing first lock")
	unlock2()
	require.DirExists(t, extDir, "dir should exist after releasing second lock")

	// Final unlock triggers synchronous move to trash.
	unlock3()
	require.NoDirExists(t, extDir, "directory should be moved to trash after releasing all locks")
}

func TestTrackExternalDirectory_ResurrectionFromAwaitingDeletion(t *testing.T) {
	ctx := context.Background()
	fcDir := testfs.MakeTempDir(t)
	// Very small cache size to trigger eviction.
	fc, err := filecache.NewFileCache(fcDir, 3000, false)
	require.NoError(t, err)
	t.Cleanup(func() { fc.Close() })
	fc.WaitForDirectoryScanToComplete()

	// Create and track first directory, keeping it locked.
	extDir1 := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, extDir1, map[string]string{"file.txt": "contents1"})

	unlock1, err := fc.TrackExternalDirectory(ctx, extDir1, 2000)
	require.NoError(t, err)

	// Create and track a second directory that will cause eviction of the first.
	extDir2 := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, extDir2, map[string]string{"file.txt": "contents2"})

	unlock2, err := fc.TrackExternalDirectory(ctx, extDir2, 2000)
	require.NoError(t, err)

	// First directory should be awaiting deletion (locked during eviction).
	require.DirExists(t, extDir1)

	// Re-track the first directory while it's awaiting deletion.
	// This should "resurrect" it back to the LRU.
	unlock1Resurrected, err := fc.TrackExternalDirectory(ctx, extDir1, 2000)
	require.NoError(t, err, "should be able to re-track directory awaiting deletion")

	// Unlock the original lock.
	unlock1()

	// Directory should still exist because we have a new lock.
	time.Sleep(100 * time.Millisecond)
	require.DirExists(t, extDir1, "resurrected directory should not be deleted while locked")

	// Unlock all.
	unlock2()
	unlock1Resurrected()
}

func TestTrackExternalDirectory_LRUEvictionOrder(t *testing.T) {
	ctx := context.Background()
	fcDir := testfs.MakeTempDir(t)
	// Cache can hold 2 directories of size 1000 each, but not 3.
	fc, err := filecache.NewFileCache(fcDir, 2500, false)
	require.NoError(t, err)
	t.Cleanup(func() { fc.Close() })
	fc.WaitForDirectoryScanToComplete()

	// Create three directories.
	extDir1 := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, extDir1, map[string]string{"file.txt": "contents1"})

	extDir2 := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, extDir2, map[string]string{"file.txt": "contents2"})

	extDir3 := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, extDir3, map[string]string{"file.txt": "contents3"})

	// Track directories in order: dir1, dir2, dir3.
	unlock1, err := fc.TrackExternalDirectory(ctx, extDir1, 1000)
	require.NoError(t, err)
	unlock1()

	unlock2, err := fc.TrackExternalDirectory(ctx, extDir2, 1000)
	require.NoError(t, err)
	unlock2()

	// At this point, dir1 is LRU (oldest).
	// Adding dir3 should evict dir1.
	unlock3, err := fc.TrackExternalDirectory(ctx, extDir3, 1000)
	require.NoError(t, err)
	defer unlock3()

	// dir1 should be evicted (oldest) and moved to trash synchronously.
	require.NoDirExists(t, extDir1, "dir1 (oldest) should be moved to trash")

	// dir2 should still exist.
	require.DirExists(t, extDir2, "dir2 should not be evicted yet")

	// dir3 should still exist.
	require.DirExists(t, extDir3, "dir3 should not be evicted")
}

func TestFilecache_ConcurrentFileAndDirectoryOperations(t *testing.T) {
	// This test runs many concurrent operations to try to trigger race conditions.
	// Run with -race to detect data races.
	ctx := context.Background()
	fcDir := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(fcDir, 100_000_000, false)
	require.NoError(t, err)
	t.Cleanup(func() { fc.Close() })
	fc.WaitForDirectoryScanToComplete()

	const (
		numFiles         = 50
		numDirs          = 20
		numGoroutines    = 100
		opsPerRoutine    = 50
		concurrencyLimit = 8
	)

	// Pre-create files in a scratch directory.
	scratchDir := testfs.MakeTempDir(t)
	var nodes [numFiles]*repb.FileNode
	for i := range numFiles {
		content := fmt.Sprintf("file-content-%d", i)
		path := filepath.Join(scratchDir, fmt.Sprintf("file%d", i))
		err := os.WriteFile(path, []byte(content), 0644)
		require.NoError(t, err)
		nodes[i] = nodeFromString(content, false)
	}

	// Pre-create directories.
	var extDirs [numDirs]string
	for i := range numDirs {
		extDirs[i] = testfs.MakeTempDir(t)
		testfs.WriteAllFileContents(t, extDirs[i], map[string]string{
			"file.txt": fmt.Sprintf("dir-contents-%d", i),
		})
	}

	// Track which directories have active locks.
	var dirLocks [numDirs][]func()
	var dirLocksMu sync.Mutex

	eg := &errgroup.Group{}
	eg.SetLimit(concurrencyLimit)

	for g := range numGoroutines {
		eg.Go(func() error {
			rng := rand.New(rand.NewSource(int64(g)))
			for op := range opsPerRoutine {
				switch rng.Intn(6) {
				case 0: // AddFile
					fileIdx := rng.Intn(numFiles)
					filePath := filepath.Join(scratchDir, fmt.Sprintf("file%d", fileIdx))
					_ = fc.AddFile(ctx, nodes[fileIdx], filePath)

				case 1: // FastLinkFile
					fileIdx := rng.Intn(numFiles)
					outPath := filepath.Join(fc.TempDir(), fmt.Sprintf("link-%d-%d-%d", g, op, fileIdx))
					fc.FastLinkFile(ctx, nodes[fileIdx], outPath)
					os.Remove(outPath) // Cleanup

				case 2: // ContainsFile
					fileIdx := rng.Intn(numFiles)
					fc.ContainsFile(ctx, nodes[fileIdx])

				case 3: // TrackExternalDirectory
					dirIdx := rng.Intn(numDirs)
					unlock, err := fc.TrackExternalDirectory(ctx, extDirs[dirIdx], 1000)
					if err == nil {
						dirLocksMu.Lock()
						dirLocks[dirIdx] = append(dirLocks[dirIdx], unlock)
						dirLocksMu.Unlock()
					}

				case 4: // Unlock a random directory
					dirLocksMu.Lock()
					for i := range numDirs {
						dirIdx := (rng.Intn(numDirs) + i) % numDirs
						if len(dirLocks[dirIdx]) > 0 {
							unlock := dirLocks[dirIdx][0]
							dirLocks[dirIdx] = dirLocks[dirIdx][1:]
							dirLocksMu.Unlock()
							unlock()
							goto nextOp
						}
					}
					dirLocksMu.Unlock()

				case 5: // DeleteFile
					fileIdx := rng.Intn(numFiles)
					fc.DeleteFile(ctx, nodes[fileIdx])
				}
			nextOp:
			}
			return nil
		})
	}

	err = eg.Wait()
	require.NoError(t, err)

	// Cleanup: release all remaining locks.
	for i := range numDirs {
		for _, unlock := range dirLocks[i] {
			unlock()
		}
	}
}

func TestFilecache_ConcurrentDirectoryEvictionAndLocking(t *testing.T) {
	// This test specifically targets race conditions between eviction and locking.
	// It uses a small cache to force frequent evictions.
	ctx := context.Background()
	fcDir := testfs.MakeTempDir(t)
	// Small cache: can only hold ~2 directories of size 1000.
	fc, err := filecache.NewFileCache(fcDir, 2500, false)
	require.NoError(t, err)
	t.Cleanup(func() { fc.Close() })
	fc.WaitForDirectoryScanToComplete()

	const (
		numDirs       = 10
		numGoroutines = 20
		opsPerRoutine = 100
	)

	// Pre-create directories.
	var extDirs [numDirs]string
	for i := range numDirs {
		extDirs[i] = testfs.MakeTempDir(t)
		testfs.WriteAllFileContents(t, extDirs[i], map[string]string{
			"file.txt": fmt.Sprintf("dir-contents-%d", i),
		})
	}

	eg := &errgroup.Group{}
	eg.SetLimit(numGoroutines)

	for g := range numGoroutines {
		eg.Go(func() error {
			rng := rand.New(rand.NewSource(int64(g)))
			for range opsPerRoutine {
				dirIdx := rng.Intn(numDirs)

				// Try to track the directory. It may fail if the directory
				// was evicted and deleted.
				unlock, err := fc.TrackExternalDirectory(ctx, extDirs[dirIdx], 1000)
				if err != nil {
					// NotFound is expected if the directory was evicted.
					if !status.IsNotFoundError(err) {
						return fmt.Errorf("unexpected error tracking dir %d: %w", dirIdx, err)
					}
					continue
				}

				// Hold the lock briefly to increase contention.
				time.Sleep(time.Duration(rng.Intn(100)) * time.Microsecond)

				unlock()
			}
			return nil
		})
	}

	err = eg.Wait()
	require.NoError(t, err)
}
