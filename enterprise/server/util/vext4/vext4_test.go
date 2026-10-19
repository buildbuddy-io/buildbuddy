//go:build linux

package vext4_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vext4"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	blockSize = 4096
	// A file needs more than 4 extents of 32768 blocks for its extent tree
	// to spill into an index block.
	bigFileSize = 4*32768*blockSize + 10*blockSize
)

func TestBuild_ImageMatchesSourceTree(t *testing.T) {
	root := testfs.MakeTempDir(t)
	src := filepath.Join(root, "src")
	rng := rand.New(rand.NewPCG(1, 2))
	writeFile := func(name string, size int, mode os.FileMode) {
		path := filepath.Join(src, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		buf := make([]byte, size)
		for i := range buf {
			buf[i] = byte(rng.IntN(256))
		}
		require.NoError(t, os.WriteFile(path, buf, mode))
		require.NoError(t, os.Chmod(path, mode))
	}
	// Files of sizes around the block size, in nested directories.
	for i, size := range []int{0, 1, 4095, 4096, 4097, 100_000, 1_000_000} {
		writeFile(fmt.Sprintf("files/f%d", i), size, 0644)
	}
	writeFile("bin/tool", 5000, 0755)
	writeFile("secret", 10, 0600)
	require.NoError(t, os.MkdirAll(filepath.Join(src, "empty"), 0755))
	// A directory with enough entries to need several blocks, plus names
	// at the length limit and with multibyte characters.
	for i := range 1500 {
		writeFile(fmt.Sprintf("many/entry-%04d", i), i%7, 0644)
	}
	writeFile("many/"+strings.Repeat("n", 255), 3, 0644)
	writeFile("many/ünïcödé-名前", 3, 0644)
	// Symlinks with inline and block-backed targets, including a dangling one.
	require.NoError(t, os.Symlink("f0", filepath.Join(src, "files", "short-link")))
	require.NoError(t, os.Symlink(strings.Repeat("x", 59), filepath.Join(src, "max-inline-link")))
	require.NoError(t, os.Symlink(strings.Repeat("y", 60), filepath.Join(src, "min-block-link")))
	require.NoError(t, os.Symlink("/does/not/exist/"+strings.Repeat("z", 200), filepath.Join(src, "dangling")))
	// The same file linked at two paths shares one inode in the image.
	require.NoError(t, os.Link(filepath.Join(src, "bin", "tool"), filepath.Join(src, "tool-link")))
	// A big sparse file whose extents need an index block. A few bytes are
	// set so that the comparison catches misplaced extents.
	big, err := os.Create(filepath.Join(src, "big"))
	require.NoError(t, err)
	require.NoError(t, big.Truncate(bigFileSize))
	for _, off := range []int64{0, 4096 * 32768, 4096*32768*3 + 5, bigFileSize - 1} {
		_, err := big.WriteAt([]byte{byte(off)}, off)
		require.NoError(t, err)
	}
	require.NoError(t, big.Close())

	imagePath := filepath.Join(root, "image.ext4")
	store, err := vext4.Build(t.Context(), src, imagePath, 64<<20)
	require.NoError(t, err)
	defer store.Close()
	stats := store.Stats()
	// tool-link shares an inode with bin/tool.
	require.Equal(t, 7+2+1502+1, stats.Files)
	require.Equal(t, 4, stats.Symlinks)
	// src, files, bin, empty, many, lost+found
	require.Equal(t, 6, stats.Directories)

	// The metadata image is small, since nothing but inodes, directories and
	// bitmaps is written to it.
	info, err := os.Stat(imagePath)
	require.NoError(t, err)
	require.Less(t, info.Sys().(*syscall.Stat_t).Blocks*512, int64(8<<20))

	flat := materialize(t, store, filepath.Join(root, "flat.ext4"))
	fsck(t, flat)
	out := filepath.Join(root, "out")
	require.NoError(t, os.Mkdir(out, 0755))
	debugfs(t, flat, "rdump / "+out)
	compareTrees(t, src, out, true)
	// debugfs dumps a hard-linked inode as separate files, so check the
	// link count on the inode itself.
	require.Contains(t, debugfs(t, flat, "stat /tool-link"), "Links: 2")
	require.Contains(t, debugfs(t, flat, "stat /files/f0"), "Links: 1")
	require.Contains(t, debugfs(t, flat, "stat /bin"), "Links: 2")
	// src has four subdirectories plus lost+found.
	require.Contains(t, debugfs(t, flat, "stat /"), "Links: 7")
	// debugfs labels extent tree blocks ETB.
	require.Contains(t, debugfs(t, flat, "stat /big"), "(ETB0)")
}

func TestBuild_LeavesRequestedFreeSpace(t *testing.T) {
	root := testfs.MakeTempDir(t)
	src := filepath.Join(root, "src")
	testfs.WriteAllFileContents(t, src, map[string]string{"a": "a"})

	store, err := vext4.Build(t.Context(), src, filepath.Join(root, "image.ext4"), 300<<20)
	require.NoError(t, err)
	defer store.Close()

	flat := materialize(t, store, filepath.Join(root, "flat.ext4"))
	fsck(t, flat)
	free := dumpe2fsValue(t, flat, "Free blocks:")
	require.GreaterOrEqual(t, free*blockSize, int64(300<<20))
	size, err := store.SizeBytes()
	require.NoError(t, err)
	require.Equal(t, int64(0), size%(blockSize*blockSize*8), "image size is a whole number of block groups")
}

func TestStore_WritesShadowVirtualBlocks(t *testing.T) {
	root := testfs.MakeTempDir(t)
	src := filepath.Join(root, "src")
	// Three blocks of file data, each filled with a distinct byte.
	content := bytes.Repeat([]byte{0xA1}, blockSize)
	content = append(content, bytes.Repeat([]byte{0xB2}, blockSize)...)
	content = append(content, bytes.Repeat([]byte{0xC3}, 100)...)
	testfs.WriteAllFileContents(t, src, map[string]string{"data": string(content)})

	imagePath := filepath.Join(root, "image.ext4")
	store, err := vext4.Build(t.Context(), src, imagePath, 1<<20)
	require.NoError(t, err)
	defer store.Close()

	// Find the file's first block by scanning the image for its contents.
	size, err := store.SizeBytes()
	require.NoError(t, err)
	image := make([]byte, size)
	_, err = store.ReadAt(image, 0)
	require.NoError(t, err)
	start := int64(bytes.Index(image, content[:blockSize]))
	require.NotEqual(t, int64(-1), start)
	require.Equal(t, int64(0), start%blockSize)
	// The blocks are virtual, so nothing is in the image file itself.
	raw := make([]byte, blockSize)
	rawImage, err := os.Open(imagePath)
	require.NoError(t, err)
	defer rawImage.Close()
	_, err = rawImage.ReadAt(raw, start)
	require.NoError(t, err)
	require.Equal(t, make([]byte, blockSize), raw)
	// The last block reads as the file's tail followed by zeros.
	want := append(bytes.Repeat([]byte{0xC3}, 100), make([]byte, blockSize-100)...)
	require.Equal(t, want, image[start+2*blockSize:start+3*blockSize])
	require.Equal(t, int64(0), store.Stats().DirtyBytes)

	// A partial write to a virtual block keeps the rest of the block.
	_, err = store.WriteAt([]byte("hello"), start+blockSize+10)
	require.NoError(t, err)
	got := make([]byte, blockSize)
	_, err = store.ReadAt(got, start+blockSize)
	require.NoError(t, err)
	want = bytes.Repeat([]byte{0xB2}, blockSize)
	copy(want[10:], "hello")
	require.Equal(t, want, got)
	require.Equal(t, int64(blockSize), store.Stats().DirtyBytes)
	// The write lands in the image file, and the neighbouring blocks are
	// still served from the source file.
	_, err = rawImage.ReadAt(raw, start+blockSize)
	require.NoError(t, err)
	require.Equal(t, want, raw)
	_, err = rawImage.ReadAt(raw, start)
	require.NoError(t, err)
	require.Equal(t, make([]byte, blockSize), raw)

	// A write spanning a whole block and part of the next copies only the
	// partially written block first.
	span := bytes.Repeat([]byte{0xD4}, blockSize+8)
	_, err = store.WriteAt(span, start)
	require.NoError(t, err)
	got = make([]byte, 3*blockSize)
	_, err = store.ReadAt(got, start)
	require.NoError(t, err)
	want = append(bytes.Repeat([]byte{0xD4}, blockSize+8), bytes.Repeat([]byte{0xB2}, blockSize-8)...)
	copy(want[blockSize+10:], "hello")
	want = append(want, bytes.Repeat([]byte{0xC3}, 100)...)
	want = append(want, make([]byte, blockSize-100)...)
	require.Equal(t, want, got)
	require.Equal(t, int64(2*blockSize), store.Stats().DirtyBytes)

	// Writes outside the virtual blocks go straight to the image, and the
	// source file is untouched throughout.
	_, err = store.WriteAt([]byte("free"), size-blockSize)
	require.NoError(t, err)
	_, err = rawImage.ReadAt(raw[:4], size-blockSize)
	require.NoError(t, err)
	require.Equal(t, []byte("free"), raw[:4])
	srcContent, err := os.ReadFile(filepath.Join(src, "data"))
	require.NoError(t, err)
	require.Equal(t, content, srcContent)
}

func TestBuildTree_ImageMatchesInputTree(t *testing.T) {
	root := testfs.MakeTempDir(t)
	// Build the same tree on disk, to compare the image against, and as an
	// input tree whose file contents come from a test Source that counts
	// how often each file is opened.
	src := filepath.Join(root, "src")
	contents := map[string][]byte{}
	rng := rand.New(rand.NewPCG(3, 4))
	for name, size := range map[string]int{"a": 0, "b": 1, "c": 4096, "d": 4097, "e": 100_000} {
		buf := make([]byte, size)
		for i := range buf {
			buf[i] = byte(rng.IntN(256))
		}
		contents[name] = buf
	}
	contents["tool"] = []byte("#!/bin/sh\n")
	writeFile := func(name string, content []byte, mode os.FileMode) {
		path := filepath.Join(src, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, content, mode))
		require.NoError(t, os.Chmod(path, mode))
	}
	writeFile("files/a", contents["a"], 0644)
	writeFile("files/b", contents["b"], 0644)
	writeFile("files/sub/c", contents["c"], 0644)
	writeFile("files/sub/d", contents["d"], 0644)
	writeFile("e", contents["e"], 0644)
	writeFile("bin/tool", contents["tool"], 0755)
	// The same contents at two paths.
	writeFile("tool-copy", contents["tool"], 0644)
	require.NoError(t, os.MkdirAll(filepath.Join(src, "empty"), 0755))
	require.NoError(t, os.Symlink("../e", filepath.Join(src, "files", "link")))
	require.NoError(t, os.Symlink(strings.Repeat("y", 60), filepath.Join(src, "long-link")))
	// Output directories, which the input tree does not contain.
	require.NoError(t, os.MkdirAll(filepath.Join(src, "out/nested"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(src, "files/sub/out"), 0755))

	node := func(name string, exec bool) *repb.FileNode {
		d, err := digest.Compute(bytes.NewReader(contents[name]), repb.DigestFunction_SHA256)
		require.NoError(t, err)
		return &repb.FileNode{Name: name, Digest: d, IsExecutable: exec}
	}
	dirNode := func(name string, dir *repb.Directory) *repb.DirectoryNode {
		d, err := digest.ComputeForMessage(dir, repb.DigestFunction_SHA256)
		require.NoError(t, err)
		return &repb.DirectoryNode{Name: name, Digest: d}
	}
	sub := &repb.Directory{Files: []*repb.FileNode{node("c", false), node("d", false)}}
	files := &repb.Directory{
		Directories: []*repb.DirectoryNode{dirNode("sub", sub)},
		Files:       []*repb.FileNode{node("a", false), node("b", false)},
		Symlinks:    []*repb.SymlinkNode{{Name: "link", Target: "../e"}},
	}
	bin := &repb.Directory{Files: []*repb.FileNode{node("tool", true)}}
	empty := &repb.Directory{}
	toolCopy := node("tool", false)
	toolCopy.Name = "tool-copy"
	inputs := &repb.Tree{
		Root: &repb.Directory{
			Directories: []*repb.DirectoryNode{dirNode("bin", bin), dirNode("empty", empty), dirNode("files", files)},
			Files:       []*repb.FileNode{node("e", false), toolCopy},
			Symlinks:    []*repb.SymlinkNode{{Name: "long-link", Target: strings.Repeat("y", 60)}},
		},
		Children: []*repb.Directory{bin, files, sub},
	}
	opens := &openCounter{counts: map[string]int{}}
	tree := &vext4.Tree{
		Inputs:         inputs,
		DigestFunction: repb.DigestFunction_SHA256,
		Dirs:           []string{"out/nested", "files/sub/out", "."},
		Source: func(path string, n *repb.FileNode) vext4.Source {
			return &memSource{opens: opens, path: path, content: contents[n.GetName()]}
		},
	}
	imagePath := filepath.Join(root, "image.ext4")
	store, err := vext4.BuildTree(t.Context(), tree, imagePath, 64<<20)
	require.NoError(t, err)
	defer store.Close()
	stats := store.Stats()
	require.Equal(t, 7, stats.Files)
	require.Equal(t, 2, stats.Symlinks)
	// src, bin, empty, files, sub, out, nested, sub/out, lost+found
	require.Equal(t, 9, stats.Directories)
	require.Equal(t, int64(len(contents["tool"])*2+1+4096+4097+100_000), stats.FileBytes)
	require.Equal(t, 0, stats.FilesRead)

	flat := materialize(t, store, filepath.Join(root, "flat.ext4"))
	fsck(t, flat)
	out := filepath.Join(root, "out")
	require.NoError(t, os.Mkdir(out, 0755))
	debugfs(t, flat, "rdump / "+out)
	compareTrees(t, src, out, false)
	// Reading the whole image reads every file once, through one Source
	// per distinct digest, so tool and tool-copy share one.
	stats = store.Stats()
	require.Equal(t, 5, stats.FilesRead)
	require.Equal(t, int64(len(contents["tool"])+1+4096+4097+100_000), stats.ReadFileBytes)
	require.Equal(t, map[string]int{"bin/tool": 1, "e": 1, "files/b": 1, "files/sub/c": 1, "files/sub/d": 1}, opens.counts)
}

func TestBuildTree_RejectsBadNames(t *testing.T) {
	for _, tc := range []struct {
		name  string
		tree  *vext4.Tree
		error string
	}{
		{
			name:  "name with a slash",
			tree:  &vext4.Tree{Inputs: &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{{Name: "a/b"}}}}},
			error: "a/b",
		},
		{
			name:  "duplicate name",
			tree:  &vext4.Tree{Inputs: &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{{Name: "a"}, {Name: "a"}}}}},
			error: "a",
		},
		{
			name:  "directory outside the tree",
			tree:  &vext4.Tree{Dirs: []string{"../out"}},
			error: "outside the tree",
		},
		{
			name: "directory over a file",
			tree: &vext4.Tree{
				Inputs: &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{{Name: "a"}}}},
				Dirs:   []string{"a/out"},
			},
			error: "not a directory",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.tree.DigestFunction = repb.DigestFunction_SHA256
			tc.tree.Source = func(string, *repb.FileNode) vext4.Source { return nil }
			_, err := vext4.BuildTree(t.Context(), tc.tree, filepath.Join(testfs.MakeTempDir(t), "image.ext4"), 1<<20)
			require.ErrorContains(t, err, tc.error)
		})
	}
}

// openCounter counts how often each path's Source is opened.
type openCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

// memSource is a Source whose contents are in memory. Open writes them to
// a temporary file, as a file cache would hold them.
type memSource struct {
	opens   *openCounter
	path    string
	content []byte
}

func (s *memSource) Open(context.Context) (*os.File, error) {
	s.opens.mu.Lock()
	s.opens.counts[s.path]++
	s.opens.mu.Unlock()
	f, err := os.CreateTemp("", "memsource-*")
	if err != nil {
		return nil, err
	}
	os.Remove(f.Name())
	if _, err := f.Write(s.content); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func TestBuild_RejectsSpecialFiles(t *testing.T) {
	root := testfs.MakeTempDir(t)
	src := testfs.MakeDirAll(t, root, "src")
	require.NoError(t, syscall.Mkfifo(filepath.Join(src, "fifo"), 0644))

	_, err := vext4.Build(t.Context(), src, filepath.Join(root, "image.ext4"), 1<<20)

	require.ErrorContains(t, err, "unsupported file type")
}

// materialize reads the whole store into a flat image file at path.
func materialize(t *testing.T, store *vext4.Store, path string) string {
	size, err := store.SizeBytes()
	require.NoError(t, err)
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()
	buf := make([]byte, 1<<20)
	for off := int64(0); off < size; off += int64(len(buf)) {
		n := min(int64(len(buf)), size-off)
		_, err := store.ReadAt(buf[:n], off)
		require.NoError(t, err)
		_, err = f.WriteAt(buf[:n], off)
		require.NoError(t, err)
	}
	return path
}

func fsck(t *testing.T, image string) {
	out, err := exec.Command("/sbin/e2fsck", "-fn", image).CombinedOutput()
	require.NoError(t, err, "e2fsck: %s", out)
}

func debugfs(t *testing.T, image, request string) string {
	out, err := exec.Command("/sbin/debugfs", "-R", request, image).CombinedOutput()
	require.NoError(t, err, "debugfs: %s", out)
	return string(out)
}

func dumpe2fsValue(t *testing.T, image, label string) int64 {
	out, err := exec.Command("/sbin/dumpe2fs", "-h", image).CombinedOutput()
	require.NoError(t, err, "dumpe2fs: %s", out)
	for line := range strings.SplitSeq(string(out), "\n") {
		if strings.HasPrefix(line, label) {
			var v int64
			_, err := fmt.Sscan(strings.TrimSpace(strings.TrimPrefix(line, label)), &v)
			require.NoError(t, err)
			return v
		}
	}
	t.Fatalf("%q not found in dumpe2fs output:\n%s", label, out)
	return 0
}

// compareTrees checks that dst has the same entries as src, with the same
// types, permissions, sizes, contents and symlink targets, and the same
// modification times if times is set. Ownership is skipped, since debugfs
// can only restore it as root.
func compareTrees(t *testing.T, src, dst string, times bool) {
	n := 0
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		rel, err := filepath.Rel(src, path)
		require.NoError(t, err)
		other := filepath.Join(dst, rel)
		want, err := os.Lstat(path)
		require.NoError(t, err)
		got, err := os.Lstat(other)
		require.NoError(t, err, "missing %s", rel)
		n++
		require.Equal(t, want.Mode().Type(), got.Mode().Type(), rel)
		require.Equal(t, want.Mode().Perm(), got.Mode().Perm(), rel)
		switch {
		case want.Mode().IsRegular():
			require.Equal(t, want.Size(), got.Size(), rel)
			require.Equal(t, fileHash(t, path), fileHash(t, other), rel)
			if times {
				require.Equal(t, want.ModTime().Unix(), got.ModTime().Unix(), rel)
			}
		case want.Mode()&fs.ModeSymlink != 0:
			wantTarget, err := os.Readlink(path)
			require.NoError(t, err)
			gotTarget, err := os.Readlink(other)
			require.NoError(t, err)
			require.Equal(t, wantTarget, gotTarget, rel)
		}
		return nil
	})
	require.NoError(t, err)
	// Everything in dst comes from src, apart from lost+found.
	m := 0
	err = filepath.WalkDir(dst, func(path string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		if d.Name() == "lost+found" {
			return filepath.SkipDir
		}
		m++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, n, m)
}

func fileHash(t *testing.T, path string) string {
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	h := sha256.New()
	_, err = io.Copy(h, f)
	require.NoError(t, err)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// BenchmarkBuild compares building a virtual image with converting the same
// tree with mke2fs, as the executor does today, using the executor's default
// slack space. Set VEXT4_BENCH_TREE to a directory to benchmark a real
// tree as well.
func BenchmarkBuild(b *testing.B) {
	const slackBytes = 2000e6
	trees := []struct {
		name  string
		dir   string
		files int
		bytes int64
	}{
		{name: "1k_files_10MB", files: 1_000, bytes: 10e6},
		{name: "10k_files_100MB", files: 10_000, bytes: 100e6},
		{name: "50k_files_1GB", files: 50_000, bytes: 1e9},
		{name: "100k_files_2GB", files: 100_000, bytes: 2e9},
	}
	if dir := os.Getenv("VEXT4_BENCH_TREE"); dir != "" {
		trees = append(trees, struct {
			name  string
			dir   string
			files int
			bytes int64
		}{name: filepath.Base(dir), dir: dir})
	}
	for _, tree := range trees {
		root := testfs.MakeTempDir(b)
		src := tree.dir
		if src == "" {
			src = filepath.Join(root, "src")
			testfs.WriteRandomTree(b, src, tree.files, tree.bytes)
			// Finish writing the tree back to disk, so that the writeback
			// does not compete with the conversions being timed.
			require.NoError(b, exec.Command("sync").Run())
		}
		b.Run(tree.name+"/mke2fs", func(b *testing.B) {
			for i := range b.N {
				image := filepath.Join(root, fmt.Sprintf("mke2fs-%d.ext4", i))
				size, err := ext4.DiskSizeBytes(b.Context(), src)
				require.NoError(b, err)
				err = ext4.DirectoryToImage(b.Context(), src, image, size+slackBytes)
				require.NoError(b, err)
				b.StopTimer()
				require.NoError(b, os.Remove(image))
				b.StartTimer()
			}
		})
		b.Run(tree.name+"/vext4", func(b *testing.B) {
			for i := range b.N {
				image := filepath.Join(root, fmt.Sprintf("vext4-%d.ext4", i))
				store, err := vext4.Build(b.Context(), src, image, slackBytes)
				require.NoError(b, err)
				b.StopTimer()
				require.NoError(b, store.Close())
				require.NoError(b, os.Remove(image))
				b.StartTimer()
			}
		})
	}
}
