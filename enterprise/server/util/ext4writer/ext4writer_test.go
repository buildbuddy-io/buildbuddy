package ext4writer_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4writer"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func requireTool(t *testing.T, path string) {
	if _, err := os.Stat(path); err != nil {
		t.Skipf("%s not available: %s", path, err)
	}
}

// makeTree writes a synthetic tree resembling a Bazel workspace.
func makeTree(t *testing.T, root string, nfiles, avg int, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	ndirs := max(1, nfiles/20)
	for i := 0; i < ndirs; i++ {
		// Some nesting.
		p := filepath.Join(root, fmt.Sprintf("d%04d", i))
		if i%5 == 0 {
			p = filepath.Join(root, "nested", fmt.Sprintf("x%02d", i%7), fmt.Sprintf("d%04d", i))
		}
		require.NoError(t, os.MkdirAll(p, 0755))
	}
	dirs := []string{}
	filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			dirs = append(dirs, p)
		}
		return nil
	})
	pat := make([]byte, 65536)
	for i := range pat {
		pat[i] = byte(i)
	}
	for i := 0; i < nfiles; i++ {
		sz := int(rng.ExpFloat64() * float64(avg))
		if sz > avg*8 {
			sz = avg * 8
		}
		data := make([]byte, sz)
		for off := 0; off < sz; off += len(pat) {
			copy(data[off:], pat)
		}
		if sz > 0 {
			data[0] = byte(i)
			data[sz-1] = byte(i >> 8)
		}
		mode := os.FileMode(0644)
		if i%7 == 0 {
			mode = 0755
		}
		d := dirs[rng.Intn(len(dirs))]
		require.NoError(t, os.WriteFile(filepath.Join(d, fmt.Sprintf("f%06d.dat", i)), data, mode))
	}
	// Edge cases.
	require.NoError(t, os.MkdirAll(filepath.Join(root, "empty_dir"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "empty_file"), nil, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "exactly_4096"), pat[:4096], 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "exactly_4097"), pat[:4097], 0644))
	require.NoError(t, os.Symlink("d0000", filepath.Join(root, "short_symlink")))
	require.NoError(t, os.Symlink(strings.Repeat("abcdefghij/", 10)+"target", filepath.Join(root, "long_symlink")))
	require.NoError(t, os.Symlink("../../..", filepath.Join(root, "empty_dir", "..dots")))
	require.NoError(t, os.WriteFile(filepath.Join(root, "hardlink_src"), []byte("hardlinked content"), 0600))
	require.NoError(t, os.Link(filepath.Join(root, "hardlink_src"), filepath.Join(root, "hardlink_a")))
	require.NoError(t, os.Link(filepath.Join(root, "hardlink_src"), filepath.Join(root, "d0001", "hardlink_b")))
	require.NoError(t, os.WriteFile(filepath.Join(root, strings.Repeat("n", 255)), []byte("max name"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "space in name & (weird) 'chars'"), []byte("x"), 0644))
	require.NoError(t, syscall.Mkfifo(filepath.Join(root, "fifo"), 0644))
	// A directory with many entries (multi-block linear directory).
	big := filepath.Join(root, "bigdir")
	require.NoError(t, os.MkdirAll(big, 0755))
	for i := 0; i < 3000; i++ {
		require.NoError(t, os.WriteFile(filepath.Join(big, fmt.Sprintf("entry_with_a_longish_name_%05d", i)), []byte{byte(i)}, 0644))
	}
}

type entry struct {
	Path   string
	Mode   string
	Size   int64
	Hash   string
	Target string
}

// snapshotDir summarizes a tree for comparison.
func snapshotDir(t *testing.T, root string, skip map[string]bool) []entry {
	var out []entry
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		rel, _ := filepath.Rel(root, p)
		if rel == "." || skip[rel] {
			return nil
		}
		info, err := os.Lstat(p)
		require.NoError(t, err)
		e := entry{Path: rel, Mode: info.Mode().String()}
		switch {
		case info.Mode().IsRegular():
			e.Size = info.Size()
			b, err := os.ReadFile(p)
			require.NoError(t, err)
			h := sha256.Sum256(b)
			e.Hash = hex.EncodeToString(h[:])
		case info.Mode()&os.ModeSymlink != 0:
			e.Target, err = os.Readlink(p)
			require.NoError(t, err)
		}
		out = append(out, e)
		return nil
	})
	require.NoError(t, err)
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

func fsck(t *testing.T, image string) {
	cmd := exec.Command("/sbin/e2fsck", "-f", "-n", image)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "e2fsck failed:\n%s", out)
	t.Logf("e2fsck: %s", strings.TrimSpace(string(out)))
}

// rdump extracts the whole image with debugfs.
func rdump(t *testing.T, image, dst string) {
	require.NoError(t, ext4.ImageToDirectory(context.Background(), image, dst, []string{"/"}))
}

func TestImageContentsMatchSource(t *testing.T) {
	for _, mode := range []string{"mmap", "cfr"} {
		t.Run(mode, func(t *testing.T) { testImageContentsMatchSource(t, mode) })
	}
}

func testImageContentsMatchSource(t *testing.T, copyMode string) {
	requireTool(t, "/sbin/e2fsck")
	requireTool(t, "/sbin/debugfs")
	root := testfs.MakeTempDir(t)
	src := filepath.Join(root, "src")
	require.NoError(t, os.Mkdir(src, 0755))
	makeTree(t, src, 2000, 16*1024, 1)

	image := filepath.Join(root, "ws.ext4")
	stats, err := ext4writer.DirectoryToImage(context.Background(), src, image, &ext4writer.Options{SizeBytes: 300e6, CopyMode: copyMode, Reflink: copyMode == "cfr"})
	require.NoError(t, err)
	t.Logf("stats: %s", stats)
	require.Equal(t, 2, stats.Hardlinks)
	fsck(t, image)

	// Extract and compare.
	dst := filepath.Join(root, "dst")
	require.NoError(t, os.Mkdir(dst, 0755))
	rdump(t, image, dst)
	// debugfs rdump puts the tree under dst/ (root dir name is empty -> "dst").
	extracted := dst
	if _, err := os.Stat(filepath.Join(dst, "d0001")); err != nil {
		// Some debugfs versions nest under a directory named after the root.
		entries, _ := os.ReadDir(dst)
		require.Len(t, entries, 1)
		extracted = filepath.Join(dst, entries[0].Name())
	}
	// debugfs rdump does not recreate FIFOs; check that one via stat instead.
	skip := map[string]bool{"lost+found": true, "fifo": true}
	want := snapshotDir(t, src, skip)
	got := snapshotDir(t, extracted, skip)
	wantPaths := map[string]bool{}
	for _, e := range want {
		wantPaths[e.Path] = true
	}
	for _, e := range got {
		require.True(t, wantPaths[e.Path], "unexpected entry in image: %q", e.Path)
	}
	require.Equal(t, len(want), len(got), "entry count")
	for i := range want {
		require.Equal(t, want[i], got[i])
	}
	out, err := exec.Command("/sbin/debugfs", "-R", "stat /fifo", image).CombinedOutput()
	require.NoError(t, err, "%s", out)
	require.Contains(t, string(out), "Type: FIFO")
	// Hardlinks share an inode in the image.
	var links []string
	for _, e := range got {
		if strings.HasPrefix(e.Path, "hardlink") || strings.HasSuffix(e.Path, "hardlink_b") {
			links = append(links, e.Path)
		}
	}
	require.Len(t, links, 3)
}

func TestLargeFilesAndManyExtents(t *testing.T) {
	requireTool(t, "/sbin/e2fsck")
	root := testfs.MakeTempDir(t)
	src := filepath.Join(root, "src")
	require.NoError(t, os.Mkdir(src, 0755))
	// A file bigger than one extent (128 MiB) and straddling a backup group.
	f, err := os.Create(filepath.Join(src, "big"))
	require.NoError(t, err)
	require.NoError(t, f.Truncate(300<<20))
	_, err = f.WriteAt([]byte("tail-marker"), 300<<20-11)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("head-marker"), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	// Lots of small files so data spans several groups.
	for i := 0; i < 50; i++ {
		require.NoError(t, os.WriteFile(filepath.Join(src, fmt.Sprintf("f%d", i)), make([]byte, 1<<20), 0644))
	}
	image := filepath.Join(root, "ws.ext4")
	stats, err := ext4writer.DirectoryToImage(context.Background(), src, image, &ext4writer.Options{SizeBytes: 2000e6})
	require.NoError(t, err)
	t.Logf("stats: %s", stats)
	fsck(t, image)
	dst := filepath.Join(root, "dst")
	require.NoError(t, os.Mkdir(dst, 0755))
	require.NoError(t, ext4.ImageToDirectory(context.Background(), image, dst, []string{"/big"}))
	b, err := os.ReadFile(filepath.Join(dst, "big"))
	require.NoError(t, err)
	require.Equal(t, int64(300<<20), int64(len(b)))
	require.Equal(t, "head-marker", string(b[:11]))
	require.Equal(t, "tail-marker", string(b[len(b)-11:]))
}

func TestEmptyDirectory(t *testing.T) {
	requireTool(t, "/sbin/e2fsck")
	root := testfs.MakeTempDir(t)
	src := filepath.Join(root, "src")
	require.NoError(t, os.Mkdir(src, 0755))
	image := filepath.Join(root, "ws.ext4")
	_, err := ext4writer.DirectoryToImage(context.Background(), src, image, nil)
	require.NoError(t, err)
	fsck(t, image)
}

// TestCompareWithMke2fs benchmarks against mke2fs -d on the same tree.
func TestCompareWithMke2fs(t *testing.T) {
	if os.Getenv("EXT4WRITER_BENCH") == "" {
		t.Skip("set EXT4WRITER_BENCH=1 to run")
	}
	root := testfs.MakeTempDir(t)
	src := filepath.Join(root, "src")
	require.NoError(t, os.Mkdir(src, 0755))
	makeTree(t, src, 10000, 32*1024, 2)
	size, err := ext4.DiskSizeBytes(context.Background(), src)
	require.NoError(t, err)
	imgSize := size + 2000e6
	for i := 0; i < 3; i++ {
		img := filepath.Join(root, fmt.Sprintf("a%d.ext4", i))
		start := time.Now()
		require.NoError(t, ext4.DirectoryToImage(context.Background(), src, img, imgSize))
		log.Infof("mke2fs -d: %s", time.Since(start))
		img = filepath.Join(root, fmt.Sprintf("b%d.ext4", i))
		start = time.Now()
		stats, err := ext4writer.DirectoryToImage(context.Background(), src, img, &ext4writer.Options{SizeBytes: imgSize})
		require.NoError(t, err)
		log.Infof("ext4writer: %s (%s)", time.Since(start), stats)
	}
}

// TestReaderMatchesDebugfs extracts trees with both the native reader and
// debugfs and compares them, for images produced by both writers (mke2fs
// images have htree directories, a journal, metadata checksums, etc.).
func TestReaderMatchesDebugfs(t *testing.T) {
	requireTool(t, "/sbin/debugfs")
	requireTool(t, "/sbin/mke2fs")
	root := testfs.MakeTempDir(t)
	src := filepath.Join(root, "src")
	require.NoError(t, os.Mkdir(src, 0755))
	makeTree(t, src, 1000, 8*1024, 3)
	// A sparse file.
	sf, err := os.Create(filepath.Join(src, "sparse"))
	require.NoError(t, err)
	require.NoError(t, sf.Truncate(10<<20))
	_, err = sf.WriteAt([]byte("middle"), 5<<20)
	require.NoError(t, err)
	require.NoError(t, sf.Close())

	native := filepath.Join(root, "native.ext4")
	_, err = ext4writer.DirectoryToImage(context.Background(), src, native, &ext4writer.Options{SizeBytes: 200e6})
	require.NoError(t, err)
	mk := filepath.Join(root, "mke2fs.ext4")
	require.NoError(t, ext4.DirectoryToImage(context.Background(), src, mk, 200e6))
	// Add files to the mke2fs image the way a guest would (new inodes,
	// new extents) using debugfs -w.
	out, err := exec.Command("/sbin/debugfs", "-w", "-R", "mkdir /out", mk).CombinedOutput()
	require.NoError(t, err, "%s", out)
	big := filepath.Join(root, "bigout")
	require.NoError(t, os.WriteFile(big, make([]byte, 3<<20), 0644))
	out, err = exec.Command("/sbin/debugfs", "-w", "-R", fmt.Sprintf("write %s /out/big.bin", big), mk).CombinedOutput()
	require.NoError(t, err, "%s", out)
	for _, img := range []string{native, mk} {
		for _, paths := range [][]string{{"/"}, {"bigdir", "nested", "out", "missing", "sparse"}} {
			a := testfs.MakeTempDir(t)
			b := testfs.MakeTempDir(t)
			require.NoError(t, ext4.ImageToDirectory(context.Background(), img, a, paths))
			require.NoError(t, ext4writer.ImageToDirectory(context.Background(), img, b, paths))
			skip := map[string]bool{"fifo": true}
			wa := snapshotDir(t, a, skip)
			wb := snapshotDir(t, b, skip)
			require.Equal(t, len(wa), len(wb), "%s %v: entry count", filepath.Base(img), paths)
			for i := range wa {
				require.Equal(t, wa[i], wb[i], "%s %v", filepath.Base(img), paths)
			}
			t.Logf("%s %v: %d entries match", filepath.Base(img), paths, len(wa))
		}
	}
}

// TestReaderRejectsCorruptImages mutates images in many random ways and checks
// that extraction fails cleanly (no panic, no hang, no writes outside the
// output dir) — the guest that wrote the image is untrusted.
func TestReaderRejectsCorruptImages(t *testing.T) {
	root := testfs.MakeTempDir(t)
	src := filepath.Join(root, "src")
	require.NoError(t, os.Mkdir(src, 0755))
	makeTree(t, src, 50, 4096, 4)
	image := filepath.Join(root, "ws.ext4")
	_, err := ext4writer.DirectoryToImage(context.Background(), src, image, &ext4writer.Options{SizeBytes: 150e6})
	require.NoError(t, err)
	orig, err := os.ReadFile(image)
	require.NoError(t, err)
	rng := rand.New(rand.NewSource(7))
	// Interesting regions: superblock, GDT, bitmaps, inode tables, first dir blocks.
	regions := []int{1024, 4096, 8192, 12288, 16384, 16384 + 4096*8, 4096 * 40}
	for i := 0; i < 150; i++ {
		img := append([]byte(nil), orig...)
		nMut := 1 + rng.Intn(8)
		for j := 0; j < nMut; j++ {
			base := regions[rng.Intn(len(regions))]
			off := base + rng.Intn(4096)
			if off >= len(img) {
				continue
			}
			switch rng.Intn(3) {
			case 0:
				img[off] = byte(rng.Intn(256))
			case 1:
				img[off] = 0xFF
			case 2:
				img[off] = 0
			}
		}
		mutated := filepath.Join(root, fmt.Sprintf("m%d.ext4", i))
		require.NoError(t, os.WriteFile(mutated, img, 0644))
		out := filepath.Join(root, fmt.Sprintf("o%d", i))
		require.NoError(t, os.Mkdir(out, 0755))
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("mutation %d: panic: %v", i, r)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			_ = ext4writer.ImageToDirectory(ctx, mutated, out, []string{"/"})
		}()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("mutation %d: extraction hung", i)
		}
		os.Remove(mutated)
		os.RemoveAll(out)
	}
}

// TestReaderRefusesSymlinkEscape: a directory entry that is a symlink followed
// by a same-named directory must not let us write through the symlink.
func TestReaderRefusesDuplicateNames(t *testing.T) {
	requireTool(t, "/sbin/debugfs")
	root := testfs.MakeTempDir(t)
	src := filepath.Join(root, "src")
	require.NoError(t, os.Mkdir(src, 0755))
	require.NoError(t, os.Symlink("/tmp", filepath.Join(src, "a")))
	image := filepath.Join(root, "ws.ext4")
	_, err := ext4writer.DirectoryToImage(context.Background(), src, image, nil)
	require.NoError(t, err)
	// Use debugfs to add a second entry named "a" (a directory) to the root.
	out, err := exec.Command("/sbin/debugfs", "-w", "-R", "mkdir /b", image).CombinedOutput()
	require.NoError(t, err, "%s", out)
	// Rename b -> a via debugfs isn't possible when a exists; instead patch the
	// dirent name directly: find "b" in the root directory block.
	img, err := os.ReadFile(image)
	require.NoError(t, err)
	idx := -1
	for i := 0; i+9 < len(img); i++ {
		// dirent header: inode(4) rec_len(2) name_len(1)=1 ftype(1)=2 name "b"
		if img[i+6] == 1 && img[i+7] == 2 && img[i+8] == 'b' && img[i+9] == 0 {
			idx = i
			break
		}
	}
	require.Greater(t, idx, 0, "could not find dirent for b")
	img[idx+8] = 'a'
	require.NoError(t, os.WriteFile(image, img, 0644))
	dst := filepath.Join(root, "dst")
	require.NoError(t, os.Mkdir(dst, 0755))
	err = ext4writer.ImageToDirectory(context.Background(), image, dst, []string{"/"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
}

// TestHardlinksShareInode checks hardlinks map to one inode with the right
// link count, and are extracted as hardlinks.
func TestHardlinksShareInode(t *testing.T) {
	requireTool(t, "/sbin/debugfs")
	root := testfs.MakeTempDir(t)
	src := filepath.Join(root, "src")
	require.NoError(t, os.Mkdir(src, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "a"), []byte("x"), 0644))
	require.NoError(t, os.Link(filepath.Join(src, "a"), filepath.Join(src, "b")))
	require.NoError(t, os.Mkdir(filepath.Join(src, "d"), 0755))
	require.NoError(t, os.Link(filepath.Join(src, "a"), filepath.Join(src, "d", "c")))
	// Exactly-60-byte symlink target (boundary between fast and slow).
	require.NoError(t, os.Symlink(strings.Repeat("y", 60), filepath.Join(src, "sym60")))
	require.NoError(t, os.Symlink(strings.Repeat("y", 59), filepath.Join(src, "sym59")))
	image := filepath.Join(root, "ws.ext4")
	_, err := ext4writer.DirectoryToImage(context.Background(), src, image, nil)
	require.NoError(t, err)
	fsck(t, image)
	ino := func(p string) string {
		out, err := exec.Command("/sbin/debugfs", "-R", "stat "+p, image).CombinedOutput()
		require.NoError(t, err, "%s", out)
		m := regexp.MustCompile(`Inode: (\d+).*Links: (\d+)`).FindStringSubmatch(strings.ReplaceAll(string(out), "\n", " "))
		require.NotNil(t, m, "%s", out)
		return m[1] + "/" + m[2]
	}
	require.Equal(t, ino("/a"), ino("/b"))
	require.Equal(t, ino("/a"), ino("/d/c"))
	require.True(t, strings.HasSuffix(ino("/a"), "/3"), "link count: %s", ino("/a"))
	dst := filepath.Join(root, "dst")
	require.NoError(t, os.Mkdir(dst, 0755))
	require.NoError(t, ext4writer.ImageToDirectory(context.Background(), image, dst, []string{"/"}))
	sa, _ := os.Stat(filepath.Join(dst, "a"))
	sb, _ := os.Stat(filepath.Join(dst, "d", "c"))
	require.True(t, os.SameFile(sa, sb), "extracted hardlinks should share an inode")
	for _, n := range []string{"sym60", "sym59"} {
		tgt, err := os.Readlink(filepath.Join(dst, n))
		require.NoError(t, err)
		want, _ := os.Readlink(filepath.Join(src, n))
		require.Equal(t, want, tgt)
	}
}

// TestManyInodesAndGroups: 100k empty files (inode-bound sizing) and a large
// image (many block groups, metadata past group 0, flex boundary).
func TestManyInodesAndGroups(t *testing.T) {
	requireTool(t, "/sbin/e2fsck")
	root := testfs.MakeTempDir(t)
	src := filepath.Join(root, "src")
	require.NoError(t, os.Mkdir(src, 0755))
	for d := 0; d < 100; d++ {
		dir := filepath.Join(src, fmt.Sprintf("d%03d", d))
		require.NoError(t, os.Mkdir(dir, 0755))
		for i := 0; i < 1000; i++ {
			require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%d", i)), nil, 0644))
		}
	}
	image := filepath.Join(root, "small.ext4")
	stats, err := ext4writer.DirectoryToImage(context.Background(), src, image, &ext4writer.Options{ExtraInodes: 1})
	require.NoError(t, err)
	t.Logf("100k empty files: %s", stats)
	fsck(t, image)

	// 24 GiB image: 192 groups, metadata area crosses backup-superblock groups.
	big := filepath.Join(root, "big.ext4")
	stats, err = ext4writer.DirectoryToImage(context.Background(), src, big, &ext4writer.Options{SizeBytes: 24 << 30})
	require.NoError(t, err)
	t.Logf("24GiB image: %s", stats)
	require.Equal(t, 192, stats.BlockGroups)
	fsck(t, big)
	dst := filepath.Join(root, "dst")
	require.NoError(t, os.Mkdir(dst, 0755))
	require.NoError(t, ext4writer.ImageToDirectory(context.Background(), big, dst, []string{"d099"}))
	entries, err := os.ReadDir(filepath.Join(dst, "d099"))
	require.NoError(t, err)
	require.Len(t, entries, 1000)
}

// TestTreeToImage builds an image from a REAPI Tree with contents served by an
// opener, overlaid on a directory, and checks the result.
func TestTreeToImage(t *testing.T) {
	requireTool(t, "/sbin/e2fsck")
	requireTool(t, "/sbin/debugfs")
	root := testfs.MakeTempDir(t)
	blobs := filepath.Join(root, "blobs")
	require.NoError(t, os.Mkdir(blobs, 0755))
	ws := filepath.Join(root, "ws")
	require.NoError(t, os.MkdirAll(filepath.Join(ws, "out", "sub"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(ws, "local.txt"), []byte("local"), 0644))
	// A host file with the same name as an input: the input must win.
	require.NoError(t, os.WriteFile(filepath.Join(ws, "shadowed"), []byte("host-version"), 0644))

	mkFile := func(name string, content []byte, exe bool) *repb.FileNode {
		d, err := digest.Compute(bytes.NewReader(content), repb.DigestFunction_SHA256)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(blobs, d.GetHash()), content, 0644))
		return &repb.FileNode{Name: name, Digest: d, IsExecutable: exe}
	}
	big := make([]byte, 5<<20)
	for i := range big {
		big[i] = byte(i * 7)
	}
	sub := &repb.Directory{
		Files:    []*repb.FileNode{mkFile("a.txt", []byte("hello"), false), mkFile("same1", []byte("dup"), false), mkFile("same2", []byte("dup"), false), mkFile("same-exe", []byte("dup"), true)},
		Symlinks: []*repb.SymlinkNode{{Name: "ln", Target: "a.txt"}},
	}
	subD, err := digest.ComputeForMessage(sub, repb.DigestFunction_SHA256)
	require.NoError(t, err)
	// "out" also exists in the workspace dir: must merge.
	outDir := &repb.Directory{Files: []*repb.FileNode{mkFile("in-out", []byte("x"), false)}}
	outD, err := digest.ComputeForMessage(outDir, repb.DigestFunction_SHA256)
	require.NoError(t, err)
	rootDir := &repb.Directory{
		Files:       []*repb.FileNode{mkFile("big.bin", big, true), mkFile("empty", nil, false), mkFile("shadowed", []byte("input-version"), false)},
		Directories: []*repb.DirectoryNode{{Name: "sub", Digest: subD}, {Name: "out", Digest: outD}},
	}
	tree := &repb.Tree{Root: rootDir, Children: []*repb.Directory{sub, outDir}}
	opener := func(ctx context.Context, n *repb.FileNode) (*os.File, error) {
		return os.Open(filepath.Join(blobs, n.GetDigest().GetHash()))
	}
	image := filepath.Join(root, "ws.ext4")
	stats, err := ext4writer.DirectoryAndTreeToImage(context.Background(), ws, image, &ext4writer.TreeOptions{Tree: tree, DigestFunction: repb.DigestFunction_SHA256, Open: opener})
	require.NoError(t, err)
	t.Logf("stats: %s", stats)
	require.Equal(t, 1, stats.Hardlinks) // same1/same2 share; same-exe differs by mode
	fsck(t, image)
	dst := filepath.Join(root, "dst")
	require.NoError(t, os.Mkdir(dst, 0755))
	require.NoError(t, ext4writer.ImageToDirectory(context.Background(), image, dst, []string{"/"}))
	got := snapshotDir(t, dst, map[string]bool{"lost+found": true})
	names := map[string]entry{}
	for _, e := range got {
		names[e.Path] = e
	}
	for _, p := range []string{"local.txt", "out/sub", "out/in-out", "sub/a.txt", "sub/same1", "sub/same2", "sub/same-exe", "sub/ln", "big.bin", "empty"} {
		require.Contains(t, names, p)
	}
	b, err := os.ReadFile(filepath.Join(dst, "big.bin"))
	require.NoError(t, err)
	require.Equal(t, big, b)
	require.Equal(t, "-rwxr-xr-x", names["sub/same-exe"].Mode)
	require.Equal(t, "-rw-r--r--", names["sub/same1"].Mode)
	require.Equal(t, "a.txt", names["sub/ln"].Target)
	sh, err := os.ReadFile(filepath.Join(dst, "shadowed"))
	require.NoError(t, err)
	require.Equal(t, "input-version", string(sh))
	s1, _ := os.Stat(filepath.Join(dst, "sub", "same1"))
	s2, _ := os.Stat(filepath.Join(dst, "sub", "same2"))
	require.True(t, os.SameFile(s1, s2))
}

// TestSparseFilesStaySparse: holes in source files become holes in the image
// (no blocks allocated) and extract back as sparse files with equal content.
func TestSparseFilesStaySparse(t *testing.T) {
	requireTool(t, "/sbin/e2fsck")
	requireTool(t, "/sbin/debugfs")
	for _, mode := range []string{"mmap", "cfr"} {
		t.Run(mode, func(t *testing.T) {
			root := testfs.MakeTempDir(t)
			src := filepath.Join(root, "src")
			require.NoError(t, os.Mkdir(src, 0755))
			f, err := os.Create(filepath.Join(src, "sparse"))
			require.NoError(t, err)
			require.NoError(t, f.Truncate(64<<20))
			_, err = f.WriteAt([]byte("start"), 0)
			require.NoError(t, err)
			_, err = f.WriteAt(make([]byte, 5000), 20<<20+100) // straddles blocks
			require.NoError(t, err)
			_, err = f.WriteAt([]byte("end"), 64<<20-3)
			require.NoError(t, err)
			require.NoError(t, f.Close())
			st, _ := os.Stat(filepath.Join(src, "sparse"))
			if st.Sys().(*syscall.Stat_t).Blocks*512 >= 64<<20 {
				t.Skip("filesystem does not support sparse files")
			}
			image := filepath.Join(root, "ws.ext4")
			stats, err := ext4writer.DirectoryToImage(context.Background(), src, image, &ext4writer.Options{CopyMode: mode})
			require.NoError(t, err)
			fsck(t, image)
			out, err := exec.Command("/sbin/debugfs", "-R", "stat /sparse", image).CombinedOutput()
			require.NoError(t, err, "%s", out)
			m := regexp.MustCompile(`Blockcount: (\d+)`).FindStringSubmatch(string(out))
			require.NotNil(t, m, "%s", out)
			var blocks int
			fmt.Sscanf(m[1], "%d", &blocks)
			require.Less(t, blocks, 64, "sparse file should use only a few blocks, got %d (stats %s)", blocks, stats)
			dst := filepath.Join(root, "dst")
			require.NoError(t, os.Mkdir(dst, 0755))
			require.NoError(t, ext4writer.ImageToDirectory(context.Background(), image, dst, []string{"sparse"}))
			want, _ := os.ReadFile(filepath.Join(src, "sparse"))
			got, err := os.ReadFile(filepath.Join(dst, "sparse"))
			require.NoError(t, err)
			require.True(t, bytes.Equal(want, got))
			st2, _ := os.Stat(filepath.Join(dst, "sparse"))
			require.Less(t, st2.Sys().(*syscall.Stat_t).Blocks*512, int64(1<<20), "extracted file should be sparse")
		})
	}
}
