package ext4writer_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4writer"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"
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
	requireTool(t, "/sbin/e2fsck")
	requireTool(t, "/sbin/debugfs")
	root := testfs.MakeTempDir(t)
	src := filepath.Join(root, "src")
	require.NoError(t, os.Mkdir(src, 0755))
	makeTree(t, src, 2000, 16*1024, 1)

	image := filepath.Join(root, "ws.ext4")
	stats, err := ext4writer.DirectoryToImage(context.Background(), src, image, &ext4writer.Options{SizeBytes: 300e6})
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
