package fsync_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/fsync"
	"github.com/stretchr/testify/assert"
)

func TestRootMkdirAll(t *testing.T) {
	root := t.TempDir()
	r, err := fsync.NewRoot(root, nil)
	if err != nil {
		t.Fatalf("new root: %v", err)
	}
	defer r.Close()

	dir := "subdir"
	if err := r.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	info, err := os.Stat(filepath.Join(root, dir))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("expected directory")
	}

	if err := r.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func TestRootSymlink(t *testing.T) {
	root := t.TempDir()
	r, err := fsync.NewRoot(root, nil)
	if err != nil {
		t.Fatalf("new root: %v", err)
	}
	defer r.Close()

	link := "link"
	if err := r.Symlink("target", link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	target, err := os.Readlink(filepath.Join(root, link))
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if target != "target" {
		t.Fatalf("expected target %q, got %q", "target", target)
	}

	if err := r.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func TestRootLink(t *testing.T) {
	root := t.TempDir()
	r, err := fsync.NewRoot(root, nil)
	if err != nil {
		t.Fatalf("new root: %v", err)
	}
	defer r.Close()

	// Create original file
	original := "original"
	if err := os.WriteFile(filepath.Join(root, original), []byte("data"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	link := "link"
	if err := r.Link(original, link); err != nil {
		t.Fatalf("link: %v", err)
	}

	// Verify link exists and has same content
	data, err := os.ReadFile(filepath.Join(root, link))
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(data) != "data" {
		t.Fatalf("expected data %q, got %q", "data", string(data))
	}

	if err := r.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func TestRootCreateFile(t *testing.T) {
	root := t.TempDir()
	r, err := fsync.NewRoot(root, nil)
	if err != nil {
		t.Fatalf("new root: %v", err)
	}
	defer r.Close()

	path := "file.txt"
	content := []byte("hello world")
	mtime := time.Unix(1700000000, 123456789)
	if err := r.CreateFile(path, 0644, bytes.NewReader(content), os.Getuid(), os.Getgid(), mtime); err != nil {
		t.Fatalf("create file: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(data) != string(content) {
		t.Fatalf("expected content %q, got %q", content, data)
	}

	info, err := os.Stat(filepath.Join(root, path))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0644 {
		t.Fatalf("expected mode 0644, got %o", info.Mode().Perm())
	}
	assert.True(t, mtime.Equal(info.ModTime()), "mtime: got %s, want %s", info.ModTime(), mtime)

	if err := r.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func TestSyncOrder(t *testing.T) {
	root := t.TempDir()
	var synced []string
	syncer := func(path string) error {
		synced = append(synced, path)
		return nil
	}
	r, err := fsync.NewRoot(root, syncer)
	if err != nil {
		t.Fatalf("new root: %v", err)
	}
	defer r.Close()

	// Create nested directories in various orders
	if err := r.MkdirAll("a", 0755); err != nil {
		t.Fatalf("mkdir a: %v", err)
	}
	if err := r.MkdirAll(filepath.Join("a", "b"), 0755); err != nil {
		t.Fatalf("mkdir a/b: %v", err)
	}
	if err := r.MkdirAll(filepath.Join("a", "b", "c"), 0755); err != nil {
		t.Fatalf("mkdir a/b/c: %v", err)
	}
	if err := r.MkdirAll("x", 0755); err != nil {
		t.Fatalf("mkdir x: %v", err)
	}

	if err := r.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// All tracked paths and their ancestors up to root should be synced.
	expected := []string{
		filepath.Join("a", "b", "c"),
		filepath.Join("a", "b"),
		"a",
		".",
		"x",
	}

	assert.ElementsMatch(t, expected, synced)
}

func TestRootRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	escapeDir := t.TempDir()
	escapeFile := filepath.Join(escapeDir, "pwned.txt")
	r, err := fsync.NewRoot(root, nil)
	if err != nil {
		t.Fatalf("new root: %v", err)
	}
	defer r.Close()

	if err := r.Symlink(escapeDir, "link"); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	err = r.CreateFile(filepath.Join("link", "pwned.txt"), 0644, bytes.NewReader([]byte("pwned")), os.Getuid(), os.Getgid(), time.Unix(0, 0))
	if err == nil {
		t.Fatal("expected symlink escape to fail")
	}
	_, err = os.Stat(escapeFile)
	assert.True(t, os.IsNotExist(err), "file outside root should not be created")
}

func TestRootChtimes(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(outside, nil, 0644); err != nil {
		t.Fatal(err)
	}
	originalTime := time.Unix(1234567890, 0)
	if err := os.Chtimes(outside, originalTime, originalTime); err != nil {
		t.Fatal(err)
	}
	var synced []string
	r, err := fsync.NewRoot(root, func(path string) error {
		synced = append(synced, path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := os.WriteFile(filepath.Join(root, "file"), nil, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "dir"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing", filepath.Join(root, "dangling")); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"file", "dir", "link", "dangling", "."} {
		t.Run(path, func(t *testing.T) {
			chtimes := r.Chtimes
			if path == "link" || path == "dangling" {
				chtimes = r.Lchtimes
			}
			for _, mtime := range []time.Time{time.Unix(1700000000, 123456789), time.Unix(0, 0)} {
				if err := chtimes(path, mtime, mtime); err != nil {
					t.Fatal(err)
				}
				info, err := os.Lstat(filepath.Join(root, path))
				if err != nil {
					t.Fatal(err)
				}
				assert.True(t, mtime.Equal(info.ModTime()), "mtime: got %s, want %s", info.ModTime(), mtime)
			}
		})
	}
	info, err := os.Stat(outside)
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, originalTime.Equal(info.ModTime()), "symlink target mtime changed")
	if err := r.Sync(); err != nil {
		t.Fatal(err)
	}
	assert.ElementsMatch(t, []string{".", "file", "dir"}, synced)
}

func TestRootChtimesRejectsEscape(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	if err := os.Mkdir(root, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(parent, filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	r, err := fsync.NewRoot(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	mtime := time.Unix(1234567890, 0)
	if err := os.Chtimes(parent, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"..", "../root/..", filepath.Join("escape", "root"), parent} {
		assert.Error(t, r.Lchtimes(path, time.Unix(0, 0), time.Unix(0, 0)), path)
		assert.Error(t, r.Chtimes(path, time.Unix(0, 0), time.Unix(0, 0)), path)
	}
	info, err := os.Stat(parent)
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, mtime.Equal(info.ModTime()), "mtime outside root changed")
}

// BenchmarkExtractFiles compares the cost of timestamp restoration while
// extracting batches of new files, including syncing them to disk. PathTimes
// uses the same timestamp operation as Moby's extractor (os.Root.Chtimes after
// closing the file); the rest of the extraction is identical in all cases.
// This isolates timestamp handling, rather than comparing entire OCI runtimes.
func BenchmarkExtractFiles(b *testing.B) {
	const fileCount = 256
	data := bytes.Repeat([]byte("x"), 1024)
	mtime := time.Unix(1700000000, 123456789)
	uid, gid := os.Getuid(), os.Getgid()
	for _, layout := range []struct{ name, dir string }{
		{"Flat", "."},
		{"Nested", "usr/local/lib/python/site-packages/pkg"},
	} {
		for _, method := range []string{"NoTimes", "PathTimes", "CreateFileTimes"} {
			b.Run(layout.name+"/"+method, func(b *testing.B) {
				layerDir := filepath.Join(b.TempDir(), "layer")
				paths := make([]string, fileCount)
				for i := range paths {
					paths[i] = filepath.Join(layout.dir, fmt.Sprintf("file-%d", i))
				}
				var createTime time.Time
				if method == "CreateFileTimes" {
					createTime = mtime
				}
				b.ReportAllocs()
				b.SetBytes(int64(fileCount * len(data)))
				b.ResetTimer()
				for range b.N {
					b.StopTimer()
					if err := os.MkdirAll(filepath.Join(layerDir, layout.dir), 0755); err != nil {
						b.Fatal(err)
					}
					r, err := fsync.NewRoot(layerDir, nil)
					if err != nil {
						b.Fatal(err)
					}
					or, err := os.OpenRoot(layerDir)
					if err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
					for _, path := range paths {
						if err := r.CreateFile(path, 0644, bytes.NewReader(data), uid, gid, createTime); err != nil {
							b.Fatal(err)
						}
						if method == "PathTimes" {
							if err := or.Chtimes(path, mtime, mtime); err != nil {
								b.Fatal(err)
							}
						}
					}
					if err := r.Sync(); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					if err := or.Close(); err != nil {
						b.Fatal(err)
					}
					if err := r.Close(); err != nil {
						b.Fatal(err)
					}
					if err := os.RemoveAll(layerDir); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
				b.ReportMetric(fileCount, "files/op")
			})
		}
	}
}

// Note: not testing Setxattr or Mknod since they may require certain perms.
