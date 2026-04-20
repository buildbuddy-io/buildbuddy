package fsync_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/fsync"
	"github.com/stretchr/testify/assert"
)

func TestRootMkdirAll(t *testing.T) {
	root := t.TempDir()
	r := fsync.NewRoot(root, nil)

	dir := filepath.Join(root, "subdir")
	if err := r.MkdirAll(dir, 0755, os.Getuid(), os.Getgid()); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	info, err := os.Stat(dir)
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
	r := fsync.NewRoot(root, nil)

	link := filepath.Join(root, "link")
	if err := r.Symlink("target", link, os.Getuid(), os.Getgid()); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	target, err := os.Readlink(link)
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
	r := fsync.NewRoot(root, nil)

	// Create original file
	original := filepath.Join(root, "original")
	if err := os.WriteFile(original, []byte("data"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	link := filepath.Join(root, "link")
	if err := r.Link(original, link); err != nil {
		t.Fatalf("link: %v", err)
	}

	// Verify link exists and has same content
	data, err := os.ReadFile(link)
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
	r := fsync.NewRoot(root, nil)

	path := filepath.Join(root, "file.txt")
	content := []byte("hello world")
	if err := r.CreateFile(path, 0644, bytes.NewReader(content), os.Getuid(), os.Getgid()); err != nil {
		t.Fatalf("create file: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(data) != string(content) {
		t.Fatalf("expected content %q, got %q", content, data)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0644 {
		t.Fatalf("expected mode 0644, got %o", info.Mode().Perm())
	}

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
	r := fsync.NewRoot(root, syncer)

	uid, gid := os.Getuid(), os.Getgid()

	// Create nested directories in various orders
	if err := r.MkdirAll(filepath.Join(root, "a"), 0755, uid, gid); err != nil {
		t.Fatalf("mkdir a: %v", err)
	}
	if err := r.MkdirAll(filepath.Join(root, "a/b"), 0755, uid, gid); err != nil {
		t.Fatalf("mkdir a/b: %v", err)
	}
	if err := r.MkdirAll(filepath.Join(root, "a/b/c"), 0755, uid, gid); err != nil {
		t.Fatalf("mkdir a/b/c: %v", err)
	}
	if err := r.MkdirAll(filepath.Join(root, "x"), 0755, uid, gid); err != nil {
		t.Fatalf("mkdir x: %v", err)
	}

	if err := r.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// All tracked paths and their ancestors up to root should be synced.
	expected := []string{
		filepath.Join(root, "a/b/c"),
		filepath.Join(root, "a/b"),
		filepath.Join(root, "a"),
		root,
		filepath.Join(root, "x"),
	}

	assert.ElementsMatch(t, expected, synced)
}

// Note: not testing Setxattr or Mknod since they may require certain perms.
