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
	if err := r.CreateFile(path, 0644, bytes.NewReader(content), os.Getuid(), os.Getgid()); err != nil {
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
	err = r.CreateFile(filepath.Join("link", "pwned.txt"), 0644, bytes.NewReader([]byte("pwned")), os.Getuid(), os.Getgid())
	if err == nil {
		t.Fatal("expected symlink escape to fail")
	}
	_, err = os.Stat(escapeFile)
	assert.True(t, os.IsNotExist(err), "file outside root should not be created")
}

// Note: not testing Setxattr or Mknod since they may require certain perms.
