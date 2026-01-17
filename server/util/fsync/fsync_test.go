package fsync

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRootMkdir(t *testing.T) {
	root := t.TempDir()
	r := NewRoot(root)

	dir := filepath.Join(root, "subdir")
	if err := r.Mkdir(dir, 0755, os.Getuid(), os.Getgid()); err != nil {
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
	r := NewRoot(root)

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
	r := NewRoot(root)

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
