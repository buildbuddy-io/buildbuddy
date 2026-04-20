// Package fsync provides utilities for durably writing files to disk.
// It can be useful in cases where you need to ensure that
// file operations are not only visible in the FS layer, but also
// flushed to the storage layer.
package fsync

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// Syncer is a function that syncs a path to disk.
type Syncer func(path string) error

// Root tracks filesystem operations within a root directory and syncs them
// all at once when Sync is called. This is more efficient than syncing after
// each operation.
type Root struct {
	root   string
	paths  map[string]struct{}
	synced map[string]struct{}
	syncer Syncer
}

// NewRoot creates a Root that will track and sync paths within the given root
// directory. The root directory itself will be synced when Sync is called.
// If syncer is nil, SyncPath is used.
func NewRoot(root string, syncer Syncer) *Root {
	if syncer == nil {
		syncer = SyncPath
	}
	r := &Root{
		root:   filepath.Clean(root),
		paths:  make(map[string]struct{}),
		synced: make(map[string]struct{}),
		syncer: syncer,
	}
	r.add(r.root)
	return r
}

// add registers a path to be synced when Sync is called.
func (r *Root) add(path string) {
	if path != "" {
		r.paths[filepath.Clean(path)] = struct{}{}
	}
}

// addParent registers the parent directory of a path to be synced.
func (r *Root) addParent(path string) {
	r.add(filepath.Dir(path))
}

// MkdirAll creates a directory (and any necessary parents) and sets ownership.
func (r *Root) MkdirAll(path string, mode os.FileMode, uid, gid int) error {
	if err := os.MkdirAll(path, mode); err != nil {
		return err
	}
	if err := os.Chown(path, uid, gid); err != nil {
		return err
	}
	r.add(path)
	return nil
}

// CreateFile creates a file, writes data to it, and sets ownership.
func (r *Root) CreateFile(path string, mode os.FileMode, data io.Reader, uid, gid int) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, data); err != nil {
		return err
	}
	if err := f.Chown(uid, gid); err != nil {
		return err
	}
	r.add(path)
	r.addParent(path)
	return nil
}

// Symlink creates a symbolic link and sets ownership.
func (r *Root) Symlink(oldname, newname string, uid, gid int) error {
	if err := os.Symlink(oldname, newname); err != nil {
		return err
	}
	if err := os.Lchown(newname, uid, gid); err != nil {
		return err
	}
	r.addParent(newname)
	return nil
}

// Link creates a hard link.
func (r *Root) Link(oldname, newname string) error {
	if err := os.Link(oldname, newname); err != nil {
		return err
	}
	r.addParent(newname)
	return nil
}

// Mknod creates a device node.
func (r *Root) Mknod(path string, mode uint32, dev int) error {
	if err := unix.Mknod(path, mode, dev); err != nil {
		return err
	}
	r.addParent(path)
	return nil
}

// Setxattr sets an extended attribute.
func (r *Root) Setxattr(path string, attr string, data []byte, flags int) error {
	if err := unix.Setxattr(path, attr, data, flags); err != nil {
		return err
	}
	r.add(path)
	return nil
}

// Sync syncs all tracked paths.
func (r *Root) Sync() error {
	for path := range r.paths {
		if err := r.syncUpwards(path); err != nil {
			return err
		}
	}
	return nil
}

func (r *Root) syncUpwards(path string) error {
	for cur := filepath.Clean(path); ; {
		if cur == "" || cur == "." {
			break
		}
		if _, ok := r.synced[cur]; ok {
			break
		}
		if r.root != "" {
			if cur == r.root {
				if _, explicitlyTracked := r.paths[cur]; explicitlyTracked {
					if err := r.syncer(cur); err != nil {
						return err
					}
				}
				r.synced[cur] = struct{}{}
				break
			}
			if !strings.HasPrefix(cur, r.root+string(os.PathSeparator)) {
				break
			}
		}
		if err := r.syncer(cur); err != nil {
			return err
		}
		r.synced[cur] = struct{}{}

		parent := filepath.Dir(cur)
		if parent == cur {
			break
		}
		cur = parent
	}
	return nil
}

// SyncPath opens a path and syncs it to disk.
func SyncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
