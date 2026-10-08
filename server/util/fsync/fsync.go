// Package fsync provides utilities for durably writing files to disk.
// It can be useful in cases where you need to ensure that
// file operations are not only visible in the FS layer, but also
// flushed to the storage layer.
package fsync

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

// Syncer is a function that syncs a path relative to a Root to disk.
type Syncer func(path string) error

// Root tracks filesystem operations within a root directory and syncs them
// all at once when Sync is called. This is more efficient than syncing after
// each operation. Operation paths are interpreted relative to the root.
type Root struct {
	root   *os.Root
	paths  map[string]struct{}
	synced map[string]struct{}
	syncer Syncer
}

// NewRoot creates a Root that will track and sync paths within the given root
// directory. The root directory itself will be synced when Sync is called.
// If syncer is nil, paths are synced through os.Root.
func NewRoot(root string, syncer Syncer) (*Root, error) {
	or, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	r := &Root{
		root:   or,
		paths:  make(map[string]struct{}),
		synced: make(map[string]struct{}),
	}
	if syncer == nil {
		syncer = r.syncPath
	}
	r.syncer = syncer
	r.add(".")
	return r, nil
}

// Close closes the underlying os.Root.
func (r *Root) Close() error {
	return r.root.Close()
}

func (r *Root) localPath(path string) string {
	if path == "" {
		return "."
	}
	return filepath.Clean(path)
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

// MkdirAll creates a directory and any necessary parents.
func (r *Root) MkdirAll(path string, mode os.FileMode) error {
	path = r.localPath(path)
	if err := r.root.MkdirAll(path, mode); err != nil {
		return err
	}
	r.add(path)
	return nil
}

// Chown changes ownership of a file or directory.
func (r *Root) Chown(path string, uid, gid int) error {
	path = r.localPath(path)
	if err := r.root.Chown(path, uid, gid); err != nil {
		return err
	}
	r.add(path)
	return nil
}

// Lchown changes ownership of a file or symlink.
func (r *Root) Lchown(path string, uid, gid int) error {
	path = r.localPath(path)
	if err := r.root.Lchown(path, uid, gid); err != nil {
		return err
	}
	r.addParent(path)
	return nil
}

// Chmod changes mode bits of a file or directory.
func (r *Root) Chmod(path string, mode os.FileMode) error {
	path = r.localPath(path)
	if err := r.root.Chmod(path, mode); err != nil {
		return err
	}
	r.add(path)
	return nil
}

// Chtimes changes access and modification times of a file or directory.
func (r *Root) Chtimes(path string, atime, mtime time.Time) error {
	path = r.localPath(path)
	if err := r.root.Chtimes(path, atime, mtime); err != nil {
		return err
	}
	r.add(path)
	return nil
}

// Lchtimes changes a symlink's access and modification times without following
// it, and tracks its parent directory for syncing. Use Chtimes for regular files
// and directories so their inodes are synced as well.
func (r *Root) Lchtimes(path string, atime, mtime time.Time) error {
	path = r.localPath(path)
	// In particular, reject ".." before passing the final component directly
	// to utimensat. os.Root only resolves the parent directory below.
	if !filepath.IsLocal(path) {
		return &os.PathError{Op: "chtimes", Path: path, Err: unix.EINVAL}
	}
	var ts [2]unix.Timespec
	for i, t := range []time.Time{atime, mtime} {
		var err error
		ts[i], err = unix.TimeToTimespec(t)
		if err != nil {
			return err
		}
	}
	// Resolve the parent through os.Root, then operate on only the final path
	// component so archive symlinks cannot redirect the update outside the root.
	f, err := r.root.OpenFile(filepath.Dir(path), os.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := unix.UtimesNanoAt(int(f.Fd()), filepath.Base(path), ts[:], unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return &os.PathError{Op: "utimensat", Path: path, Err: err}
	}
	r.addParent(path)
	return nil
}

// CreateFile creates a file, writes data to it, and sets ownership, mode, and
// timestamps. If mtime is nonzero, both access and modification times are set to
// it before closing the file.
func (r *Root) CreateFile(path string, mode os.FileMode, data io.Reader, uid, gid int, mtime time.Time) error {
	path = r.localPath(path)
	f, err := r.root.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode.Perm())
	if err != nil {
		return err
	}

	if _, err := io.Copy(f, data); err != nil {
		f.Close()
		return err
	}
	if err := f.Chown(uid, gid); err != nil {
		f.Close()
		return err
	}
	if err := f.Chmod(mode); err != nil {
		f.Close()
		return err
	}
	if !mtime.IsZero() {
		if err := r.chtimesFile(f, path, mtime); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	r.add(path)
	r.addParent(path)
	return nil
}

// Symlink creates a symbolic link.
func (r *Root) Symlink(oldname, newname string) error {
	newname = r.localPath(newname)
	if err := r.root.Symlink(oldname, newname); err != nil {
		return err
	}
	r.addParent(newname)
	return nil
}

// Link creates a hard link.
func (r *Root) Link(oldname, newname string) error {
	oldname = r.localPath(oldname)
	newname = r.localPath(newname)
	if err := r.root.Link(oldname, newname); err != nil {
		return err
	}
	r.addParent(newname)
	return nil
}

// Mknod creates a device node.
func (r *Root) Mknod(path string, mode uint32, dev int) error {
	path = r.localPath(path)
	f, err := r.root.OpenFile(filepath.Dir(path), os.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return err
	}
	if err := mknodAt(int(f.Fd()), filepath.Base(path), mode, dev); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	r.addParent(path)
	return nil
}

// Setxattr sets an extended attribute.
func (r *Root) Setxattr(path string, attr string, data []byte, flags int) error {
	path = r.localPath(path)
	f, err := r.root.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	if err := unix.Fsetxattr(int(f.Fd()), attr, data, flags); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
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
		if cur == "" {
			break
		}
		if _, ok := r.synced[cur]; ok {
			break
		}
		if err := r.syncer(cur); err != nil {
			return err
		}
		r.synced[cur] = struct{}{}
		if cur == "." {
			break
		}

		parent := filepath.Dir(cur)
		if parent == cur {
			break
		}
		cur = parent
	}
	return nil
}

func (r *Root) syncPath(path string) error {
	f, err := r.root.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
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
