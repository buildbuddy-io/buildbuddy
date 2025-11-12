// Package fsync provides utilities for durably writing files to disk.
// It can be useful in cases where you need to ensure that
// file operations are not only visible in the FS layer, but also
// flushed to the storage layer.
package fsync

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// SyncPath opens a path and syncs it to disk.
func SyncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open path for sync: %w", err)
	}
	defer f.Close()
	return f.Sync()
}

// SyncParentDir syncs the parent directory of a path to disk.
func SyncParentDir(path string) error {
	return SyncPath(filepath.Dir(path))
}

// MkdirAllAndSync creates a directory tree and syncs all created directories up to a specified root directory.
// The rootDir parameter specifies where to stop recursing upward after syncing it.
// The relativePath parameter is a child path relative to rootDir.
func MkdirAllAndSync(rootDir string, relativePath string, mode os.FileMode) error {
	if rootDir == "" {
		return nil
	}

	cleanRoot := filepath.Clean(rootDir)

	// If relativePath is empty, we're just creating/syncing the root directory itself
	var fullPath string
	if relativePath == "" {
		fullPath = cleanRoot
	} else {
		fullPath = filepath.Join(cleanRoot, relativePath)
	}

	// Create all directories from rootDir to fullPath
	if err := os.MkdirAll(fullPath, mode); err != nil {
		return err
	}

	// Sync from fullPath up to and including rootDir, then stop
	for dir := fullPath; ; {
		if err := SyncPath(dir); err != nil {
			return err
		}

		// Stop after syncing rootDir
		if dir == cleanRoot {
			break
		}

		parent := filepath.Dir(dir)
		// Safety check: if we've reached filesystem root without hitting cleanRoot, break
		if parent == dir {
			break
		}

		dir = parent
	}

	// Sync the parent of rootDir to persist the rootDir directory entry.
	// This ensures that if rootDir was created during this call, the parent
	// directory entry is persisted to disk.
	if err := SyncParentDir(cleanRoot); err != nil {
		return err
	}

	return nil
}

// CreateFileAndSync creates a file, writes data to it, sets ownership, syncs both the file
// and its parent directory, then closes the file.
func CreateFileAndSync(path string, mode os.FileMode, data io.Reader, uid, gid int) error {
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

	if err := f.Sync(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	if err := SyncParentDir(path); err != nil {
		return err
	}

	return nil
}

// SymlinkAndSync creates a symbolic link and syncs the parent directory.
func SymlinkAndSync(oldname, newname string) error {
	if err := os.Symlink(oldname, newname); err != nil {
		return err
	}
	return SyncParentDir(newname)
}

// LinkAndSync creates a hard link and syncs the parent directory.
func LinkAndSync(oldname, newname string) error {
	if err := os.Link(oldname, newname); err != nil {
		return err
	}
	return SyncParentDir(newname)
}

// ChownAndSync changes ownership of a path and syncs the path to disk.
func ChownAndSync(path string, uid, gid int) error {
	if err := os.Chown(path, uid, gid); err != nil {
		return err
	}
	return SyncPath(path)
}

// LchownAndSync changes ownership of a symlink and syncs the parent directory.
func LchownAndSync(path string, uid, gid int) error {
	if err := os.Lchown(path, uid, gid); err != nil {
		return err
	}
	return SyncParentDir(path)
}

// MknodAndSync creates a device node and syncs the parent directory.
func MknodAndSync(path string, mode uint32, dev int) error {
	if err := unix.Mknod(path, mode, dev); err != nil {
		return err
	}
	return SyncParentDir(path)
}

// SetxattrAndSync sets an extended attribute and syncs the path to disk.
func SetxattrAndSync(path string, attr string, data []byte, flags int) error {
	if err := unix.Setxattr(path, attr, data, flags); err != nil {
		return err
	}
	return SyncPath(path)
}

// RenameAndSync renames a path and syncs both the source and destination parent directories.
func RenameAndSync(oldpath, newpath string) error {
	if err := os.Rename(oldpath, newpath); err != nil {
		return err
	}
	if err := SyncParentDir(newpath); err != nil {
		return err
	}
	if err := SyncParentDir(oldpath); err != nil {
		return err
	}
	return nil
}
