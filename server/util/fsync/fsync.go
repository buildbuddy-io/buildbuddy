package fsync

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// SyncFd syncs a file descriptor to disk.
func SyncFd(fd *os.File) error {
	if err := fd.Sync(); err != nil {
		return fmt.Errorf("sync fd: %w", err)
	}
	return nil
}

// SyncPath opens a path and syncs it to disk.
func SyncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open path for sync: %w", err)
	}
	defer f.Close()
	return SyncFd(f)
}

// SyncParentDir syncs the parent directory of a path to disk.
func SyncParentDir(path string) error {
	return SyncPath(filepath.Dir(path))
}

// MkdirAllAndSync creates a directory tree and syncs all created directories and their parents to disk.
// This ensures that if we create a/b/c, we sync a, b, c, and all their parents.
func MkdirAllAndSync(path string, mode os.FileMode) error {
	if err := os.MkdirAll(path, mode); err != nil {
		return err
	}

	// Sync all directories from the path up to the root
	// We sync each directory and its parent to ensure both the directory
	// metadata and the directory entry in the parent are persisted.
	current := path
	for {
		// Sync the current directory
		if err := SyncPath(current); err != nil {
			return err
		}

		// Sync the parent directory to persist the directory entry
		parent := filepath.Dir(current)
		if parent == current {
			// Reached the root (e.g., "/") - already synced
			break
		}
		if parent == "." || parent == "/" {
			// Reached the boundary - sync the parent directory to persist the entry
			if err := SyncPath(parent); err != nil {
				return err
			}
			break
		}

		// Move up to the parent for the next iteration
		current = parent
	}

	return nil
}

// CreateFileAndSync creates a file, writes data to it, sets ownership, syncs both the file
// and its parent directory, then closes the file.
func CreateFileAndSync(path string, mode os.FileMode, data io.Reader, uid, gid int) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, data); err != nil {
		return fmt.Errorf("write file content: %w", err)
	}

	if err := f.Chown(uid, gid); err != nil {
		return fmt.Errorf("chown file: %w", err)
	}

	if err := SyncFd(f); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
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
	// Sync the destination parent directory
	if err := SyncParentDir(newpath); err != nil {
		return err
	}
	// Sync the source parent directory
	if err := SyncParentDir(oldpath); err != nil {
		return err
	}
	return nil
}
