//go:build linux && !android

package overlayfs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
)

const (
	workSuffix  = ".work"
	upperSuffix = ".upper"
	lowerSuffix = ".lower"
)

// Convert converts the given directory to an overlayfs-backed directory, so
// that any writes to the directory result in a copy-up to an initially empty
// "upper" directory, rather than affecting the original directory contents.
//
// It does the following:
//   - Renames path to "${path}.lower"
//   - Creates empty dirs "${path}.work" and "${path}.upper" for the overlay
//     work and upper dirs, respectively.
//   - Creates an overlayfs mount at "${path}".
func Convert(ctx context.Context, path string, opts Opts) (*Overlay, error) {
	_, span := tracing.StartSpan(ctx)
	defer span.End()

	fs := &Overlay{
		MountDir: path,
		LowerDir: path + lowerSuffix,
		WorkDir:  path + workSuffix,
		UpperDir: path + upperSuffix,
		opts:     opts,
	}

	if opts.DirPerms == 0 {
		opts.DirPerms = 0755
	}

	if err := os.Rename(path, fs.LowerDir); err != nil {
		return nil, status.WrapError(err, "create overlay workspace lower dir")
	}
	for _, p := range []string{fs.MountDir, fs.WorkDir, fs.UpperDir} {
		if err := os.Mkdir(p, opts.DirPerms); err != nil {
			return nil, status.WrapError(err, "create overlay directory")
		}
	}
	args := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", fs.LowerDir, fs.UpperDir, fs.WorkDir)
	if err := syscall.Mount("", fs.MountDir, "overlay", syscall.MS_RELATIME, args); err != nil {
		return nil, status.WrapError(err, "mount overlayfs")
	}
	return fs, nil
}

// Remove removes an overlay workspace, including all backing directories, and
// ensures that the overlay directory is unmounted.
func (o *Overlay) Remove(ctx context.Context) error {
	if err := syscall.Unmount(o.MountDir, 0); err != nil {
		return status.WrapErrorf(err, "unmount %s", o.MountDir)
	}
	var lastErr error
	for _, p := range []string{o.MountDir, o.LowerDir, o.WorkDir, o.UpperDir} {
		if err := disk.ForceRemove(ctx, p); err != nil {
			lastErr = status.WrapErrorf(err, "force remove %s", p)
		}
	}
	return lastErr
}

// Apply applies all changes from the upperdir to the lowerdir, and clears the
// upperdir.
//
// This is intended to be called after a task is run so that outputs can be
// safely hardlinked to the filecache.
func (o *Overlay) Apply(ctx context.Context, opts ApplyOpts) error {
	_, span := tracing.StartSpan(ctx)
	defer span.End()

	// Walk the upper dir and move all files into lowerdir, or if the file is a
	// whiteout, delete from lowerdir.
	err := filepath.WalkDir(o.UpperDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return status.WrapError(err, "walk dir")
		}

		relpath := strings.TrimPrefix(path, o.UpperDir)
		lowerPath := filepath.Join(o.LowerDir, relpath)

		// If "ignored" (deleted from the overlayfs), delete from lowerdir.
		ignored, err := isIgnored(path)
		if err != nil {
			return status.WrapError(err, "check whether path is ignored")
		}
		if ignored {
			if err := os.RemoveAll(lowerPath); err != nil {
				return status.WrapError(err, "remove deleted path from lower dir")
			}
			return nil
		}

		// If a dir exists in upperdir, just make sure it exists as a dir in
		// lowerdir too.
		if entry.IsDir() {
			if err := forceMkdir(lowerPath, o.opts.DirPerms); err != nil {
				return status.WrapError(err, "make dir")
			}
			return nil
		}

		// Not a dir: remove any existing contents at the path before moving
		// to lowerdir.
		if err := os.RemoveAll(lowerPath); err != nil {
			return status.WrapError(err, "remove existing file in lower dir")
		}

		if opts.AllowRename {
			if err := os.Rename(path, lowerPath); err != nil {
				return status.WrapError(err, "rename file to lower dir")
			}
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			return status.WrapError(err, "get entry info")
		}
		if info.Mode()&os.ModeSymlink == os.ModeSymlink {
			// Symlinks are safe to rename since the symlink target is not read
			// or written via a file handle, but rather via dedicated system
			// calls that operate on path args (readlink, symlink, symlinkat).
			if err := os.Rename(path, lowerPath); err != nil {
				return status.WrapError(err, "rename file to lower dir")
			}
			return nil
		}

		// Regular file: do a full copy.
		if err := disk.CopyViaTmpSibling(path, lowerPath); err != nil {
			return status.WrapError(err, "copy file to lower dir")
		}
		if err := os.RemoveAll(path); err != nil {
			return status.WrapError(err, "remove file after copying")
		}
		return nil
	})
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(o.UpperDir)
	if err != nil {
		return status.WrapError(err, "read upper dir")
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(o.UpperDir, e.Name())); err != nil {
			return status.WrapError(err, "remove upper dir entry")
		}
	}
	return nil
}

func forceMkdir(path string, perm fs.FileMode) error {
	err := os.MkdirAll(path, perm)
	if err == nil {
		return nil
	}
	// If we failed to create the dir because the existing path is not a dir
	// (i.e. file or symlink), remove the existing path and retry.
	if !errors.Is(err, syscall.ENOTDIR) {
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return os.Mkdir(path, perm)
}

// isIgnored returns whether a path in an overlay upper dir is an "ignored"
// entry, meaning that it has been deleted in the overlay workspace.
func isIgnored(path string) (bool, error) {
	// Using Lstat instead of Stat here to avoid following symlinks.
	stat, err := os.Lstat(path)
	if err != nil {
		return false, err
	}
	// whiteout files are character device files with device number 0.
	isCharDevice := stat.Mode()&os.ModeCharDevice == os.ModeCharDevice
	return isCharDevice && stat.Sys().(*syscall.Stat_t).Rdev == 0, nil
}
