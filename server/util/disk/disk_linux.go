//go:build linux && !android

package disk

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
)

// ChildMounts returns the mount points of all filesystems mounted under the
// given path. It does not include the given path itself.
func ChildMounts(ctx context.Context, path string) ([]string, error) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()

	// Use abs path so that we can use a prefix check to see if a mount point
	// listed in /proc/self/mountinfo is a child of the given path.
	prefix, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	prefix += string(os.PathSeparator)

	b, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}

	var out []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(b)), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		mountPoint := fields[4]
		if strings.HasPrefix(mountPoint, prefix) {
			out = append(out, filepath.Clean(mountPoint))
		}
	}
	return out, nil
}

// CleanDirectory performs a comprehensive cleanup of the given directory. It
// does not recurse into child mounts, instead attempting to unmount them. It
// changes directory permissions as needed in order to remove files. It does not
// remove the directory itself.
func CleanDirectory(ctx context.Context, dir string) error {
	// First, check for child mounts, since we don't want to recurse into these.
	// Specifically, recursively deleting files within an overlayfs mount causes
	// a bunch of tombstone files to be written, which is counter-productive
	// since the goal is to delete the directory.

	childMounts, err := ChildMounts(ctx, dir)
	if err != nil {
		return fmt.Errorf("get child mounts: %s", err)
	}

	// Keep track of paths that we failed to unmount; we don't want to recurse
	// into these directories, or attempt to remove any of their parent
	// directories.
	skip := map[string]struct{}{}

	// TODO: if we have CAP_SYS_ADMIN capability then we can use the MNT_FORCE
	// option.
	for _, mountPath := range childMounts {
		log.Infof("Clean: unmounting child mount %q", mountPath)
		if err := syscall.Unmount(mountPath, 0); err == nil {
			continue
		} else if !errors.Is(err, syscall.EBUSY) {
			log.CtxWarningf(ctx, "Failed to unmount %q: %s", mountPath, err)
			skip[mountPath] = struct{}{}
			continue
		}
		// If the mount is still busy, try a lazy unmount so that we can at
		// least remove the path from the filesystem and proceed with cleanup,
		// but log a warning since this might lead to resource leaks.
		if err := syscall.Unmount(mountPath, syscall.MNT_DETACH); err != nil {
			log.CtxWarningf(ctx, "Failed to detach %q: %s", mountPath, err)
			skip[mountPath] = struct{}{}
			continue
		}
		log.CtxWarningf(ctx, "Unmounted in background due to EBUSY (path: %q)", mountPath)
	}

	// Accumulate list of files to delete in a top-down order using WalkDir.
	type dirEntry struct {
		path  string
		isDir bool
	}
	var visited []dirEntry
	err = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		// Don't recurse into skipped directories.
		if _, ok := skip[path]; ok {
			return filepath.SkipDir
		}
		// Don't clean up the root directory itself.
		if path == dir {
			return nil
		}
		visited = append(visited, dirEntry{
			path:  path,
			isDir: entry.IsDir(),
		})
		return nil
	})
	if err != nil {
		return err
	}
	// Delete in bottom-up order since children must be deleted before their
	// parent directories can be removed.
	for _, entry := range slices.Backward(visited) {
		if _, ok := skip[entry.path]; ok {
			// If this path is skipped, it's because we couldn't delete it. We
			// won't be able to delete the parent either since it's not empty,
			// so mark the parent skipped too.
			skip[filepath.Dir(entry.path)] = struct{}{}
			continue
		}
		// Don't delete visited directories if they are parents of skipped
		// paths.
		if entry.isDir {
			skipped := false
			for skippedPath := range skip {
				if isParent(entry.path, skippedPath) {
					skipped = true
					continue
				}
			}
			if skipped {
				continue
			}
		}
		if err := forceUnlink(entry.path); err != nil {
			log.CtxWarningf(ctx, "Failed to unlink %q: %s", entry.path, err)
			// Skip the parent since it contains a file we couldn't delete.
			skip[filepath.Dir(entry.path)] = struct{}{}
		}
	}

	return nil
}
