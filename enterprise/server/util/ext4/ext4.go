//go:build linux && !android

package ext4

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sys/unix"
)

const (
	// MinDiskImageSizeBytes is the approximate minimum size needed for an ext4
	// image. The functions in this package which create disk images will fail
	// if the provided size is any smaller.
	//
	// `man mkfs.ext4` says the journal size must be at least 1024 file system
	// blocks, so use that as the min disk image size for now.
	MinDiskImageSizeBytes = 1024 * blockSize

	// The number of bytes in one IEC kilobyte (K).
	iecKilobyte = 1024

	// FS block size that we always use when creating ext4 images.
	blockSize = 4096

	mke2fsPath  = "/sbin/mke2fs"
	debugfsPath = "/sbin/debugfs"

	// Filesystem UUID and directory hash seed used for reproducible images.
	// mke2fs otherwise generates both randomly.
	reproducibleUUID     = "3b3f1e6a-2a0c-4d1e-9d7e-6f0a7c1b2d3e"
	reproducibleHashSeed = "9c8b7a6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d"

	// SOURCE_DATE_EPOCH for reproducible images: 2026-01-01T00:00:00Z. mke2fs
	// clamps every timestamp newer than this to it, so the value only needs
	// to be in the past and never needs updating.
	reproducibleImageEpoch = 1767225600
)

var (
	reproducibleImages = flag.Bool("executor.reproducible_ext4_images", false, "If true, fix the filesystem UUID and directory hash seed of ext4 images created from directories, skip copying extended attributes, and clamp all timestamps newer than 2026-01-01 to that date, so that a freshly extracted directory tree produces a byte-identical image on every run. Files newer than that date lose their mtime, which breaks pre-warmed Bazel install bases. Requires e2fsprogs 1.47.2 or later.", flag.Internal)
	debugfsJailEnabled = flag.Bool("executor.debugfs_jail_enabled", true, "If true, run debugfs in a separate mount namespace where only the output directory is writable when unpacking ext4 images. Requires CAP_SYS_ADMIN.", flag.Internal)
)

// EnsureDependencies verifies that all external binaries required for ext4
// image operations are present and executable.
func EnsureDependencies() error {
	requiredBinaries := []string{mke2fsPath, debugfsPath}

	for _, binary := range requiredBinaries {
		info, err := os.Stat(binary)
		if err != nil {
			return status.UnavailableErrorf("required binary %q is missing: %s", binary, err)
		}
		if info.Mode()&0o111 == 0 {
			return status.UnavailableErrorf("required binary %q is not executable", binary)
		}
	}
	return nil
}

// DirectoryToImage creates an ext4 image of the specified size from inputDir
// and writes it to outputFile.
func DirectoryToImage(ctx context.Context, inputDir, outputFile string, sizeBytes int64) error {
	if err := checkImageOutputPath(outputFile); err != nil {
		return err
	}

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	args := []string{
		mke2fsPath,
		"-L", "''",
		"-N", "0",
		"-O", "^64bit",
		"-d", inputDir,
		"-m", "5",
		"-r", "1",
		"-b", fmt.Sprintf("%d", blockSize),
		"-t", "ext4",
	}
	var env []string
	if *reproducibleImages {
		// Remove all sources of non-determinism:
		// - Set filesystem UUID to a fixed value (-U)
		// - Set a fixed directory hash seed (-E hash_seed)
		// - Clamp all timestamps newer than a fixed date, which includes the
		//   extraction-time atimes and ctimes, to that date (SOURCE_DATE_EPOCH)
		// - Skip xattrs, which may contain host-assigned security.selinux
		//   labels on SELinux hosts (-E no_copy_xattrs)
		args = append(args, "-U", reproducibleUUID, "-E", "hash_seed="+reproducibleHashSeed+",no_copy_xattrs")
		env = append(os.Environ(), fmt.Sprintf("SOURCE_DATE_EPOCH=%d", reproducibleImageEpoch))
	}
	args = append(args, outputFile, fmt.Sprintf("%dK", sizeBytes/iecKilobyte))
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Env = env
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Errorf("Error running %q: %s %s", cmd.String(), err, out)
		return status.InternalErrorf("%s: %s", err, out)
	}
	if cmd.ProcessState != nil {
		if span.IsRecording() {
			span.SetAttributes(
				attribute.Int64(
					"cpu_millis",
					(cmd.ProcessState.UserTime()+cmd.ProcessState.SystemTime()).Milliseconds()),
				attribute.Int64("directory_bytes", sizeBytes),
			)
		}
		if rusage, _ := cmd.ProcessState.SysUsage().(*syscall.Rusage); rusage != nil {
			metrics.FirecrackerWorkspaceDiskWriteOps.With(prometheus.Labels{
				metrics.CommandName: "mke2fs",
			}).Add(float64(rusage.Oublock))
		}
	}
	return nil
}

// MakeEmptyImage creates a new empty ext4 disk image of the specified size
// and writes it to outputFile.
func MakeEmptyImage(ctx context.Context, outputFile string, sizeBytes int64) error {
	if err := checkImageOutputPath(outputFile); err != nil {
		return err
	}

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	args := []string{
		mke2fsPath,
		"-L", "",
		"-N", "0",
		"-O", "^64bit",
		"-m", "5",
		"-r", "1",
		"-b", fmt.Sprintf("%d", blockSize),
		"-t", "ext4",
		outputFile,
		fmt.Sprintf("%dK", sizeBytes/iecKilobyte),
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Errorf("Error running %q: %s %s", cmd.String(), err, out)
		return status.InternalErrorf("%s: %s", err, out)
	}
	return nil
}

// Checks an image output path to make sure a non-empty file doesn't already
// exist at that path. Overwriting an existing image can cause corruption.
func checkImageOutputPath(path string) error {
	stat, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return status.InternalErrorf("failed to create disk image %s: failed to stat output path: %s", path, err)
	}
	if stat != nil && stat.Size() > 0 {
		return status.InternalErrorf("failed to create disk image %s: file already exists and is not empty", path)
	}
	return nil
}

// DiskSizeBytes returns the disk space required to create an ext4 image from
// the given directory.
func DiskSizeBytes(ctx context.Context, inputDir string) (int64, error) {
	// Some images like alpine include a lot of symbolic links which `du`
	// does not report. So we calculate the size ourselves here.
	var total int64
	err := filepath.WalkDir(inputDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// Use lstat to avoid following symlinks. This avoids double-counting
		// symlink target files, and also makes sure we don't return an error
		// for dangling symlinks.
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		// Estimate from the entry type and size rather than from the blocks
		// the host filesystem allocated, which vary with the filesystem type
		// and allocation state and would make the image size depend on the
		// host. Count a block per entry for its inode and metadata, plus the
		// data blocks a regular file needs and a data block for each
		// directory.
		total += blockSize
		switch {
		case info.Mode().IsRegular():
			// Round the file size up to whole blocks.
			blocks := (info.Size() + blockSize - 1) / blockSize
			total += blocks * blockSize
		case info.IsDir():
			total += blockSize
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

// DirectoryToImageAutoSize is like DirectoryToImage, but it will attempt to
// automatically pick a file size that is "big enough".
func DirectoryToImageAutoSize(ctx context.Context, inputDir, outputFile string) error {
	dirSizeBytes, err := DiskSizeBytes(ctx, inputDir)
	if err != nil {
		return status.WrapError(err, "estimate disk usage")
	}

	imageSizeBytes := int64(float64(dirSizeBytes)*1.2) + MinDiskImageSizeBytes
	return DirectoryToImage(ctx, inputDir, outputFile, imageSizeBytes)
}

// isDirEmpty returns a bool indicating if a directory contains no files, or
// an error.
func isDirEmpty(dir string) (bool, error) {
	f, err := os.Open(dir)
	if err != nil {
		return false, err
	}
	defer f.Close()
	_, err = f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

// ImageToDirectory unpacks an ext4 image into outputDir, which must be empty.
// Only the given paths are unpacked. Paths are unpacked recursively, which
// means that they can reference either directories or files. Non-existent
// paths are silently ignored.
//
// If executor.debugfs_jail_enabled is set, debugfs runs in a separate mount
// namespace where only outputDir is writable, which requires CAP_SYS_ADMIN.
func ImageToDirectory(ctx context.Context, inputFile, outputDir string, paths []string) error {
	empty, err := isDirEmpty(outputDir)
	if err != nil {
		return err
	}
	if !empty {
		return status.FailedPreconditionError("Unpacking image in non-empty directory is unsupported.")
	}
	if !*debugfsJailEnabled {
		return rdump(ctx, inputFile, outputDir, paths)
	}
	newRoot, err := os.MkdirTemp("", "debugfs-root-*")
	if err != nil {
		return err
	}
	defer os.Remove(newRoot)

	errCh := make(chan error, 1)
	go func() {
		// Lock this goroutine to the OS thread for its lifetime since it
		// runs unpackImage in a new mount namespace.
		runtime.LockOSThread()
		errCh <- unpackImage(ctx, newRoot, inputFile, outputDir, paths)
	}()
	return <-errCh
}

// bindMount creates target and bind-mounts source on it.
func bindMount(source, target string, readOnly bool) error {
	info, err := os.Stat(source)
	if err != nil {
		return err
	}
	if info.IsDir() {
		if err := os.MkdirAll(target, 0755); err != nil {
			return err
		}
	} else {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%q is not a regular file or directory", source)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return err
		}
		// Create the mount point with a read-only fd. This avoids EBUSY when
		// enterChrootJail remounts the tmpfs at newRoot read-only, in case the
		// executor forks while the fd is open and the child still holds a
		// copy of it.
		f, err := os.OpenFile(target, os.O_CREATE|os.O_RDONLY, 0644)
		if err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	if err := unix.Mount(source, target, "", unix.MS_BIND, ""); err != nil {
		return err
	}
	// Keep the flags of the source mount when remounting, because the kernel
	// rejects a remount that clears flags locked by a user namespace.
	var stat unix.Statfs_t
	if err := unix.Statfs(source, &stat); err != nil {
		return err
	}
	flags := uintptr(stat.Flags) & (unix.MS_RDONLY | unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOEXEC | unix.MS_NOATIME | unix.MS_NODIRATIME)
	flags |= unix.MS_BIND | unix.MS_REMOUNT | unix.MS_NODEV | unix.MS_NOSUID
	if readOnly {
		flags |= unix.MS_RDONLY
	}
	return unix.Mount("", target, "", flags, "")
}

// jailMount is a bind mount of source at target inside the jail.
type jailMount struct {
	source   string
	target   string
	readOnly bool
}

// enterChrootJail moves the calling thread into a new mount namespace whose
// root directory is a read-only tmpfs at newRoot, holding only the given
// mounts. The caller must run it on a locked thread that no other goroutine
// uses afterwards.
//
// TODO: run the entire action (including input/output handling) in a jail
// instead.
func enterChrootJail(newRoot string, mounts []jailMount) error {
	if err := unix.Unshare(unix.CLONE_NEWNS); err != nil {
		return fmt.Errorf("unshare mount namespace: %w", err)
	}
	// Stop mounts and unmounts in this namespace from propagating to the
	// executor's namespace.
	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		return fmt.Errorf("make mounts private: %w", err)
	}
	if err := unix.Mount("tmpfs", newRoot, "tmpfs", unix.MS_NODEV|unix.MS_NOSUID, "mode=0755"); err != nil {
		return fmt.Errorf("mount jail root: %w", err)
	}
	for _, m := range mounts {
		if err := bindMount(m.source, filepath.Join(newRoot, m.target), m.readOnly); err != nil {
			return fmt.Errorf("mount %q: %w", m.source, err)
		}
	}
	oldRoot := filepath.Join(newRoot, "oldroot")
	if err := os.Mkdir(oldRoot, 0700); err != nil {
		return err
	}
	if err := unix.Mount("", newRoot, "", unix.MS_REMOUNT|unix.MS_RDONLY|unix.MS_NODEV|unix.MS_NOSUID, ""); err != nil {
		return fmt.Errorf("remount jail root read-only: %w", err)
	}
	if err := unix.PivotRoot(newRoot, oldRoot); err != nil {
		return fmt.Errorf("pivot_root: %w", err)
	}
	if err := unix.Chdir("/"); err != nil {
		return err
	}
	// Detach the mounts copied from the executor's namespace. While this
	// namespace holds a copy of a FUSE mount, the kernel keeps the filesystem
	// running even after the executor unmounts it.
	if err := unix.Unmount("/oldroot", unix.MNT_DETACH); err != nil {
		return fmt.Errorf("unmount old root: %w", err)
	}
	return nil
}

// unpackImage runs debugfs in a chroot jail that holds debugfs and its
// libraries, the image at /image, and outputDir at /output, which is the only
// writable mount. The caller must run it on a locked thread that no other
// goroutine uses afterwards.
func unpackImage(ctx context.Context, newRoot, inputFile, outputDir string, paths []string) error {
	// Set up mounts individually instead of making the executor's mount tree
	// read-only, to avoid holding on to the executor's existing mounts while
	// debugfs runs, which adds too much overhead.
	var mounts []jailMount
	for _, dir := range []string{"/lib", "/lib64", "/usr/lib", "/usr/lib64"} {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			continue
		}
		mounts = append(mounts, jailMount{source: dir, target: dir, readOnly: true})
	}
	mounts = append(mounts,
		jailMount{source: debugfsPath, target: debugfsPath, readOnly: true},
		jailMount{source: inputFile, target: "/image", readOnly: true},
		jailMount{source: outputDir, target: "/output", readOnly: false},
	)
	if err := enterChrootJail(newRoot, mounts); err != nil {
		return err
	}
	return rdump(ctx, "/image", "/output", paths)
}

// rdump runs debugfs to unpack the given paths from inputFile into outputDir.
func rdump(ctx context.Context, inputFile, outputDir string, paths []string) error {
	requests := make([]string, 0, len(paths))
	for _, p := range paths {
		p = filepath.Clean("/" + p)
		parent := filepath.Dir(p)
		if err := os.MkdirAll(filepath.Join(outputDir, parent), 0755); err != nil {
			return status.InternalErrorf("make parent dir %q: %s", parent, err)
		}
		requests = append(requests, fmt.Sprintf("rdump %q %q", p, filepath.Join(outputDir, parent)))
	}
	cmd := exec.CommandContext(ctx, debugfsPath, inputFile)
	cmd.Stdin = strings.NewReader(strings.Join(requests, "\n"))
	if out, err := cmd.CombinedOutput(); err != nil {
		return status.InternalErrorf("%s: %s", err, out)
	}
	if cmd.ProcessState != nil {
		if rusage, _ := cmd.ProcessState.SysUsage().(*syscall.Rusage); rusage != nil {
			metrics.FirecrackerWorkspaceDiskWriteOps.With(prometheus.Labels{
				metrics.CommandName: "debugfs",
			}).Add(float64(rusage.Oublock))
		}
	}
	return nil
}
