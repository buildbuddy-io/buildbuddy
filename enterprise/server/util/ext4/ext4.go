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
	"strings"
	"sync"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
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

// ImageOptions configures ext4 image creation.
type ImageOptions struct {
	CopyWorkers int
}

// DirectoryToImage creates an ext4 image of the specified size from inputDir
// and writes it to outputFile.
func DirectoryToImage(ctx context.Context, inputDir, outputFile string, sizeBytes int64, opts ...ImageOptions) error {
	var opt ImageOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	if err := checkImageOutputPath(outputFile); err != nil {
		return err
	}

	if opt.CopyWorkers > 0 {
		return directoryToImageParallel(ctx, inputDir, outputFile, sizeBytes, opt)
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
		outputFile,
		fmt.Sprintf("%dK", sizeBytes/iecKilobyte),
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
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

var copyBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 256*1024)
		return &buf
	},
}

type reader struct {
	r io.Reader
}

func (r *reader) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

type writer struct {
	w io.Writer
}

func (w *writer) Write(p []byte) (int, error) {
	return w.w.Write(p)
}

// copyFile copies a single file, preserving permissions.
// It tries sendfile(2) first for zero-copy transfer, then falls back
// to a pooled-buffer userspace copy.
func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer out.Close()

	size, err := in.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if size == 0 {
		return out.Close()
	}
	if _, err := in.Seek(0, io.SeekStart); err != nil {
		return err
	}
	written, sendfileErr := sendfileAll(int(out.Fd()), int(in.Fd()), size)
	if sendfileErr == nil {
		if written < size {
			return io.ErrShortWrite
		}
		return out.Close()
	}
	// sendfile failed (e.g. EINVAL, ENOSYS) — fall back to
	// userspace copy. Reset both file offsets and truncate output
	// in case sendfile made partial progress.
	if _, err := in.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := out.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := out.Truncate(0); err != nil {
		return err
	}

	bufp := copyBufPool.Get().(*[]byte)
	defer copyBufPool.Put(bufp)
	if _, err := io.CopyBuffer(&writer{out}, &reader{in}, *bufp); err != nil {
		return err
	}
	return out.Close()
}

// sendfileAll calls sendfile(2) in a loop until all bytes are transferred.
func sendfileAll(dstFd, srcFd int, size int64) (int64, error) {
	var total int64
	for total < size {
		n, err := unix.Sendfile(dstFd, srcFd, nil, int(size-total))
		total += int64(n)
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

func directoryToImageParallel(ctx context.Context, inputDir, outputFile string, sizeBytes int64, opt ImageOptions) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	// Step 1: Create empty ext4 image.
	if err := MakeEmptyImage(ctx, outputFile, sizeBytes); err != nil {
		return status.WrapError(err, "create empty image")
	}

	// Step 2: Mount the image via loop device.
	mountDir, err := os.MkdirTemp("", "ext4-mount-*")
	if err != nil {
		return status.InternalErrorf("create mount dir: %s", err)
	}
	defer os.Remove(mountDir)

	unmount, err := mountImage(ctx, outputFile, mountDir)
	if err != nil {
		return err
	}
	defer unmount()

	// Step 3: Walk the source directory and copy files in parallel.
	// Directories are created synchronously during the walk so they exist
	// before any file copies land in them. File and symlink copies are
	// dispatched to workers as the walk progresses.
	serial := false
	if opt.CopyWorkers < 0 {
		serial = true
		opt.CopyWorkers = 1
	}

	eg, _ := errgroup.WithContext(ctx)
	eg.SetLimit(opt.CopyWorkers)
	err = filepath.WalkDir(inputDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(inputDir, path)
		if err != nil {
			return err
		}
		if relPath == "." {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		dstPath := filepath.Join(mountDir, relPath)

		if d.Type()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			if serial {
				if err := os.Symlink(target, dstPath); err != nil {
					return err
				}
			} else {
				eg.Go(func() error {
					return os.Symlink(target, dstPath)
				})
			}
			return nil
		}

		if d.IsDir() {
			return os.MkdirAll(dstPath, info.Mode().Perm())
		}

		mode := info.Mode().Perm()
		if serial {
			return copyFile(path, dstPath, mode)
		}
		eg.Go(func() error {
			return copyFile(path, dstPath, mode)
		})
		return nil
	})
	if err != nil {
		return status.WrapError(err, "walk source directory")
	}
	if err := eg.Wait(); err != nil {
		return status.WrapError(err, "parallel copy")
	}

	// Step 4: Unmount (sync data to image).
	if err := unmount(); err != nil {
		return err
	}

	return nil
}

func mountImage(ctx context.Context, imageFile, mountDir string) (func() error, error) {
	mountCmd := exec.CommandContext(ctx, "mount", "-o", "loop,noatime", imageFile, mountDir)
	if out, err := mountCmd.CombinedOutput(); err != nil {
		return nil, status.InternalErrorf("mount: %s: %s", err, out)
	}
	done := false
	return func() error {
		if done {
			return nil
		}
		done = true
		umountCmd := exec.Command("umount", mountDir)
		if out, err := umountCmd.CombinedOutput(); err != nil {
			return status.InternalErrorf("umount: %s: %s", err, out)
		}
		return nil
	}, nil
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
		// stat() does not account for file or symlink metadata or for
		// filesystem data structures like indirect blocks which consume disk
		// space, so add 2 extra disk blocks for each entry as a rough way to
		// account for this. Also note that stat() blocks are always 512 bytes
		// regardless of the FS settings.
		total += blockSize + info.Sys().(*syscall.Stat_t).Blocks*512
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
func ImageToDirectory(ctx context.Context, inputFile, outputDir string, paths []string) error {
	empty, err := isDirEmpty(outputDir)
	if err != nil {
		return err
	}
	if !empty {
		return status.FailedPreconditionError("Unpacking image in non-empty directory is unsupported.")
	}
	requests := make([]string, 0, len(paths))
	for _, p := range paths {
		p = filepath.Clean(filepath.Join(outputDir, p))
		p = strings.TrimPrefix(p, filepath.Clean(outputDir))
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
