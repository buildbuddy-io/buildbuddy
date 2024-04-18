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
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
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
)

// DirectoryToImage creates an ext4 image of the specified size from inputDir
// and writes it to outputFile.
func DirectoryToImage(ctx context.Context, inputDir, outputFile string, sizeBytes int64) error {
	if err := checkImageOutputPath(outputFile); err != nil {
		return err
	}

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	args := []string{
		"/sbin/mke2fs",
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
		"/sbin/mke2fs",
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
func ImageToDirectory(ctx context.Context, inputFile, outputDir string) error {
	empty, err := isDirEmpty(outputDir)
	if err != nil {
		return err
	}
	if !empty {
		return status.FailedPreconditionError("Unpacking image in non-empty directory is unsupported.")
	}
	args := []string{
		"/sbin/debugfs",
		inputFile,
		"-R",
		fmt.Sprintf("rdump \"/\" \"%s\"", outputDir),
	}
	if out, err := exec.CommandContext(ctx, args[0], args[1:]...).CombinedOutput(); err != nil {
		return status.InternalErrorf("%s: %s", err, out)
	}
	return nil
}
