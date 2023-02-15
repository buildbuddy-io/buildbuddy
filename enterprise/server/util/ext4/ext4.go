package ext4

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
)

const (
	// MinDiskImageSizeBytes is the approximate minimum size needed for an ext4
	// image. The functions in this package which create disk images will fail
	// if the provided size is any smaller.
	MinDiskImageSizeBytes = 225e3
)

// DirectoryToImage creates an ext4 image of the specified size from inputDir
// and writes it to outputFile.
func DirectoryToImage(ctx context.Context, inputDir, outputFile string, sizeBytes int64) error {
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
		"-t", "ext4",
		outputFile,
		fmt.Sprintf("%dK", sizeBytes/1e3),
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
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	args := []string{
		"/sbin/mke2fs",
		"-L", "",
		"-N", "0",
		"-O", "^64bit",
		"-m", "5",
		"-r", "1",
		"-t", "ext4",
		outputFile,
		fmt.Sprintf("%dK", sizeBytes/1e3),
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Errorf("Error running %q: %s %s", cmd.String(), err, out)
		return status.InternalErrorf("%s: %s", err, out)
	}
	return nil
}

// DiskSizeBytes returns the size in bytes of a directory according to "du -sk".
// It can be used when creating ext4 images -- to ensure they are large enough.
func DiskSizeBytes(ctx context.Context, inputDir string) (int64, error) {
	out, err := exec.CommandContext(ctx, "du", "-sk", inputDir).CombinedOutput()
	if err != nil {
		return 0, status.InternalErrorf("%s: %s", err, out)
	}

	parts := strings.Split(string(out), "\t")
	if len(parts) != 2 {
		return 0, status.InternalErrorf("du output %q did not match 'SIZE  /file/path'", out)
	}
	s, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, status.InternalErrorf("du output %q did not match 'SIZE  /file/path': %s", out, err)
	}
	return int64(s * 1000), nil
}

// DirectoryToImageAutoSize is like DirectoryToImage, but it will attempt to
// automatically pick a file size that is "big enough".
func DirectoryToImageAutoSize(ctx context.Context, inputDir, outputFile string) error {
	dirSizeBytes, err := DiskSizeBytes(ctx, inputDir)
	if err != nil {
		return nil
	}

	imageSizeBytes := int64(float64(dirSizeBytes)*1.2) + 1000000
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
