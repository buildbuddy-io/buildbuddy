package scratchspace

import (
	"flag"
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var (
	// NOTE: This flag shouldn't be used directly except in the tempDir() func.
	// We intentionally write to a subdirectory under this dir to avoid blowing
	// away existing files in the configured directory, in case of accidental
	// misconfiguration.
	tempDir_doNotUseDirectly = flag.String("storage.tempdir", defaultTempDir(), "Root directory for temporary files. Defaults to the OS-specific temp dir.")
)

const (
	// tempSubDir is a subdirectory under the temp dir where we actually write
	// temporary files. It is recursively removed and re-created from scratch
	// when Init() is called.
	tempSubDir = "buildbuddy-scratch"
)

// Init creates the scratch directory if needed and deletes any pre-existing
// files. Calling this function is required before using any other functions in
// this package.
func Init() error {
	if err := os.RemoveAll(tempDir()); err != nil {
		return status.InvalidArgumentErrorf("failed to clear scratch directory: %s", err)
	}
	if err := os.MkdirAll(tempDir(), 0755); err != nil {
		return status.InvalidArgumentErrorf("failed to create scratch directory: %s", err)
	}
	return nil
}

func defaultTempDir() string {
	if d := os.Getenv("TEST_TMPDIR"); d != "" {
		return d
	}
	return os.TempDir()
}

// tempDir returns the directory where scratch files are written.
func tempDir() string {
	return filepath.Join(*tempDir_doNotUseDirectly, tempSubDir)
}

// CreateTemp has the same semantics as os.CreateTemp, with the temp directory
// set to the configured scratch directory.
// The caller is responsible for deleting the file once it is no longer needed.
//
// Example usage:
//
//	f, err := scratchspace.CreateTemp("download-*")
//	if err != nil {
//	    return err
//	}
//	defer func() {
//	    f.Close()
//	    os.Remove(f.Name())
//	}()
func CreateTemp(pattern string) (*os.File, error) {
	return os.CreateTemp(tempDir(), pattern)
}

// MkdirTemp has the same semantics as os.MkdirTemp, with the parent directory
// set to the configured scratch directory.
// The caller is responsible for deleting the directory once it is no longer needed.
//
// Example usage:
//
//	dir, err := scratchspace.MkdirTemp("dirname-*")
//	if err != nil {
//	    return err
//	}
//
// defer os.RemoveAll(dir) // clean up
func MkdirTemp(pattern string) (string, error) {
	return os.MkdirTemp(tempDir(), pattern)
}
