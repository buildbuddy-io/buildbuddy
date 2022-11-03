package scratchspace

import (
	"flag"
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var (
	tempDir_doNotUseDirectly = flag.String("storage.tempdir", filepath.Join(os.TempDir(), "buildbuddy-scratch"), "Staging directory for ephemeral files. If unspecified, defaults to 'TEMPDIR/buildbuddy' where TEMPDIR is the OS-specific temp dir.")
)

// Init creates the scratch directory if needed and deletes any pre-existing
// files. Calling this function is required before using any other functions in
// this package.
func Init() error {
	if err := os.RemoveAll(TempDir()); err != nil {
		return status.InvalidArgumentErrorf("failed to clear scratch directory: %s", err)
	}
	if err := os.MkdirAll(TempDir(), 0755); err != nil {
		return status.InvalidArgumentErrorf("failed to create scratch directory: %s", err)
	}
	return nil
}

// TempDir returns the directory where scratch files are written.
func TempDir() string {
	// Note: This is the only func that should directly reference the tempdir
	// flag. We intentionally write to a _tmp subdirectory to avoid blowing away
	// existing files in the scratch_directory, in case of accidental
	// misconfiguration.
	return filepath.Join(*tempDir_doNotUseDirectly, "_tmp")
}

// CreateTemp has the same semantics as os.CreateTemp, with the temp directory
// set to the configured scratch directory.
// The caller is responsible for deleting the file once it is no longer needed.
//
// Example usage:
//
//     f, err := os.CreateTemp("download-*")
//     if err != nil {
//         return err
//     }
//     defer func() {
//         f.Close()
//         os.Remove(f.Name())
//     }()
//
func CreateTemp(pattern string) (*os.File, error) {
	return os.CreateTemp(TempDir(), pattern)
}
