//go:build windows

package fastcopy_test

import (
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/fastcopy"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

const (
	// In Windows Github Actions, we setup a DevDrive at D:.
	// See .github/workflows/build-executor-win.yaml for details.
	DevDrivePath = "D:"

	// FILE_SUPPORTS_BLOCK_REFCOUNTING indicates the file system supports block cloning
	// Reference:
	//
	//	https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/ebc7e6e5-4650-4e54-b17c-cf60f6fbeeaa
	//
	// TODO(sluongng): add this to golang.org/x/sys/windows~syscall_windows.go
	FILE_SUPPORTS_BLOCK_REFCOUNTING = uint32(0x08000000)
)

func isBlockCloningSupported(t *testing.T) bool {
	// Create a temporary file to test block cloning support
	tempDir := filepath.Join(DevDrivePath, "temp_test_dir")
	err := os.MkdirAll(tempDir, 0755)
	require.NoError(t, err, "Failed to create temporary directory for block cloning check")
	defer os.RemoveAll(tempDir)

	// Create a small test file
	testFile := filepath.Join(tempDir, "test.txt")
	err = os.WriteFile(testFile, []byte("test"), 0644)
	require.NoError(t, err, "Failed to create test file for block cloning check")

	// Try to open the file and check block cloning support
	file, err := os.Open(testFile)
	require.NoError(t, err, "Failed to open test file for block cloning check")
	defer file.Close()

	// https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getvolumeinformationbyhandlew
	var flags uint32
	err = windows.GetVolumeInformationByHandle(
		windows.Handle(file.Fd()), // lpRootPathName
		nil,                       // lpVolumeNameBuffer
		0,                         // nVolumeNameSize
		nil,                       // lpVolumeSerialNumber
		nil,                       // lpMaximumComponentLength
		&flags,                    // lpFileSystemFlags
		nil,                       // lpFileSystemNameBuffer
		0,                         // nFileSystemNameSize
	)
	require.NoError(t, err, "GetVolumeInformationByHandleW should succeed")

	return flags&FILE_SUPPORTS_BLOCK_REFCOUNTING != 0
}

func TestCloneWindows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skipf("test runs on windows only")
	}
	flags.Set(t, "executor.enable_fastcopy_reflinking", true)

	// Try to find a DevDrive for optimal CoW testing
	hasBlockCloning := isBlockCloningSupported(t)
	if !hasBlockCloning {
		t.Skipf("No DevDrive found, skipping test for block cloning support")
	}

	t.Logf("DevDrive found at %s - testing with block cloning support", DevDrivePath)
	testDir, err := os.MkdirTemp(DevDrivePath, "test-clone-windows-*")
	require.NoError(t, err, "Failed to create temporary directory for test")
	source := testfs.MakeTempFile(t, testDir, "test-source-*.txt")
	target := filepath.Join(testDir, "test_target.txt")

	// Ensure target doesn't exist
	_, err = os.Stat(target)
	require.Error(t, err)
	require.ErrorIs(t, err, os.ErrNotExist)

	// Perform the clone operation
	err = fastcopy.Clone(source, target)
	require.NoError(t, err, "Clone should succeed on DevDrive with ReFS")
	t.Cleanup(func() { os.Remove(target) })

	// Verify target file exists and has correct content
	_, err = os.Stat(target)
	require.NoError(t, err, "target file should exist after clone")

	// Read original test data for comparison
	testData, err := os.ReadFile(source)
	require.NoError(t, err)

	targetData, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, testData, targetData, "target file should have same content as source")

	// Test CoW behavior: modify target file and ensure source is unchanged
	f, err := os.OpenFile(target, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()

	modifiedData := []byte("Modified data to test CoW")
	_, err = f.WriteAt(modifiedData, 0)
	require.NoError(t, err)

	// Verify source file is unchanged (CoW behavior)
	sourceData, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, testData, sourceData, "source file should be unchanged after modifying target (CoW)")

	// Verify target file has the modified data
	targetDataAfterModify, err := os.ReadFile(target)
	require.NoError(t, err)
	require.NotEqual(t, testData, targetDataAfterModify, "target file should have modified data")
}

func TestCloneWindowsCopyFileW(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skipf("test runs on windows only")
	}
	flags.Set(t, "executor.enable_fastcopy_reflinking", false)

	ws := testfs.MakeTempDir(t)
	source := testfs.MakeTempFile(t, ws, "test data for CopyFileW")
	target := path.Join(ws, "copyfilew_target.txt")

	// Ensure target doesn't exist
	_, err := os.Stat(target)
	require.Error(t, err)
	require.ErrorIs(t, err, os.ErrNotExist)

	// Perform the clone operation - should use CopyFileW
	err = fastcopy.Clone(source, target)
	require.NoError(t, err, "Clone should succeed using CopyFileW")

	// Verify target file exists and has correct content
	_, err = os.Stat(target)
	require.NoError(t, err, "target file should exist after CopyFileW")

	// Read original test data for comparison
	testData, err := os.ReadFile(source)
	require.NoError(t, err)

	targetData, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, testData, targetData, "target file should have same content as source")

	// Test os.IsExist handling - try to copy to existing file
	err = fastcopy.Clone(source, target)
	require.NoError(t, err, "Clone should succeed when target exists (os.IsExist handling)")
}
