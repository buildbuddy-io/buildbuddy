//go:build windows

package fastcopy_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

func ensureTrailingBackslash(p string) string {
	if p == "" {
		return p
	}
	if p[len(p)-1] == '\\' {
		return p
	}
	if len(p) == 2 && p[1] == ':' {
		return p + "\\"
	}
	return p
}

func volumeExists(root string) bool {
	root = ensureTrailingBackslash(root)
	if root == "" {
		return false
	}
	_, err := os.Stat(root)
	return err == nil
}

func getFSInfoForRoot(t testing.TB, root string) (fsName string, fsFlags uint32) {
	root = ensureTrailingBackslash(root)
	p, err := windows.UTF16PtrFromString(root)
	require.NoError(t, err)
	var fsNameBuf [261]uint16
	var fileSystemFlags uint32
	err = windows.GetVolumeInformation(p, nil, 0, nil, nil, &fileSystemFlags, &fsNameBuf[0], uint32(len(fsNameBuf)))
	require.NoError(t, err)
	return windows.UTF16ToString(fsNameBuf[:]), fileSystemFlags
}

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
	var fsFlags uint32
	err = windows.GetVolumeInformationByHandle(
		windows.Handle(file.Fd()), // lpRootPathName
		nil,                       // lpVolumeNameBuffer
		0,                         // nVolumeNameSize
		nil,                       // lpVolumeSerialNumber
		nil,                       // lpMaximumComponentLength
		&fsFlags,                  // lpFileSystemFlags
		nil,                       // lpFileSystemNameBuffer
		0,                         // nFileSystemNameSize
	)
	require.NoError(t, err, "GetVolumeInformationByHandleW should succeed")

	return fsFlags&FILE_SUPPORTS_BLOCK_REFCOUNTING != 0
}

// createRandomFile creates a file with random content using PowerShell for fast generation
func createRandomFile(path string, size int64) error {
	// Use PowerShell to generate random content quickly
	cmd := exec.Command("powershell", "-Command",
		fmt.Sprintf("$out = new-object byte[] %d; (new-object Random).NextBytes($out); [IO.File]::WriteAllBytes('%s', $out)",
			size, path))
	return cmd.Run()
}

func TestCloneWindows(t *testing.T) {
	flags.Set(t, "executor.enable_fastcopy_reflinking", true)

	// Try to find a DevDrive for optimal CoW testing
	hasBlockCloning := isBlockCloningSupported(t)
	if !hasBlockCloning {
		t.Skipf("No DevDrive found, skipping test for block cloning support")
	}

	// Note: Bazel Windows has no sandboxing, so we can use the DevDrive directly.
	t.Logf("DevDrive found at %s - testing with block cloning support", DevDrivePath)
	testDir, err := os.MkdirTemp(DevDrivePath, "test-clone-windows-*")
	require.NoError(t, err, "Failed to create temporary directory for test")
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(testDir)) })

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
	flags.Set(t, "executor.enable_fastcopy_reflinking", false)

	ws := testfs.MakeTempDir(t)
	source := testfs.MakeTempFile(t, ws, "copyfilew_source-*.txt")
	// Write actual content so the CopyFileW path is validated meaningfully.
	require.NoError(t, os.WriteFile(source, []byte("test data for CopyFileW"), 0644))
	target := filepath.Join(ws, "copyfilew_target.txt")

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

// Benchmarks comparing CopyFileW vs Reflink across volumes and sizes.
func BenchmarkWindowsFastCopy(b *testing.B) {
	// Discover candidate volumes.
	vols := make([]string, 0, 2)
	if volumeExists("C:") {
		vols = append(vols, "C:")
	}
	if volumeExists("D:") {
		vols = append(vols, "D:")
	}
	if len(vols) == 0 {
		b.Skip("No suitable volumes found (need C: and/or D:/DEVDRIVE_PATH)")
	}

	type sizeCfg struct {
		label string
		bytes int64
	}
	sizes := []sizeCfg{
		{label: "small", bytes: 8 * 1024},           // 8 KiB
		{label: "medium", bytes: 8 * 1024 * 1024},   // 8 MiB
		{label: "large", bytes: 1024 * 1024 * 1024}, // 1 GiB
	}

	type methodCfg struct {
		label   string
		reflink bool
	}
	methods := []methodCfg{
		{label: "CopyFileW", reflink: false},
		{label: "Reflink", reflink: true},
	}

	for _, vol := range vols {
		vol = ensureTrailingBackslash(vol)
		fsName, fsFlags := getFSInfoForRoot(b, vol)
		b.Run(fmt.Sprintf("volume=%s_fs=%s", vol, fsName), func(b *testing.B) {
			root := filepath.Join(vol, "buildbuddy_fastcopy_bench")
			require.NoError(b, os.MkdirAll(root, 0755))

			for _, sz := range sizes {
				b.Run(fmt.Sprintf("size=%s", sz.label), func(b *testing.B) {
					// Prepare source once per size.
					src := filepath.Join(root, fmt.Sprintf("source_%s.bin", sz.label))
					// Re-create to minimize stale state effects.
					_ = os.Remove(src)
					// Create file with random content using PowerShell for realistic performance testing
					require.NoError(b, createRandomFile(src, sz.bytes))

					for _, m := range methods {
						// Skip Reflink where the FS doesn't support block refcounting.
						if m.reflink && (fsFlags&FILE_SUPPORTS_BLOCK_REFCOUNTING) == 0 {
							b.Run(m.label, func(b *testing.B) {
								b.Skipf("Reflink unsupported on %s (%s)", vol, fsName)
							})
							continue
						}
						b.Run(m.label, func(b *testing.B) {
							flags.Set(b, "executor.enable_fastcopy_reflinking", m.reflink)
							b.SetBytes(sz.bytes)
							// Each iteration clones to a unique dst. Only time the clone itself.
							for i := 0; i < b.N; i++ {
								dst := filepath.Join(root, fmt.Sprintf("dst_%s_%s_%d.bin", m.label, sz.label, i))
								b.StartTimer()
								err := fastcopy.Clone(src, dst)
								b.StopTimer()
								require.NoError(b, err)
								_ = os.Remove(dst)
							}
						})
					}
				})
			}
		})
	}
}
