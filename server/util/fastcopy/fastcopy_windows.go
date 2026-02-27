//go:build windows

package fastcopy

import (
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	// 1 giB
	giB = 1024 * 1024 * 1024
)

var (
	modkernel32   = windows.NewLazySystemDLL("kernel32.dll")
	procCopyFileW = modkernel32.NewProc("CopyFileW")

	// ReFS only supports 64KiB and 4KiB cluster with 4KiB being the recommended size.
	// - https://learn.microsoft.com/en-us/windows-server/storage/refs/refs-overview#performance
	// - https://techcommunity.microsoft.com/blog/filecab/cluster-size-recommendations-for-refs-and-ntfs/425960
	//
	// Note: this can be detected at runtime using GetDiskFreeSpaceA on the Volume and
	// calculate the returned `sectorsPerCluster * bytesPerSector`.
	// However, we are assuming ReFS/DevDrive is being used here.
	reFSClusterSizes = []int64{64 * 1024, 4 * 1024}
)

// User can set executor.enable_fastcopy_reflinking=true to force
// using block cloning on Windows.
// Otherwise, fallback to using CopyFileW which will use block-cloning
// automatically based on the underlying file system and Windows version.
func Clone(source, destination string) error {
	if *enableFastcopyReflinking {
		return reflink(source, destination)
	}
	return FastCopy(source, destination)
}

// FastCopy uses the Win32 CopyFileW API to copy files.
// On newer Windows versions, CopyFileW will automatically use block cloning
// if the underlying filesystem supports it (e.g., ReFS, DevDrive).
// Reference:
//
//	https://devblogs.microsoft.com/engineering-at-microsoft/copy-on-write-in-win32-api-early-access/
func FastCopy(source, destination string) error {
	lpExistingFileName, err := windows.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	lpNewFileName, err := windows.UTF16PtrFromString(destination)
	if err != nil {
		return err
	}

	var bFailIfExists uint32 = 1
	r1, _, err := procCopyFileW.Call(
		uintptr(unsafe.Pointer(lpExistingFileName)),
		uintptr(unsafe.Pointer(lpNewFileName)),
		uintptr(bFailIfExists),
	)
	if r1 != 0 {
		return nil
	}
	// Handle file exists case the same way as Linux and Darwin
	if os.IsExist(err) {
		return nil
	}
	return fmt.Errorf("failed CopyFileW Win32 call from '%s' to '%s': %s", source, destination, err)
}

// Implementation is heavily inspired by Git LFS
// References:
//
//   - https://github.com/microsoft/CopyOnWrite/
//   - https://github.com/git-lfs/git-lfs/blob/main/tools/util_windows.go
//   - https://github.com/nicokoch/reflink/blob/e8d93b465f5d9ad340cd052b64bbc77b8ee107e2/src/sys/windows.rs#L199
func reflink(source, destination string) error {
	src, err := os.Open(source)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.OpenFile(destination, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
	if err != nil {
		// Handle file exists case the same way as Linux and Darwin
		if os.IsExist(err) {
			return nil
		}
		return err
	}
	// Ensure we clean up the destination file on any error path.
	reflinkWasSuccessful := false
	defer func() {
		dst.Close()
		if !reflinkWasSuccessful {
			// Best-effort cleanup; ignore errors.
			_ = os.Remove(destination)
		}
	}()

	srcStat, err := src.Stat()
	if err != nil {
		return err
	}

	fileSize := srcStat.Size()

	// set file size. There is a requirement "The destination region must not extend past the end of file."
	if err := dst.Truncate(fileSize); err != nil {
		return err
	}

	offset := int64(0)

	// Requirement
	// * The source and destination regions must begin and end at a cluster boundary. (4KiB or 64KiB)
	// * cloneRegionSize less than 4GiB.
	// see https://docs.microsoft.com/windows/win32/fileio/block-cloning
	// Clone first xGiB region.
	for ; offset+giB < fileSize; offset += giB {
		if err := callDuplicateExtentsToFile(dst, src, offset, giB); err != nil {
			return err
		}
	}

	// Determine remaining bytes after cloning 1GiB chunks. If none remain we are done.
	remaining := fileSize - offset
	if remaining == 0 {
		reflinkWasSuccessful = true
		return nil
	}

	// Clone remaining contents less than 1GiB.
	// First try with 64KiB round up, then fallback to 4KiB.
	var tailErr error
	for _, cloneRegionSize := range reFSClusterSizes {
		cloneSize := roundUp(remaining, cloneRegionSize)
		if err := callDuplicateExtentsToFile(dst, src, offset, cloneSize); err != nil {
			tailErr = errors.Join(tailErr, fmt.Errorf("failed to call DuplicateExtentsToFile with size %d: %w", cloneRegionSize, err))
			continue
		}
		// Success path: mark successful so cleanup does not remove the file.
		reflinkWasSuccessful = true
		return nil
	}
	return tailErr
}

func callDuplicateExtentsToFile(dst, src *os.File, offset int64, cloneRegionSize int64) (err error) {
	if cloneRegionSize >= 4*giB {
		return fmt.Errorf("cloneRegionSize must be less than 4GiB, got %d", cloneRegionSize)
	}

	// unused return values
	var (
		bytesReturned uint32
		overlapped    windows.Overlapped
	)

	// https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ns-winioctl-duplicate_extents_data
	type duplicateExtentsData struct {
		FileHandle       windows.Handle
		SourceFileOffset int64
		TargetFileOffset int64
		ByteCount        int64
	}
	request := duplicateExtentsData{
		FileHandle:       windows.Handle(src.Fd()),
		SourceFileOffset: offset,
		TargetFileOffset: offset,
		ByteCount:        cloneRegionSize,
	}

	// https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ni-winioctl-fsctl_duplicate_extents_to_file
	return windows.DeviceIoControl(
		windows.Handle(dst.Fd()),
		windows.FSCTL_DUPLICATE_EXTENTS_TO_FILE,
		(*byte)(unsafe.Pointer(&request)),
		uint32(unsafe.Sizeof(request)),
		nil,
		0,
		&bytesReturned,
		&overlapped)
}

func roundUp(value, base int64) int64 {
	mod := value % base
	if mod == 0 {
		return value
	}

	return value - mod + base
}
