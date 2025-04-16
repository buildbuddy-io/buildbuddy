//go:build linux && !android && (amd64 || arm64)

package vfs_test

import (
	"context"
	"crypto/rand"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vfs"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func setupVFS(t *testing.T) string {
	tmp := testfs.MakeTempDir(t)
	mnt := filepath.Join(tmp, "vfs")
	err := os.MkdirAll(mnt, 0755)
	require.NoError(t, err)
	back := filepath.Join(tmp, "back")
	err = os.MkdirAll(back, 0755)
	require.NoError(t, err)

	env := testenv.GetTestEnv(t)

	server, err := vfs_server.New(env, back)
	require.NoError(t, err)
	err = server.Prepare(context.Background(), &container.FileSystemLayout{
		Inputs: &repb.Tree{
			Root: &repb.Directory{},
		},
	})
	require.NoError(t, err)

	client := vfs_server.NewDirectClient(server)
	fs := vfs.New(client, mnt, &vfs.Options{
		Verbose: true,
		//LogFUSEOps: true,
	})
	err = fs.Mount()
	require.NoError(t, err)
	t.Cleanup(func() {
		err = fs.Unmount()
		if err != nil {
			log.Warningf("unmount failed: %s", err)
		}
	})
	return mnt
}

type dirEntry struct {
	name string
	size int
	mode os.FileMode
}

func checkDirectoryContents(t *testing.T, path string, expected []dirEntry) {
	dirEntries, err := os.ReadDir(path)
	require.NoError(t, err)

	var parsedEntries []dirEntry
	for _, e := range dirEntries {
		info, err := e.Info()
		require.NoError(t, err)
		entry := dirEntry{
			name: info.Name(),
			mode: info.Mode() & fs.ModeType,
		}
		if !info.IsDir() {
			entry.size = int(info.Size())
		}
		parsedEntries = append(parsedEntries, entry)
	}

	require.Equal(t, expected, parsedEntries)
}

func TestReaddir(t *testing.T) {
	fsPath := setupVFS(t)

	// Write some test files at top level and subdirectory.
	testData1 := "hello world"
	testFile1 := "hello.txt"
	testFile1Path := filepath.Join(fsPath, testFile1)
	err := os.WriteFile(testFile1Path, []byte(testData1), 0600)
	require.NoError(t, err)

	testData2 := "hello world 2"
	testFile2 := "hello2.txt"
	testFile2Path := filepath.Join(fsPath, testFile2)
	err = os.WriteFile(testFile2Path, []byte(testData2), 0700)
	require.NoError(t, err)

	subDir := "dir1"
	subDirPath := filepath.Join(fsPath, subDir)
	err = os.MkdirAll(subDirPath, 0700)
	require.NoError(t, err)

	subDirFileData := "hello hello"
	subDirFile := "world.txt"
	subDirFilePath := filepath.Join(subDirPath, subDirFile)
	err = os.WriteFile(subDirFilePath, []byte(subDirFileData), 0666)
	require.NoError(t, err)

	checkDirectoryContents(t, fsPath, []dirEntry{
		{
			name: subDir,
			mode: fs.ModeDir,
		},
		{
			name: testFile1,
			size: len(testData1),
		},
		{
			name: testFile2,
			size: len(testData2),
		},
	})
	checkDirectoryContents(t, subDirPath, []dirEntry{
		{
			name: subDirFile,
			size: len(subDirFileData),
		},
	})

	// Now write a new file and make sure it's reflected in the directory contents.
	newTestData := "new data"
	newTestFile := "new.txt"
	newTestFilePath := filepath.Join(fsPath, newTestFile)
	err = os.WriteFile(newTestFilePath, []byte(newTestData), 0600)
	require.NoError(t, err)
	checkDirectoryContents(t, fsPath, []dirEntry{
		{
			name: subDir,
			mode: fs.ModeDir,
		},
		{
			name: testFile1,
			size: len(testData1),
		},
		{
			name: testFile2,
			size: len(testData2),
		},
		{
			name: newTestFile,
			size: len(newTestData),
		},
	})

	// Now delete one of the files.
	err = os.Remove(testFile2Path)
	require.NoError(t, err)
	checkDirectoryContents(t, fsPath, []dirEntry{
		{
			name: subDir,
			mode: fs.ModeDir,
		},
		{
			name: testFile1,
			size: len(testData1),
		},
		{
			name: newTestFile,
			size: len(newTestData),
		},
	})

	// Now overwrite the contents of a file.
	newData := "goodbye old data"
	err = os.WriteFile(testFile1Path, []byte(newData), 0700)
	require.NoError(t, err)
	checkDirectoryContents(t, fsPath, []dirEntry{
		{
			name: subDir,
			mode: fs.ModeDir,
		},
		{
			name: testFile1,
			size: len(newData),
		},
		{
			name: newTestFile,
			size: len(newTestData),
		},
	})
}

func stat(t *testing.T, path string) os.FileInfo {
	fi, err := os.Stat(path)
	require.NoError(t, err)
	return fi
}

func TestFileOps(t *testing.T) {
	fsPath := setupVFS(t)

	testFile := "hello.txt"
	testFilePath := filepath.Join(fsPath, testFile)

	f, err := os.Create(testFilePath)
	require.NoError(t, err)
	defer f.Close()
	require.EqualValues(t, 0, stat(t, testFilePath).Size())

	testContents := "hello"
	_, err = f.Write([]byte(testContents))
	require.NoError(t, err)
	require.EqualValues(t, len(testContents), stat(t, testFilePath).Size())

	err = syscall.Fallocate(int(f.Fd()), 0, 0, 1000)
	require.NoError(t, err)
	require.EqualValues(t, 1000, stat(t, testFilePath).Size())

	err = syscall.Fallocate(int(f.Fd()), 0, 500, 1000)
	require.NoError(t, err)
	require.EqualValues(t, 1500, stat(t, testFilePath).Size())

	err = syscall.Fallocate(int(f.Fd()), 0, 0, 10)
	require.NoError(t, err)
	require.EqualValues(t, 1500, stat(t, testFilePath).Size())

	err = f.Close()
	require.NoError(t, err)

	f, err = os.Open(testFilePath)
	require.NoError(t, err)
	buf := make([]byte, 2000)
	n, err := f.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1500, n)
	expectedData := make([]byte, n)
	copy(expectedData, testContents)
	require.Equal(t, expectedData, buf[:n])

	err = f.Close()
	require.NoError(t, err)

	err = os.Remove(testFilePath)
	require.NoError(t, err)
	_, err = os.Stat(testFilePath)
	require.ErrorIs(t, err, os.ErrNotExist)

}

func TestNestedDirs(t *testing.T) {
	fsPath := setupVFS(t)

	testContents := "hello"
	subDir := filepath.Join(fsPath, "subdir1")
	nestedFile := filepath.Join(subDir, "nested.txt")
	nestedSubDir := filepath.Join(subDir, "subdir2")
	deeplyNestedSubdir := filepath.Join(nestedSubDir, "subdir3")
	deeplyNestedFile := filepath.Join(deeplyNestedSubdir, "deeplynested.txt")
	err := os.MkdirAll(deeplyNestedSubdir, 0644)
	require.NoError(t, err)
	err = os.WriteFile(deeplyNestedFile, []byte(testContents), 0755)
	require.NoError(t, err)
	err = os.WriteFile(nestedFile, []byte(testContents), 0755)
	require.NoError(t, err)

	// Read back the file.
	bs, err := os.ReadFile(deeplyNestedFile)
	require.NoError(t, err)
	require.Equal(t, testContents, string(bs))

	// Delete the files and verify we can delete the empty nested directories.
	err = os.Remove(deeplyNestedFile)
	require.NoError(t, err)
	err = os.Remove(nestedFile)
	require.NoError(t, err)

	err = os.Remove(deeplyNestedSubdir)
	require.NoError(t, err)
	err = os.Remove(nestedSubDir)
	require.NoError(t, err)
	err = os.Remove(subDir)
	require.NoError(t, err)
}

func TestSymlinks(t *testing.T) {
	fsPath := setupVFS(t)

	testFile := "hello.txt"
	testContents := "hello"
	testFilePath := filepath.Join(fsPath, testFile)

	err := os.WriteFile(testFilePath, []byte(testContents), 0644)
	require.NoError(t, err)

	symlinkName := "sym"
	symlinkPath := filepath.Join(fsPath, symlinkName)
	err = os.Symlink(testFilePath, symlinkPath)
	require.NoError(t, err)

	data, err := os.ReadFile(testFilePath)
	require.NoError(t, err)
	require.Equal(t, testContents, string(data))

	rl, err := os.Readlink(symlinkPath)
	require.NoError(t, err)
	require.Equal(t, testFilePath, rl)

	_, err = os.Lstat(symlinkPath)
	require.NoError(t, err)

	subDir := "subdir"
	subDirPath := filepath.Join(fsPath, subDir)
	err = os.Mkdir(subDirPath, 0755)
	require.NoError(t, err)

	subDirSymlinkName := "sym2"
	subDirSymlinkPath := filepath.Join(fsPath, subDirSymlinkName)
	err = os.Symlink(subDirPath, subDirSymlinkPath)
	require.NoError(t, err)

	subDirFile := "subfile.txt"
	subDirFilePath := filepath.Join(subDirSymlinkPath, subDirFile)
	err = os.WriteFile(subDirFilePath, []byte(subDirFile), 0644)
	require.NoError(t, err)

	des, err := os.ReadDir(subDirSymlinkPath)
	require.NoError(t, err)
	require.Len(t, des, 1)
	require.Equal(t, subDirFile, des[0].Name())

	rl, err = os.Readlink(subDirSymlinkPath)
	require.NoError(t, err)
	require.Equal(t, subDirPath, rl)

	_, err = os.Lstat(subDirSymlinkPath)
	require.NoError(t, err)
}

type rawStats struct {
	Ino       uint64
	Nlink     uint64
	Atime     time.Time
	Mtime     time.Time
	Blocks    int64
	BlockSize int64
	Size      int64
}

func toRawStats(fi fs.FileInfo) *rawStats {
	rs := fi.Sys().(*syscall.Stat_t)
	return &rawStats{
		Ino:       rs.Ino,
		Nlink:     uint64(rs.Nlink),
		Mtime:     time.Unix(rs.Mtim.Sec, rs.Mtim.Nsec),
		Atime:     time.Unix(rs.Atim.Sec, rs.Atim.Nsec),
		Blocks:    rs.Blocks,
		BlockSize: int64(rs.Blksize),
		Size:      rs.Size,
	}
}

func rawStat(t *testing.T, path string) *rawStats {
	fi, err := os.Stat(path)
	require.NoError(t, err)
	return toRawStats(fi)
}

func TestHardlinks(t *testing.T) {
	fsPath := setupVFS(t)

	testFile := "hello.txt"
	testContents := "hello"
	testFilePath := filepath.Join(fsPath, testFile)

	err := os.WriteFile(testFilePath, []byte(testContents), 0644)
	require.NoError(t, err)

	// Newly created file should have a single link.
	fi := rawStat(t, testFilePath)
	require.EqualValues(t, 1, fi.Nlink)

	hardlinkName := "bye"
	hardlinkPath := filepath.Join(fsPath, hardlinkName)
	err = os.Link(testFilePath, hardlinkPath)
	require.NoError(t, err)

	// The original and new files should both have a link count of 2 and the
	// same inode number.
	origFI := rawStat(t, testFilePath)
	require.EqualValues(t, 2, origFI.Nlink)
	newFI := rawStat(t, hardlinkPath)
	require.EqualValues(t, 2, newFI.Nlink)
	require.Equal(t, origFI.Ino, newFI.Ino)

	// Reading from the hardlink should return the same contents as the
	// original file.
	bs, err := os.ReadFile(hardlinkPath)
	require.NoError(t, err)
	require.Equal(t, testContents, string(bs))

	// Changes to one file should be reflected across all links.
	testContents = "bonjour"
	err = os.WriteFile(hardlinkPath, []byte(testContents), 0644)
	require.NoError(t, err)
	bs, err = os.ReadFile(hardlinkPath)
	require.NoError(t, err)
	require.Equal(t, testContents, string(bs))
	bs, err = os.ReadFile(testFilePath)
	require.NoError(t, err)
	require.Equal(t, testContents, string(bs))

	// Removing one of the links.
	err = os.Remove(hardlinkPath)
	require.NoError(t, err)

	// Link count should go back to 1 on the original file after the hardlink
	// is removed.
	origFI = rawStat(t, testFilePath)
	require.EqualValues(t, 1, origFI.Nlink)

	// Should still be able to read the original file when hardlink is removed.
	bs, err = os.ReadFile(testFilePath)
	require.NoError(t, err)
	require.Equal(t, testContents, string(bs))

	// Create the hardlink again, and this time delete the original file.
	err = os.Link(testFilePath, hardlinkPath)
	require.NoError(t, err)
	err = os.Remove(testFilePath)
	require.NoError(t, err)

	// Verify that we're still able to read the file through the hardlinked
	// file.
	bs, err = os.ReadFile(hardlinkPath)
	require.NoError(t, err)
	require.Equal(t, testContents, string(bs))

	// Replace the original file at the same path with different contents.
	// Any existing hardlinks should still reference the original data.
	replacedContents := "bananas"
	err = os.WriteFile(testFilePath, []byte(replacedContents), 0644)
	require.NoError(t, err)
	bs, err = os.ReadFile(hardlinkPath)
	require.NoError(t, err)
	require.Equal(t, testContents, string(bs))
}

func TestSparseFile(t *testing.T) {
	fsPath := setupVFS(t)
	sparseFile := filepath.Join(fsPath, "sparse")

	f, err := os.Create(sparseFile)
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)

	// Extend the file length, should be all holes at this point.
	err = syscall.Truncate(sparseFile, 65535)
	require.NoError(t, err)

	f, err = os.OpenFile(sparseFile, os.O_RDWR, 0644)
	require.NoError(t, err)
	defer f.Close()

	// No data yet, so we should get back an ENXIO error.
	_, err = f.Seek(0, unix.SEEK_DATA)
	require.ErrorIs(t, err, syscall.ENXIO)

	// Verify file is not using any data on disk.
	rs := rawStat(t, sparseFile)
	require.EqualValues(t, 0, rs.Blocks)

	// Write some data at an offset and verify that we can find it using seek.
	dataOff := rs.BlockSize
	_, err = f.WriteAt([]byte("z"), dataOff)
	require.NoError(t, err)
	off, err := f.Seek(0, unix.SEEK_DATA)
	require.NoError(t, err)
	require.EqualValues(t, dataOff, off)

	// Verify that only one block is reported as being used.
	// N.B. Stat always reports in 512 byte blocks so we need to convert to
	// BlockSize blocks.
	rs = rawStat(t, sparseFile)
	require.EqualValues(t, 1, (rs.Blocks*512)/rs.BlockSize)
}

func TestTimestamps(t *testing.T) {
	fsPath := setupVFS(t)

	testFile := "hello.txt"
	testContents := "hello"
	testFilePath := filepath.Join(fsPath, testFile)

	// Create a new file and verify the timestamps are current.
	err := os.WriteFile(testFilePath, []byte(testContents), 0644)
	require.NoError(t, err)
	rs := rawStat(t, testFilePath)
	require.NoError(t, err)
	require.Less(t, time.Since(rs.Mtime).Seconds(), float64(5))
	require.InDelta(t, rs.Mtime.UnixMilli(), rs.Atime.UnixMilli(), 10)

	// Manually reset the mtime and check that it has been updated.
	nextYear := time.Now().Add(365 * 24 * time.Hour)
	err = os.Chtimes(testFilePath, time.Time{}, nextYear)
	require.NoError(t, err)

	rs = rawStat(t, testFilePath)
	require.NoError(t, err)
	require.True(t, rs.Mtime.Equal(nextYear), "mtime is %q", rs.Mtime)
	// atime should not have been affected.
	require.Less(t, time.Since(rs.Atime).Seconds(), float64(5))

	es, err := os.ReadDir(fsPath)
	require.NoError(t, err)
	require.Len(t, es, 1)
	fi, err := es[0].Info()
	require.NoError(t, err)
	rs = toRawStats(fi)
	require.True(t, rs.Mtime.Equal(nextYear), "file mod time is %s", fi.ModTime())
	require.Less(t, time.Since(rs.Atime).Seconds(), float64(5))

	// Read the file and verify mtime does not change.
	_, err = os.ReadFile(testFilePath)
	require.NoError(t, err)
	rs = rawStat(t, testFilePath)
	require.True(t, rs.Mtime.Equal(nextYear), "file mod time is %s", fi.ModTime())

	// Write to the file and check that mtime has been reset.
	oldAtime := rs.Atime
	err = os.WriteFile(testFilePath, []byte(testContents), 0644)
	require.NoError(t, err)
	rs = rawStat(t, testFilePath)
	require.NoError(t, err)
	require.Less(t, time.Since(rs.Mtime).Seconds(), float64(5))
	require.Equal(t, oldAtime, rs.Atime)

	// Manually reset the atime and check that it has been updated.
	err = os.Chtimes(testFilePath, nextYear, time.Time{})
	require.NoError(t, err)
	rs = rawStat(t, testFilePath)
	require.NoError(t, err)
	require.True(t, rs.Atime.Equal(nextYear))
	// mtime should not have been affected.
	require.Less(t, time.Since(rs.Mtime).Seconds(), float64(5))

	// Read the file and verify that only atime has changed.
	time.Sleep(10 * time.Millisecond)
	oldMTime := rs.Mtime
	_, err = os.ReadFile(testFilePath)
	require.NoError(t, err)
	rs = rawStat(t, testFilePath)
	require.Equal(t, oldMTime, rs.Mtime)
	require.Greater(t, rs.Atime, rs.Mtime)
}

func TestSetAttr(t *testing.T) {
	fsPath := setupVFS(t)

	testFile := "hello.txt"
	testFilePath := filepath.Join(fsPath, testFile)
	err := os.WriteFile(testFilePath, []byte("a"), 0644)
	require.NoError(t, err)

	// Create a hard link to test link count.
	err = os.Link(testFilePath, testFilePath+".clone")
	require.NoError(t, err)

	err = os.Chmod(testFilePath, 0755)
	require.NoError(t, err)

	err = syscall.Truncate(testFilePath, 1000)
	require.NoError(t, err)

	// Try linking again to verify nlink was correctly  returned in the
	// SetAttr call. Stat below calls GetAttr which does not exercise the attrs
	// returned by SetAttr.
	// This is a regression test for a previous issue where we were not setting
	// nlink in the attrs returned by SetAttr.
	err = os.Link(testFilePath, testFilePath+".clone2")
	require.NoError(t, err)

	rs := rawStat(t, testFilePath)
	require.EqualValues(t, 3, rs.Nlink)
	require.EqualValues(t, rs.BlockSize/512, rs.Blocks)
	require.EqualValues(t, 1000, rs.Size)
}

func TestMkdir(t *testing.T) {
	fsPath := setupVFS(t)

	testDir := "dir"
	testDirPath := filepath.Join(fsPath, testDir)
	err := os.Mkdir(testDirPath, 0700)
	require.NoError(t, err)

	rs := rawStat(t, testDirPath)
	require.Greater(t, rs.Size, int64(0))
}

func nullTerminated(bs []byte) []byte {
	return append(bs, 0)
}

func TestExtendedAttrs(t *testing.T) {
	fsPath := setupVFS(t)

	testFile := "hello.txt"
	testFilePath := filepath.Join(fsPath, testFile)
	err := os.WriteFile(testFilePath, []byte("a"), 0644)
	require.NoError(t, err)

	// Simple set/get check.
	key1 := "key1"
	data1 := []byte("val1")
	err = unix.Setxattr(testFilePath, key1, data1, 0)
	require.NoError(t, err)
	sz, err := unix.Getxattr(testFilePath, key1, nil)
	require.NoError(t, err)
	require.EqualValues(t, len(data1)+1, sz)
	buf := make([]byte, 1024)
	sz, err = unix.Getxattr(testFilePath, key1, buf)
	require.NoError(t, err)
	require.Equal(t, len(data1)+1, sz)
	require.Equal(t, nullTerminated(data1), buf[:sz])

	// Replace an existing attribute.
	newData := []byte("newVal")
	err = unix.Setxattr(testFilePath, key1, newData, 0)
	require.NoError(t, err)
	sz, err = unix.Getxattr(testFilePath, key1, nil)
	require.NoError(t, err)
	require.EqualValues(t, len(newData)+1, sz)
	buf = make([]byte, 1024)
	sz, err = unix.Getxattr(testFilePath, key1, buf)
	require.NoError(t, err)
	require.Equal(t, len(newData)+1, sz)
	require.Equal(t, nullTerminated(newData), buf[:sz])

	// XATTR_CREATE on an existing attribute should fail.
	err = unix.Setxattr(testFilePath, key1, newData, unix.XATTR_CREATE)
	require.ErrorIs(t, err, syscall.EEXIST)

	// XATTR_REPLACE on a non-existent attribute should fail.
	err = unix.Setxattr(testFilePath, "no-such-key", newData, unix.XATTR_REPLACE)
	require.ErrorIs(t, err, syscall.ENODATA)

	// Remove the attribute.
	err = unix.Removexattr(testFilePath, key1)
	require.NoError(t, err)

	// Removing non-existent attribute should fail.
	err = unix.Removexattr(testFilePath, key1)
	require.ErrorIs(t, err, syscall.ENODATA)

	// Test buffer sizes for "get".
	{
		err = unix.Setxattr(testFilePath, key1, data1, 0)
		require.NoError(t, err)

		// Not enough space for terminating null.
		buf = make([]byte, len(data1))
		_, err = unix.Getxattr(testFilePath, key1, buf)
		require.ErrorIs(t, err, syscall.ERANGE)

		// Exactly the right amount of space.
		buf = make([]byte, len(data1)+1)
		_, err = unix.Getxattr(testFilePath, key1, buf)
		require.NoError(t, err)
	}

	// Set multiple attributes.
	err = unix.Setxattr(testFilePath, key1, data1, 0)
	require.NoError(t, err)
	key2 := "key2"
	data2 := []byte("some-other-val")
	err = unix.Setxattr(testFilePath, key2, data2, 0)
	require.NoError(t, err)
	sz, err = unix.Listxattr(testFilePath, nil)
	require.NoError(t, err)
	require.Equal(t, len(key1)+len(key2)+2, sz)
	buf = make([]byte, 1024)
	sz, err = unix.Listxattr(testFilePath, buf)
	require.NoError(t, err)
	require.Equal(t, len(key1)+len(key2)+2, sz)
	require.Equal(t, nullTerminated([]byte(key1)), buf[:len(key1)+1])
	require.Equal(t, nullTerminated([]byte(key2)), buf[len(key1)+1:sz])
}

func TestStatfs(t *testing.T) {
	fsPath := setupVFS(t)

	stats := &unix.Statfs_t{}
	err := unix.Statfs(fsPath, stats)
	require.NoError(t, err)
	require.Greater(t, stats.Bsize, int64(0))
	require.Greater(t, stats.Blocks, uint64(0))
	require.Greater(t, stats.Bfree, uint64(0))
	require.Greater(t, stats.Bavail, uint64(0))
	require.Equal(t, stats.Bfree, stats.Blocks)
	require.Equal(t, stats.Bavail, stats.Blocks)
	log.Infof("stats %+v", stats)

	data := make([]byte, stats.Bsize*2)
	rand.Read(data)
	testFile := "hello.txt"
	testFilePath := filepath.Join(fsPath, testFile)
	err = os.WriteFile(testFilePath, data, 0644)
	require.NoError(t, err)

	err = unix.Statfs(fsPath, stats)
	require.NoError(t, err)
	log.Infof("stats %+v", stats)
	require.Equal(t, stats.Bfree, stats.Blocks-2)
	require.Equal(t, stats.Bavail, stats.Blocks-2)

	err = os.Remove(testFilePath)
	require.NoError(t, err)
	err = unix.Statfs(fsPath, stats)
	require.NoError(t, err)
	log.Infof("stats %+v", stats)
	require.Equal(t, stats.Bfree, stats.Blocks)
	require.Equal(t, stats.Bavail, stats.Blocks)
}

func TestAttrCaching(t *testing.T) {
	fsPath := setupVFS(t)

	testFile := filepath.Join(fsPath, "hello.txt")
	f, err := os.Create(testFile)
	require.NoError(t, err)
	defer f.Close()

	// File should be empty.
	s := rawStat(t, testFile)
	require.EqualValues(t, 0, s.Size)

	// While a file is opened there shouldn't be any caching happening.
	// This should be reflected in stat always returning the latest data.
	_, err = f.Write([]byte("hello"))
	require.NoError(t, err)
	s = rawStat(t, testFile)
	require.EqualValues(t, 5, s.Size)

	// Create a bunch of hardlinks and verify the link count is correctly
	// visible on all links.
	linkFiles := []string{testFile + ".link1", testFile + ".link2", testFile + ".link3"}
	for _, lf := range linkFiles {
		err = os.Link(testFile, lf)
		require.NoError(t, err)
	}
	for _, lf := range linkFiles {
		s := rawStat(t, lf)
		require.EqualValues(t, 4, s.Nlink)
	}
}
