package vfs_test

import (
	"context"
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

	server := vfs_server.New(env, back)
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
	Ino   uint64
	Nlink uint64
	Atime time.Time
	Mtime time.Time
}

func toRawStats(fi fs.FileInfo) *rawStats {
	rs := fi.Sys().(*syscall.Stat_t)
	return &rawStats{
		Ino:   rs.Ino,
		Nlink: rs.Nlink,
		Mtime: time.Unix(rs.Mtim.Sec, rs.Mtim.Nsec),
		Atime: time.Unix(rs.Atim.Sec, rs.Atim.Nsec),
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
