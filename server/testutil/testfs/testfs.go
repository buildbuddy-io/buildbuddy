package testfs

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// EmptyDir is a representation of an empty directory for use in functions
	// that represent filesystem contents as a map.
	//
	// Example:
	//	testfs.WriteAllFileContents(t, dest, map[string]string{
	//		"greeting.txt": "Hello world",
	//		"emptydir": testfs.EmptyDir,
	//	})
	EmptyDir = "<empty directory>"
)

// RunfilePath returns the path to the given bazel runfile.
func RunfilePath(t testing.TB, path string) string {
	path, err := runfiles.Rlocation(path)
	require.NoError(t, err)
	return path
}

// CreateTemp creates a temp file which is automatically cleaned up after the
// test.
func CreateTemp(t testing.TB) *os.File {
	f, err := os.CreateTemp(os.Getenv("TEST_TMPDIR"), "buildbuddy-test-file-*")
	require.NoError(t, err)
	path := f.Name()
	t.Cleanup(func() {
		_ = f.Close()
		if err := os.RemoveAll(path); err != nil && !os.IsNotExist(err) {
			assert.NoError(t, err, "failed to remove temp file")
		}
	})
	return f
}

// MakeTempDir creates and returns an empty directory that exists for the scope
// of a test.
func MakeTempDir(t testing.TB) string {
	tmpDir, err := os.MkdirTemp(os.Getenv("TEST_TMPDIR"), "buildbuddy-test-*")
	if err != nil {
		assert.FailNow(t, "failed to create temp dir", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(tmpDir); err != nil && !os.IsNotExist(err) {
			assert.FailNow(t, "failed to clean up temp dir", err)
		}
	})
	return tmpDir
}

func MakeDirAll(t testing.TB, rootDir, childPath string) string {
	dir := path.Join(rootDir, childPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		assert.FailNow(t, "failed to make all dir", err)
	}
	return dir
}

func MakeSocket(t testing.TB, socketName string) string {
	socketDir, err := os.MkdirTemp("/tmp", "buildbuddy-test-*")
	if err != nil {
		assert.FailNow(t, "failed to create temp dir", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(socketDir); err != nil && !os.IsNotExist(err) {
			assert.FailNow(t, "failed to clean up temp dir", err)
		}
	})
	return path.Join(socketDir, socketName)
}

func MakeTempFile(t testing.TB, rootDir, pattern string) string {
	if rootDir == "" {
		rootDir = os.Getenv("TEST_TMPDIR")
	}
	if pattern == "" {
		pattern = "buildbuddy-test-*"
	}
	tmpFile, err := os.CreateTemp(rootDir, pattern)
	if err != nil {
		assert.FailNow(t, "failed to create temp file", err)
	}
	if err := tmpFile.Close(); err != nil {
		assert.FailNow(t, "failed to close temp file", err)
	}
	t.Cleanup(func() {
		if err := os.Remove(tmpFile.Name()); err != nil && !os.IsNotExist(err) {
			assert.FailNow(t, "failed to clean up temp file", err)
		}
	})
	return tmpFile.Name()
}

func MakeTempSymlink(t testing.TB, rootDir, pattern, dst string) string {
	name := MakeTempFile(t, rootDir, pattern)
	if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
		assert.FailNow(t, "failed to remove temp file to be replaced with symlink", err)
	}
	if err := os.Symlink(dst, name); os.IsExist(err) {
		assert.FailNow(t, "Failed to create temp symlink; file exists.", err.Error())
	} else if err != nil {
		assert.FailNow(t, "failed to create temp symlink", err)
	}
	return name
}

func CopyFile(t testing.TB, src, destRootDir, destPath string) {
	info, err := os.Stat(src)
	if err != nil {
		assert.FailNow(t, "stat failed", err)
	}
	b, err := os.ReadFile(src)
	if err != nil {
		assert.FailNow(t, "read failed", err)
	}
	if err := os.WriteFile(filepath.Join(destRootDir, destPath), b, info.Mode()); err != nil {
		assert.FailNow(t, "write failed", err)
	}
}

func MakeExecutable(t testing.TB, rootDir string, path string) {
	err := os.Chmod(filepath.Join(rootDir, path), 0755)
	require.NoError(t, err)
}

func WriteAllFileContents(t testing.TB, rootDir string, contents map[string]string) {
	for relPath, content := range contents {
		path := filepath.Join(rootDir, relPath)
		if content == EmptyDir {
			if err := os.MkdirAll(path, 0755); err != nil {
				assert.FailNow(t, "failed to create empty dir", err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			assert.FailNow(t, "failed to create parent dir for file", err)
		}
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			assert.FailNow(t, "write failed", err)
		}
		// Make scripts executable.
		if strings.HasSuffix(relPath, ".sh") {
			err := os.Chmod(path, 0755)
			require.NoError(t, err)
		}
	}
}

func WriteRandomString(t testing.TB, rootDir, path string, n int) string {
	s, err := random.RandomString(n)
	if err != nil {
		assert.FailNow(t, "failed to generate random string", err)
	}
	if err := os.WriteFile(filepath.Join(rootDir, path), []byte(s), 0644); err != nil {
		assert.FailNow(t, "write failed", err)
	}
	return s
}

func ReadFileAsString(t testing.TB, rootDir, path string) string {
	b, err := os.ReadFile(filepath.Join(rootDir, path))
	if err != nil {
		assert.FailNow(t, "read failed", err)
	}
	return string(b)
}

func Exists(t testing.TB, rootDir, path string) bool {
	_, err := os.Stat(filepath.Join(rootDir, path))
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		assert.FailNow(t, "stat failed", err)
	}
	return true
}

// AssertExactFileContents checks that the given mapping exactly represents the
// contents of rootDir. The mapping is keyed by path relative to rootDir.
// Symlinks are ignored. Empty directories must be listed in the given map
// using EmptyDir as the map value.
func AssertExactFileContents(t testing.TB, rootDir string, contents map[string]string) {
	expectedFilePaths := []string{}
	for k := range contents {
		expectedFilePaths = append(expectedFilePaths, k)
	}
	actualFilePaths := []string{}
	err := filepath.WalkDir(rootDir, func(path string, entry fs.DirEntry, err error) error {
		if !entry.Type().IsRegular() && !entry.IsDir() {
			// Ignore symlinks and special files for now.
			return nil
		}

		var actualContent string

		if entry.IsDir() {
			children, err := os.ReadDir(path)
			require.NoError(t, err)
			// Skip nonempty dirs - they don't need explicit <empty> entries
			// since they have children.
			if len(children) > 0 {
				return nil
			}
			actualContent = EmptyDir
		} else {
			b, err := os.ReadFile(path)
			require.NoError(t, err)
			actualContent = string(b)
		}

		require.NoError(t, err)
		relPath := strings.TrimPrefix(path, rootDir+string(os.PathSeparator))
		actualFilePaths = append(actualFilePaths, relPath)
		if content, ok := contents[relPath]; ok {
			require.NoError(t, err)
			assert.Equalf(t, content, string(actualContent), "unexpected contents at %s", relPath)
		}
		return nil
	})
	require.NoError(t, err)
	sort.Strings(actualFilePaths)
	sort.Strings(expectedFilePaths)
	assert.Equal(
		t, expectedFilePaths, actualFilePaths,
		"some files were missing or unexpected files were found",
	)
}
