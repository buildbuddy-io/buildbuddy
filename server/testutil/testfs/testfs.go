package testfs

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RunfilePath returns the path to the given bazel runfile.
func RunfilePath(t testing.TB, path string) string {
	path, err := bazel.Runfile(path)
	require.NoError(t, err)
	return path
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
		if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
			assert.FailNow(t, "failed to create parent dir for file", err)
		}
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			assert.FailNow(t, "write failed", err)
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
// files in rootDir. The mapping is keyed by path relative to rootDir.
// Empty dirs and non-regular files (e.g. symlinks) are ignored in the
// comparison.
func AssertExactFileContents(t testing.TB, rootDir string, contents map[string]string) {
	expectedFilePaths := []string{}
	for k := range contents {
		expectedFilePaths = append(expectedFilePaths, k)
	}
	actualFilePaths := []string{}
	err := filepath.WalkDir(rootDir, func(path string, entry fs.DirEntry, err error) error {
		require.NoError(t, err)
		if !entry.Type().IsRegular() {
			return nil
		}
		relPath := strings.TrimPrefix(path, rootDir+string(os.PathSeparator))
		actualFilePaths = append(actualFilePaths, relPath)
		if content, ok := contents[relPath]; ok {
			actualContent, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.Equalf(t, content, string(actualContent), "unexpected contents in %s", relPath)
		}
		return nil
	})
	require.NoError(t, err)
	assert.ElementsMatch(
		t, expectedFilePaths, actualFilePaths,
		"some files were missing or unexpected files were found",
	)
}
