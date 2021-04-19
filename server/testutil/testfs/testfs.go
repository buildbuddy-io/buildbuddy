package testfs

import (
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MakeTempDir creates and returns an empty directory that exists for the scope
// of a test.
func MakeTempDir(t testing.TB) string {
	syscall.Umask(0)
	tmpDir, err := ioutil.TempDir("", "buildbuddy-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Fatal(err)
		}
	})
	if err := os.Chmod(tmpDir, 0777); err != nil {
		t.Fatal(err)
	}
	return tmpDir
}

func CopyFile(t testing.TB, src, destRootDir, destPath string) {
	info, err := os.Stat(src)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ioutil.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(filepath.Join(destRootDir, destPath), b, info.Mode()); err != nil {
		t.Fatal(err)
	}
}

func WriteAllFileContents(t testing.TB, rootDir string, contents map[string]string) {
	for relPath, content := range contents {
		path := filepath.Join(rootDir, relPath)
		if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
			t.Fatal(err)
		}
		if err := ioutil.WriteFile(path, []byte(content), 0777); err != nil {
			t.Fatal(err)
		}
	}
}

func WriteRandomString(t testing.TB, rootDir, path string, n int) string {
	s, err := random.RandomString(n)
	if err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(filepath.Join(rootDir, path), []byte(s), 0777); err != nil {
		t.Fatal(err)
	}
	return s
}

func ReadFileAsString(t testing.TB, rootDir, path string) string {
	b, err := ioutil.ReadFile(filepath.Join(rootDir, path))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func Exists(t testing.TB, rootDir, path string) bool {
	_, err := os.Stat(filepath.Join(rootDir, path))
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		t.Fatal(err)
	}
	return true
}

// AssertExactFileContents checks that the given mapping exactly represents the
// files in rootDir. The mapping is keyed by path relative to rootDir.
// Empty dirs and non-regular files (e.g. symlinks) are ignored in the
// comparison.
func AssertExactFileContents(t testing.TB, rootDir string, contents map[string]string) {
	expectedFilePaths := []string{}
	for k, _ := range contents {
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
			actualContent, err := ioutil.ReadFile(path)
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
