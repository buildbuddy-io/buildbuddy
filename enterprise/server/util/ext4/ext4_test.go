//go:build linux && !android

package ext4_test

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func randomDir(subDirs []string) string {
	pathParts := []string{}
	i := 0
	for {
		j := rand.Intn(len(subDirs) - i)
		if i+j >= len(subDirs)-1 {
			break
		}
		pathParts = append(pathParts, subDirs[i+j])
		i = i + j
	}
	return filepath.Join(pathParts...)
}

func TestE2E(t *testing.T) {
	ctx := context.Background()
	rootDir := testfs.MakeTempDir(t)
	allowedPaths := []string{"a", "b", "c", "d", "e", "f"}

	// Create a "random" looking directory full of random digests.
	// Save a map of path -> digest hash.
	pathHashMap := make(map[string]string, 0)
	for i := 0; i < 100; i++ {
		parentDir := filepath.Join(rootDir, randomDir(allowedPaths))
		disk.EnsureDirectoryExists(parentDir)
		r, buf := testdigest.RandomCASResourceBuf(t, 1000)
		dest := filepath.Join(parentDir, r.GetDigest().GetHash())
		if err := os.WriteFile(dest, buf, 0644); err != nil {
			t.Fatal(err)
		}
		pathHashMap[dest] = r.GetDigest().GetHash()
	}

	// Make the random directory into an ext4 image.
	imageFile := testfs.MakeTempFile(t, "", "*.img")
	if err := ext4.DirectoryToImageAutoSize(ctx, rootDir, imageFile); err != nil {
		t.Fatal(err)
	}

	// Create a new (empty) directory and unpack the image file to the new directory.
	newRoot := testfs.MakeTempDir(t)
	if err := ext4.ImageToDirectory(ctx, imageFile, newRoot, []string{"/"}); err != nil {
		t.Fatal(err)
	}

	// Ensure that everything in the image, plus the few random files we
	// added manually, is in the new directory.
	for path, hash := range pathHashMap {
		newPath := strings.Replace(path, rootDir, newRoot, 1)
		f, err := os.Open(newPath)
		if err != nil {
			t.Fatal(err)
		}
		d, err := digest.Compute(f, repb.DigestFunction_SHA256)
		if err != nil {
			t.Fatal(err)
		}
		if d.GetHash() != hash {
			t.Fatalf("File content mismatch: %q != %q, %q", d.GetHash(), hash, newPath)
		}
	}
}

func TestDirectoryToImageAutoSize_NonExistentDir(t *testing.T) {
	ctx := context.Background()
	root := testfs.MakeTempDir(t)

	err := ext4.DirectoryToImageAutoSize(ctx, "/does/not/exist", filepath.Join(root, "/image.ext4"))

	require.Error(t, err)
}

func TestDirectoryToImageAutoSize_DanglingSymlink(t *testing.T) {
	ctx := context.Background()
	root := testfs.MakeTempDir(t)
	workspace := testfs.MakeDirAll(t, root, "workspace")
	err := os.Symlink("/does/not/exist", filepath.Join(workspace, "a"))
	require.NoError(t, err)

	err = ext4.DirectoryToImageAutoSize(ctx, workspace, filepath.Join(root, "workspace.ext4"))

	require.NoError(t, err)
}

func TestImageToDirectory(t *testing.T) {
	for _, test := range []struct {
		name     string
		image    map[string]string
		paths    []string
		expected map[string]string
	}{
		{
			name: "passing the root path extracts the whole image",
			image: map[string]string{
				"foo/bar/hello.txt": "hello",
				"world.txt":         "world",
			},
			paths: []string{"/"},
			expected: map[string]string{
				"foo/bar/hello.txt": "hello",
				"world.txt":         "world",
				// ext4 recovery dir
				"lost+found": testfs.EmptyDir,
			},
		},
		{
			name: "file paths are allowed",
			image: map[string]string{
				"ignore.txt":         "IGNORE_ME",
				"foo/hello.txt":      "hello",
				"foo/ignore.txt":     "hello",
				"foo/bar/ignore.txt": "IGNORE_ME",
			},
			paths: []string{"/foo/hello.txt"},
			expected: map[string]string{
				"foo/hello.txt": "hello",
			},
		},
		{
			name: "directory paths are allowed",
			image: map[string]string{
				"parent1/child1/hello.txt":  "hello",
				"parent1/child2/ignore.txt": "IGNORE_ME",
				"parent2/world.txt":         "world",
			},
			paths: []string{"/parent1/child1", "/parent2/"},
			expected: map[string]string{
				"parent1/child1/hello.txt": "hello",
				"parent2/world.txt":        "world",
			},
		},
		{
			name: "nonexistent paths are silently ignored",
			image: map[string]string{
				"parent1/child1/hello.txt": "hello",
				"world.txt":                "world",
			},
			paths: []string{
				"/does_not_exist",
				"/parent1/does_not_exist",
				"/parent1/child1/hello.txt",
			},
			expected: map[string]string{
				"parent1/child1/hello.txt": "hello",
			},
		},
		{
			name: "empty directories are copied",
			image: map[string]string{
				"parent1/emptydir1": testfs.EmptyDir,
				"parent2/emptydir2": testfs.EmptyDir,
				"emptydir3":         testfs.EmptyDir,
			},
			paths: []string{"/parent1/emptydir1", "/parent2", "/emptydir3"},
			expected: map[string]string{
				"parent1/emptydir1": testfs.EmptyDir,
				"parent2/emptydir2": testfs.EmptyDir,
				"emptydir3":         testfs.EmptyDir,
			},
		},
		{
			name: "duplicate paths are allowed",
			image: map[string]string{
				"parent1/hello.txt": "hello",
				"parent2/world.txt": "world",
			},
			paths: []string{"/parent1", "/parent1/hello.txt", "/parent2", "/parent2"},
			expected: map[string]string{
				"parent1/hello.txt": "hello",
				"parent2/world.txt": "world",
			},
		},
		{
			name: "paths not beginning with slash are allowed",
			image: map[string]string{
				"parent1/hello.txt": "hello",
				"parent2/world.txt": "world",
			},
			paths: []string{"parent1"},
			expected: map[string]string{
				"parent1/hello.txt": "hello",
			},
		},
		{
			name: "paths beginning with dotslash are allowed",
			image: map[string]string{
				"parent1/hello.txt": "hello",
				"parent2/world.txt": "world",
			},
			paths: []string{"./parent1"},
			expected: map[string]string{
				"parent1/hello.txt": "hello",
			},
		},
		{
			name: "empty path is the same as root path",
			image: map[string]string{
				"foo/bar/hello.txt": "hello",
				"world.txt":         "world",
			},
			paths: []string{""},
			expected: map[string]string{
				"foo/bar/hello.txt": "hello",
				"world.txt":         "world",
				// ext4 recovery dir
				"lost+found": testfs.EmptyDir,
			},
		},
		{
			name: "dot is the same as root path",
			image: map[string]string{
				"foo/bar/hello.txt": "hello",
				"world.txt":         "world",
			},
			paths: []string{"."},
			expected: map[string]string{
				"foo/bar/hello.txt": "hello",
				"world.txt":         "world",
				// ext4 recovery dir
				"lost+found": testfs.EmptyDir,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			inputDir := testfs.MakeTempDir(t)
			testfs.WriteAllFileContents(t, inputDir, test.image)
			tmp := testfs.MakeTempDir(t)
			imagePath := filepath.Join(tmp, "image.ext4")
			err := ext4.DirectoryToImageAutoSize(ctx, inputDir, imagePath)
			require.NoError(t, err)
			outputDir := testfs.MakeTempDir(t)

			err = ext4.ImageToDirectory(ctx, imagePath, outputDir, test.paths)
			require.NoError(t, err)

			testfs.AssertExactFileContents(t, outputDir, test.expected)
		})
	}
}
