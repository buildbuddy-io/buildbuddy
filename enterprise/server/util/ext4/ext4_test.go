//go:build linux && !android

package ext4_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
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

// writeRandomFile writes a file filled with pseudo-random bytes of the given size.
func writeRandomFile(b *testing.B, dir, name string, size int, rng *rand.Rand) {
	b.Helper()
	buf := make([]byte, size)
	rng.Read(buf)
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, buf, 0644); err != nil {
		b.Fatal(err)
	}
}

// makeDockerImageDir creates a directory structure resembling a ~1GB container
// image. If parentDir is non-empty, the directory is created under it.
func makeDockerImageDir(b *testing.B, parentDir string) string {
	b.Helper()
	var root string
	if parentDir != "" {
		var err error
		root, err = os.MkdirTemp(parentDir, "dockerimg-*")
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { os.RemoveAll(root) })
	} else {
		root = testfs.MakeTempDir(b)
	}

	dirs := []string{
		"etc", "etc/apt", "etc/default", "etc/security", "etc/ssl/certs",
		"usr/bin", "usr/sbin",
		"usr/lib/x86_64-linux-gnu", "usr/lib/python3/dist-packages",
		"usr/share/doc", "usr/share/man/man1", "usr/share/locale",
		"var/lib/apt/lists", "var/lib/dpkg/info",
		"var/cache", "var/log",
		"opt/app/data", "opt/app/lib",
		"tmp",
	}
	for _, d := range dirs {
		if err := os.MkdirAll(filepath.Join(root, d), 0755); err != nil {
			b.Fatal(err)
		}
	}

	rng := rand.New(rand.NewSource(42))
	var totalBytes int64

	// /etc: ~200 small config files, 512B-4KB each (~400KB total)
	for i := 0; i < 200; i++ {
		size := 512 + rng.Intn(3584)
		writeRandomFile(b, filepath.Join(root, "etc"), fmt.Sprintf("config_%03d.conf", i), size, rng)
		totalBytes += int64(size)
	}

	// /usr/bin: ~300 executables, 50KB-500KB each (~80MB total)
	for i := 0; i < 300; i++ {
		size := 50*1024 + rng.Intn(450*1024)
		writeRandomFile(b, filepath.Join(root, "usr/bin"), fmt.Sprintf("binary_%03d", i), size, rng)
		totalBytes += int64(size)
	}

	// /usr/lib shared libraries: ~150 files, 1MB-10MB each (~800MB total)
	for i := 0; i < 150; i++ {
		size := 1*1024*1024 + rng.Intn(9*1024*1024)
		writeRandomFile(b, filepath.Join(root, "usr/lib/x86_64-linux-gnu"), fmt.Sprintf("lib%03d.so.1.0", i), size, rng)
		totalBytes += int64(size)
	}

	// /usr/share/doc: ~500 small text files, 1KB-8KB each (~2MB total)
	for i := 0; i < 500; i++ {
		subdir := filepath.Join(root, "usr/share/doc", fmt.Sprintf("package-%03d", i))
		os.MkdirAll(subdir, 0755)
		size := 1024 + rng.Intn(7*1024)
		writeRandomFile(b, subdir, "README", size, rng)
		totalBytes += int64(size)
	}

	// /var/lib/dpkg/info: ~400 small package metadata files (~2MB total)
	for i := 0; i < 400; i++ {
		size := 256 + rng.Intn(4096)
		writeRandomFile(b, filepath.Join(root, "var/lib/dpkg/info"), fmt.Sprintf("pkg%03d.list", i), size, rng)
		totalBytes += int64(size)
	}

	// /opt/app/data: a few large data files to reach ~1GB total
	remaining := int64(1024*1024*1024) - totalBytes
	if remaining > 0 {
		// Split remaining into 2-4 large files
		nLarge := 2 + rng.Intn(3)
		for i := 0; i < nLarge; i++ {
			size := int(remaining) / nLarge
			writeRandomFile(b, filepath.Join(root, "opt/app/data"), fmt.Sprintf("data_%02d.bin", i), size, rng)
		}
	}

	// Symlinks (typical in container images)
	os.Symlink("/usr/bin/binary_000", filepath.Join(root, "usr/sbin/link_to_binary"))
	os.Symlink("/etc/ssl/certs", filepath.Join(root, "usr/share/ca-certificates"))

	// b.Logf("Created docker-like directory at %s (~1GB)", root)
	return root
}

// makeTmpfs creates a tmpfs mount of the given size and returns its path.
// The mount is automatically cleaned up when the test finishes.
func makeTmpfs(b *testing.B, size string) string {
	b.Helper()
	dir, err := os.MkdirTemp("", "ext4-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	cmd := exec.Command("mount", "-t", "tmpfs", "-o", "size="+size, "tmpfs", dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		os.Remove(dir)
		b.Fatalf("mount tmpfs: %s: %s", err, out)
	}
	b.Cleanup(func() {
		exec.Command("umount", dir).Run()
		os.Remove(dir)
	})
	return dir
}

func alignToMultiple(n int64, multiple int64) int64 {
	remainder := n % multiple

	// If remainder is zero, n is already aligned
	if remainder == 0 {
		return n
	}
	// Otherwise, adjust n to the next multiple of size
	return n + multiple - remainder
}

func BenchmarkDirectoryToImage(b *testing.B) {
	ctx := context.Background()

	// Use tmpfs for both input and output to eliminate disk I/O noise.
	tmpfs := makeTmpfs(b, "4G")
	inputDir := makeDockerImageDir(b, tmpfs)
	outputDir, err := os.MkdirTemp(tmpfs, "output-*")
	if err != nil {
		b.Fatal(err)
	}

	dirSize, err := ext4.DiskSizeBytes(ctx, inputDir)
	if err != nil {
		b.Fatal(err)
	}
	imageSize := int64(float64(dirSize)*1.2) + ext4.MinDiskImageSizeBytes
	imageSize = alignToMultiple(imageSize, int64(os.Getpagesize()))

	b.Run("baseline", func(b *testing.B) {
		b.SetBytes(imageSize)
		outputFile := filepath.Join(outputDir, "baseline.ext4")
		for b.Loop() {
			if err := ext4.DirectoryToImage(ctx, inputDir, outputFile, imageSize); err != nil {
				b.Fatal(err)
			}
			os.Remove(outputFile)
		}
	})
	b.Run("parallel_0", func(b *testing.B) {
		b.SetBytes(imageSize)
		outputFile := filepath.Join(outputDir, "exec0.ext4")
		for b.Loop() {
			if err := ext4.DirectoryToImage(ctx, inputDir, outputFile, imageSize, ext4.ImageOptions{CopyWorkers: -1}); err != nil {
				b.Fatal(err)
			}
			os.Remove(outputFile)
		}
	})
	b.Run("parallel_1", func(b *testing.B) {
		b.SetBytes(imageSize)
		outputFile := filepath.Join(outputDir, "exec1.ext4")
		for b.Loop() {
			if err := ext4.DirectoryToImage(ctx, inputDir, outputFile, imageSize, ext4.ImageOptions{CopyWorkers: 1}); err != nil {
				b.Fatal(err)
			}
			os.Remove(outputFile)
		}
	})
	b.Run("parallel_2", func(b *testing.B) {
		b.SetBytes(imageSize)
		outputFile := filepath.Join(outputDir, "exec2.ext4")
		for b.Loop() {
			if err := ext4.DirectoryToImage(ctx, inputDir, outputFile, imageSize, ext4.ImageOptions{CopyWorkers: 2}); err != nil {
				b.Fatal(err)
			}
			os.Remove(outputFile)
		}
	})
	b.Run("parallel_4", func(b *testing.B) {
		b.SetBytes(imageSize)
		outputFile := filepath.Join(outputDir, "exec4.ext4")
		for b.Loop() {
			if err := ext4.DirectoryToImage(ctx, inputDir, outputFile, imageSize, ext4.ImageOptions{CopyWorkers: 4}); err != nil {
				b.Fatal(err)
			}
			os.Remove(outputFile)
		}
	})
	b.Run("parallel_8", func(b *testing.B) {
		b.SetBytes(imageSize)
		outputFile := filepath.Join(outputDir, "exec8.ext4")
		for b.Loop() {
			if err := ext4.DirectoryToImage(ctx, inputDir, outputFile, imageSize, ext4.ImageOptions{CopyWorkers: 8}); err != nil {
				b.Fatal(err)
			}
			os.Remove(outputFile)
		}
	})
}

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
