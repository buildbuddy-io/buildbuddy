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
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)
		dest := filepath.Join(parentDir, d.GetHash())
		if err := os.WriteFile(dest, buf, 0644); err != nil {
			t.Fatal(err)
		}
		pathHashMap[dest] = d.GetHash()
	}

	// Make the random directory into an ext4 image.
	imageFile := testfs.MakeTempFile(t, "", "*.img")
	if err := ext4.DirectoryToImageAutoSize(ctx, rootDir, imageFile); err != nil {
		t.Fatal(err)
	}

	// Create a new (empty) directory and unpack the image file to the new directory.
	newRoot := testfs.MakeTempDir(t)
	if err := ext4.ImageToDirectory(ctx, imageFile, newRoot); err != nil {
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
		d, err := digest.Compute(f)
		if err != nil {
			t.Fatal(err)
		}
		if d.GetHash() != hash {
			t.Fatalf("File content mismatch: %q != %q, %q", d.GetHash(), hash, newPath)
		}
	}
}
