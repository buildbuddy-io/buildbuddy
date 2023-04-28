package snaploader_test

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaploader"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"
)

func TestPackAndUnpack(t *testing.T) {
	const maxFilecacheSizeBytes = 1_000_000 // 1MB
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	filecacheDir := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(filecacheDir, maxFilecacheSizeBytes)
	require.NoError(t, err)
	fc.WaitForDirectoryScanToComplete()
	env.SetFileCache(fc)
	workDir := testfs.MakeTempDir(t)

	// Create two snapshots, A and B, each with artifacts totaling 100KB,
	// and add them to the cache. Note, the snapshot digests don't actually
	// correspond to any real content; they just need to be unique cache
	// keys.
	loader, err := snaploader.New(env)
	require.NoError(t, err)
	keyA := snaploader.NewKey("vm-config-hash-A", "runner-A")
	optsA := makeFakeSnapshot(t, workDir)
	snapA, err := loader.CacheSnapshot(ctx, keyA, optsA)
	require.NoError(t, err)

	require.NoError(t, err)
	keyB := snaploader.NewKey("vm-config-hash-B", "runner-B")
	optsB := makeFakeSnapshot(t, workDir)
	snapB, err := loader.CacheSnapshot(ctx, keyB, optsB)
	require.NoError(t, err)

	// We should be able to unpack snapshot A, delete it, and then replace it
	// with a new snapshot several times, without evicting snapshot B.
	for i := 0; i < 20; i++ {
		// Unpack (this should also evict from cache).
		outDir := testfs.MakeDirAll(t, workDir, fmt.Sprintf("unpack-a-%d", i))
		mustUnpack(t, ctx, loader, snapA, workDir, outDir, optsA)

		// Delete, since it's no longer needed.
		err = loader.DeleteSnapshot(ctx, snapA)
		require.NoError(t, err)

		// Re-add to cache with the same key, but with new contents.
		optsA = makeFakeSnapshot(t, workDir)
		snapA, err = loader.CacheSnapshot(ctx, keyA, optsA)
		require.NoError(t, err)
	}

	// Snapshot B should not have been evicted.
	outDir := testfs.MakeDirAll(t, workDir, "unpack-b")
	mustUnpack(t, ctx, loader, snapB, workDir, outDir, optsB)
}

func makeFakeSnapshot(t *testing.T, workDir string) *snaploader.CacheSnapshotOptions {
	return &snaploader.CacheSnapshotOptions{
		MemSnapshotPath:     makeRandomFile(t, workDir, "mem", 100_000),
		VMStateSnapshotPath: makeRandomFile(t, workDir, "vmstate", 1_000),
		KernelImagePath:     makeRandomFile(t, workDir, "kernel", 1_000),
		InitrdImagePath:     makeRandomFile(t, workDir, "initrd", 1_000),
		ContainerFSPath:     makeRandomFile(t, workDir, "containerfs", 1_000),
	}
}

func makeRandomFile(t *testing.T, rootDir, prefix string, size int) string {
	name := prefix + "-" + strconv.Itoa(rand.Int())
	testfs.WriteRandomString(t, rootDir, name, size)
	return filepath.Join(rootDir, name)
}

// Unpacks a snapshot to outDir and asserts that the contents match the
// originally cached contents.
func mustUnpack(t *testing.T, ctx context.Context, loader snaploader.Loader, snap *snaploader.Snapshot, workDir, outDir string, originalSnapshot *snaploader.CacheSnapshotOptions) {
	err := loader.UnpackSnapshot(ctx, snap, outDir)
	require.NoError(t, err)

	for _, path := range []string{
		originalSnapshot.MemSnapshotPath,
		originalSnapshot.VMStateSnapshotPath,
		originalSnapshot.KernelImagePath,
		originalSnapshot.InitrdImagePath,
		originalSnapshot.ContainerFSPath,
	} {
		originalContent := testfs.ReadFileAsString(t, filepath.Dir(path), filepath.Base(path))
		unpackedContent := testfs.ReadFileAsString(t, outDir, filepath.Base(path))
		if originalContent != unpackedContent {
			// Note: not using require.Equal since the diff would be useless due
			// to the content being random.
			require.FailNow(t, "unpacked snapshot content does not match original snapshot")
		}
	}
}
