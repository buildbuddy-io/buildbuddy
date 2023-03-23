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
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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
	la, err := snaploader.New(env, workDir)
	require.NoError(t, err)
	da := snaploader.NewKey("vm-config-hash-A", "runner-A")
	sa := makeFakeSnapshot(t, workDir)
	err = la.CacheSnapshot(ctx, da, sa)
	require.NoError(t, err)

	lb, err := snaploader.New(env, workDir)
	require.NoError(t, err)
	db := snaploader.NewKey("vm-config-hash-B", "runner-B")
	sb := makeFakeSnapshot(t, workDir)
	err = lb.CacheSnapshot(ctx, db, sb)
	require.NoError(t, err)

	// We should be able to unpack snapshot A, delete it, and then replace it
	// with a new snapshot several times, without evicting snapshot B.
	for i := 0; i < 20; i++ {
		// Unpack (this should also evict from cache).
		outDir := testfs.MakeDirAll(t, workDir, fmt.Sprintf("unpack-a-%d", i))
		mustUnpack(t, ctx, env, da, workDir, outDir, sa)

		// Delete, since it's no longer needed.
		// Note: we construct a new loader here to ensure the current
		// snapshot manifest gets loaded.
		loader, err := snaploader.New(env, workDir)
		require.NoError(t, err)
		err = loader.DeleteSnapshot(ctx, da)
		require.NoError(t, err)

		// Re-add to cache with the same key, but with new contents.
		sa = makeFakeSnapshot(t, workDir)
		err = la.CacheSnapshot(ctx, da, sa)
		require.NoError(t, err)
	}

	// Snapshot B should not have been evicted.
	outDir := testfs.MakeDirAll(t, workDir, "unpack-b")
	mustUnpack(t, ctx, env, db, workDir, outDir, sb)
}

func makeFakeSnapshot(t *testing.T, workDir string) *snaploader.LoadSnapshotOptions {
	return &snaploader.LoadSnapshotOptions{
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
func mustUnpack(t *testing.T, ctx context.Context, env environment.Env, d *repb.Digest, workDir, outDir string, originalSnapshot *snaploader.LoadSnapshotOptions) {
	loader, err := snaploader.New(env, workDir)
	require.NoError(t, err)
	err = loader.UnpackSnapshot(ctx, d, outDir)
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
