package snaploader_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaploader"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestPackAndUnpack(t *testing.T) {
	const maxFilecacheSizeBytes = 1_000_000 // 1MB
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	filecacheDir := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(filecacheDir, maxFilecacheSizeBytes, false)
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
	taskA := &repb.ExecutionTask{}
	keyA, err := snaploader.NewKey(taskA, "vm-config-hash-A", "runner-A")
	require.NoError(t, err)
	optsA := makeFakeSnapshot(t, workDir)
	snapA, err := loader.CacheSnapshot(ctx, keyA, optsA)
	require.NoError(t, err)

	require.NoError(t, err)
	taskB := &repb.ExecutionTask{}
	keyB, err := snaploader.NewKey(taskB, "vm-config-hash-B", "runner-B")
	require.NoError(t, err)
	optsB := makeFakeSnapshot(t, workDir)
	snapB, err := loader.CacheSnapshot(ctx, keyB, optsB)
	require.NoError(t, err)

	// We should be able to unpack snapshot A, delete it, and then replace it
	// with a new snapshot several times, without evicting snapshot B.
	for i := 0; i < 20; i++ {
		// Unpack (this should also evict from cache).
		outDir := testfs.MakeDirAll(t, workDir, fmt.Sprintf("unpack-a-%d", i))
		mustUnpack(t, ctx, loader, snapA, outDir, optsA)

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
	mustUnpack(t, ctx, loader, snapB, outDir, optsB)
}

func TestPackAndUnpackChunkedFiles(t *testing.T) {
	const maxFilecacheSizeBytes = 20_000_000 // 20 MB
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	filecacheDir := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(filecacheDir, maxFilecacheSizeBytes, false)
	require.NoError(t, err)
	fc.WaitForDirectoryScanToComplete()
	env.SetFileCache(fc)
	loader, err := snaploader.New(env)
	require.NoError(t, err)
	workDir := testfs.MakeTempDir(t)

	// Create an initial chunked file for VM A.
	workDirA := testfs.MakeDirAll(t, workDir, "VM-A")
	const chunkSize = 512 * 1024
	const fileSize = 13 + (chunkSize * 10) // ~5 MB total, with uneven size
	originalImagePath := makeRandomFile(t, workDirA, "scratchfs.ext4", fileSize)
	chunkDirA := testfs.MakeDirAll(t, workDirA, "scratchfs_chunks")
	cowA, err := copy_on_write.ConvertFileToCOW(originalImagePath, chunkSize, chunkDirA)
	require.NoError(t, err)
	// Overwrite a random range to simulate the disk being written to. This
	// should create some dirty chunks.
	writeRandomRange(t, cowA)
	// Now store a snapshot for VM A, including the COW we created.
	taskA := &repb.ExecutionTask{}
	keyA, err := snaploader.NewKey(taskA, "config-hash-a", "")
	require.NoError(t, err)
	optsA := makeFakeSnapshot(t, workDirA)
	optsA.ChunkedFiles = map[string]*copy_on_write.COWStore{
		"scratchfs": cowA,
	}
	snapA, err := loader.CacheSnapshot(ctx, keyA, optsA)
	require.NoError(t, err)
	// Note: we'd normally close cowA here, but we keep it open so that
	// mustUnpack() can verify the original contents.

	// Read the bytes from cowA now to avoid relying on cowA being immutable
	// (though it should be).
	scratchfsBytesA := mustReadStore(t, cowA)

	// Now unpack the snapshot for use by VM B, then make a modification.
	workDirB := testfs.MakeDirAll(t, workDir, "VM-B")
	unpackedB := mustUnpack(t, ctx, loader, snapA, workDirB, optsA)
	cowB := unpackedB.ChunkedFiles["scratchfs"]
	writeRandomRange(t, cowB)

	// Unpack the snapshot again for use by VM C. It should see the original
	// contents, not the modification made by VM B.
	workDirC := testfs.MakeDirAll(t, workDir, "VM-C")
	unpackedC := mustUnpack(t, ctx, loader, snapA, workDirC, optsA)
	// mustUnpack already verifies the contents against cowA, but this will give
	// us false confidence if cowA was somehow mutated. So check again against
	// the originally snapshotted bytes, rather than the original COW instance.
	r, err := interfaces.StoreReader(unpackedC.ChunkedFiles["scratchfs"])
	require.NoError(t, err)
	scratchfsBytesC, err := io.ReadAll(r)
	require.NoError(t, err)
	if !bytes.Equal(scratchfsBytesA, scratchfsBytesC) {
		require.FailNow(t, "scratchfs bytes for VM C should match original contents from snapshot A")
	}
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

func writeRandomRange(t *testing.T, store *copy_on_write.COWStore) {
	s, err := store.SizeBytes()
	require.NoError(t, err)
	off := rand.Intn(int(s))
	length := rand.Intn(int(s) - off)
	str, err := random.RandomString(length)
	require.NoError(t, err)
	_, err = store.WriteAt([]byte(str), int64(off))
	require.NoError(t, err)
}

// Unpacks a snapshot to outDir and asserts that the contents match the
// originally cached contents.
func mustUnpack(t *testing.T, ctx context.Context, loader snaploader.Loader, snap *snaploader.Snapshot, outDir string, originalSnapshot *snaploader.CacheSnapshotOptions) *snaploader.UnpackedSnapshot {
	unpacked, err := loader.UnpackSnapshot(ctx, snap, outDir)
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
	for name, originalCOW := range originalSnapshot.ChunkedFiles {
		originalContent := mustReadStore(t, originalCOW)
		unpackedContent := mustReadStore(t, unpacked.ChunkedFiles[name])
		require.NoError(t, err)
		if !bytes.Equal(originalContent, unpackedContent) {
			require.FailNowf(t, "unpacked ChunkedFile does not match original snapshot", "file name: %s", name)
		}
	}
	return unpacked
}

func mustReadStore(t *testing.T, store *copy_on_write.COWStore) []byte {
	r, err := interfaces.StoreReader(store)
	require.NoError(t, err)
	b, err := io.ReadAll(r)
	require.NoError(t, err)
	return b
}
