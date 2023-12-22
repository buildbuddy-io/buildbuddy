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
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

const maxFilecacheSizeBytes = 20_000_000

func init() {
	// Ensure that we allocate enough memory for the mmap LRU.
	if err := resources.Configure(true /*=snapshotSharingEnabled*/); err != nil {
		log.Fatalf("Failed to configure resources: %s", err)
	}
}

func setupEnv(t *testing.T) *testenv.TestEnv {
	env := testenv.GetTestEnv(t)
	filecacheDir := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(filecacheDir, maxFilecacheSizeBytes, false)
	require.NoError(t, err)
	fc.WaitForDirectoryScanToComplete()
	env.SetFileCache(fc)
	_, run := testenv.RegisterLocalGRPCServer(env)
	testcache.Setup(t, env)
	go run()
	return env
}

func TestPackAndUnpackChunkedFiles(t *testing.T) {
	for _, enableRemote := range []bool{true, false} {
		flags.Set(t, "executor.enable_remote_snapshot_sharing", enableRemote)

		ctx := context.Background()
		env := setupEnv(t)
		loader, err := snaploader.New(env)
		require.NoError(t, err)
		workDir := testfs.MakeTempDir(t)

		// Create an initial chunked file for VM A.
		workDirA := testfs.MakeDirAll(t, workDir, "VM-A")
		const chunkSize = 512 * 1024
		const fileSize = 13 + (chunkSize * 10) // ~5 MB total, with uneven size
		originalImagePath := makeRandomFile(t, workDirA, "scratchfs.ext4", fileSize)
		cowA, err := copy_on_write.ConvertFileToCOW(ctx, env, originalImagePath, chunkSize, workDirA, "", enableRemote)
		require.NoError(t, err)

		// Overwrite a random range to simulate the disk being written to. This
		// should create some dirty chunks.
		writeRandomRange(t, cowA)

		// Now store a snapshot for VM A, including the COW we created.
		task := &repb.ExecutionTask{}
		keys, err := snaploader.SnapshotKeySet(task, "config-hash", "")
		require.NoError(t, err)
		optsA := makeFakeSnapshot(t, workDirA, enableRemote)
		optsA.ChunkedFiles = map[string]*copy_on_write.COWStore{
			"scratchfs": cowA,
		}
		err = loader.CacheSnapshot(ctx, keys.GetBranchKey(), optsA)
		require.NoError(t, err)
		// Note: we'd normally close cowA here, but we keep it open so that
		// mustUnpack() can verify the original contents.

		// We want to make sure we can save/unpack snapshots that were also
		// unpacked from a snapshot.
		// i.e. For SnapA -> ForkA -> ForkA' we want to make sure ForkA' functions
		// correctly
		originalOpts := optsA
		for i := 0; i < 3; i++ {
			forkWorkDir := testfs.MakeDirAll(t, workDir, fmt.Sprintf("VM-%d", i))
			unpacked := mustUnpack(t, ctx, loader, keys, forkWorkDir, originalOpts)
			forkCOW := unpacked.ChunkedFiles["scratchfs"]
			writeRandomRange(t, forkCOW)
			forkOpts := makeFakeSnapshot(t, forkWorkDir, enableRemote)
			forkOpts.ChunkedFiles = map[string]*copy_on_write.COWStore{
				"scratchfs": forkCOW,
			}
			err := loader.CacheSnapshot(ctx, keys.GetBranchKey(), forkOpts)
			require.NoError(t, err)
			originalOpts = forkOpts
		}
	}
}

func TestUnpackFallbackKey(t *testing.T) {
	flags.Set(t, "executor.enable_remote_snapshot_sharing", true)

	ctx := context.Background()
	env := setupEnv(t)
	loader, err := snaploader.New(env)
	require.NoError(t, err)
	workDir := testfs.MakeTempDir(t)

	task := &repb.ExecutionTask{
		Command: &repb.Command{
			EnvironmentVariables: []*repb.Command_EnvironmentVariable{
				// Set env vars matching a push to the main branch.
				{Name: "GIT_BRANCH", Value: "main"},
				{Name: "GIT_BASE_BRANCH", Value: ""},
				{Name: "GIT_REPO_DEFAULT_BRANCH", Value: "main"},
			},
		},
	}
	keys, err := snaploader.SnapshotKeySet(task, "config-hash", "")
	require.NoError(t, err)
	require.Equal(t, "main", keys.GetBranchKey().GetRef())
	require.Empty(t, keys.GetFallbackKeys())

	opts := makeFakeSnapshot(t, workDir, true /*=enableRemote*/)
	err = loader.CacheSnapshot(ctx, keys.GetBranchKey(), opts)
	require.NoError(t, err)

	forkTask := &repb.ExecutionTask{
		Command: &repb.Command{
			EnvironmentVariables: []*repb.Command_EnvironmentVariable{
				// Set env vars matching a stacked PR, with the main branch as
				// the last fallback.
				{Name: "GIT_BRANCH", Value: "my-cool-pr"},
				{Name: "GIT_BASE_BRANCH", Value: "my-cool-stacked-pr-base-branch"},
				{Name: "GIT_REPO_DEFAULT_BRANCH", Value: "main"},
			},
		},
	}
	forkWorkDir := testfs.MakeDirAll(t, workDir, "VM-fallback")
	forkKeys, err := snaploader.SnapshotKeySet(forkTask, "config-hash", "")
	require.NoError(t, err)
	require.Equal(t, "my-cool-pr", forkKeys.GetBranchKey().GetRef())
	require.Len(t, forkKeys.GetFallbackKeys(), 2)
	require.Equal(t, "my-cool-stacked-pr-base-branch", forkKeys.FallbackKeys[0].Ref)
	require.Equal(t, "main", forkKeys.FallbackKeys[1].Ref)

	// Sanity check that the branch key is not found in cache, to make sure
	// we're actually falling back.
	_, err = loader.GetSnapshot(ctx, &fcpb.SnapshotKeySet{BranchKey: forkKeys.GetBranchKey()}, true /*=enableRemote*/)
	require.Error(t, err)

	// Now try unpacking with the complete PR key set, including fallbacks. This
	// should succeed.
	mustUnpack(t, ctx, loader, forkKeys, forkWorkDir, opts)
}

func TestPackAndUnpackChunkedFiles_Immutability(t *testing.T) {
	for _, enableRemote := range []bool{true, false} {
		flags.Set(t, "executor.enable_remote_snapshot_sharing", enableRemote)

		ctx := context.Background()
		env := setupEnv(t)
		loader, err := snaploader.New(env)
		require.NoError(t, err)
		workDir := testfs.MakeTempDir(t)

		// Create an initial chunked file for VM A.
		workDirA := testfs.MakeDirAll(t, workDir, "VM-A")
		const chunkSize = 512 * 1024
		const fileSize = 13 + (chunkSize * 10) // ~5 MB total, with uneven size
		originalImagePath := makeRandomFile(t, workDirA, "scratchfs.ext4", fileSize)
		chunkDirA := testfs.MakeDirAll(t, workDirA, "scratchfs_chunks")
		cowA, err := copy_on_write.ConvertFileToCOW(ctx, env, originalImagePath, chunkSize, chunkDirA, "", enableRemote)
		require.NoError(t, err)
		// Overwrite a random range to simulate the disk being written to. This
		// should create some dirty chunks.
		writeRandomRange(t, cowA)
		// Now store a snapshot for VM A, including the COW we created.
		taskA := &repb.ExecutionTask{}
		keysA, err := snaploader.SnapshotKeySet(taskA, "config-hash-a", "")
		require.NoError(t, err)
		optsA := makeFakeSnapshot(t, workDirA, enableRemote)
		optsA.ChunkedFiles = map[string]*copy_on_write.COWStore{
			"scratchfs": cowA,
		}
		err = loader.CacheSnapshot(ctx, keysA.GetBranchKey(), optsA)
		require.NoError(t, err)
		// Note: we'd normally close cowA here, but we keep it open so that
		// mustUnpack() can verify the original contents.

		// Read the bytes from cowA now to avoid relying on cowA being immutable
		// (though it should be).
		scratchfsBytesA := mustReadStore(t, cowA)

		// Now unpack the snapshot for use by VM B, then make a modification.
		workDirB := testfs.MakeDirAll(t, workDir, "VM-B")
		unpackedB := mustUnpack(t, ctx, loader, keysA, workDirB, optsA)
		cowB := unpackedB.ChunkedFiles["scratchfs"]
		writeRandomRange(t, cowB)

		// Unpack the snapshot again for use by VM C. It should see the original
		// contents, not the modification made by VM B.
		workDirC := testfs.MakeDirAll(t, workDir, "VM-C")
		unpackedC := mustUnpack(t, ctx, loader, keysA, workDirC, optsA)
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
}

func TestRemoteSnapshotFetching(t *testing.T) {
	flags.Set(t, "executor.enable_remote_snapshot_sharing", true)

	ctx := context.Background()
	env := setupEnv(t)
	fc := env.GetFileCache()
	loader, err := snaploader.New(env)
	require.NoError(t, err)
	workDir := testfs.MakeTempDir(t)

	// Save a snapshot - should cache all artifacts in both the local and remote
	// cache
	workDirA := testfs.MakeDirAll(t, workDir, "VM-A")
	const chunkSize = 512 * 1024
	const fileSize = 13 + (chunkSize * 10) // ~5 MB total, with uneven size
	originalImagePath := makeRandomFile(t, workDirA, "scratchfs.ext4", fileSize)
	cowA, err := copy_on_write.ConvertFileToCOW(ctx, env, originalImagePath, chunkSize, workDirA, "", true)
	require.NoError(t, err)
	writeRandomRange(t, cowA)
	task := &repb.ExecutionTask{}
	keys, err := snaploader.SnapshotKeySet(task, "config-hash", "")
	require.NoError(t, err)
	optsA := makeFakeSnapshot(t, workDirA, true)
	optsA.ChunkedFiles = map[string]*copy_on_write.COWStore{
		"scratchfs": cowA,
	}
	err = loader.CacheSnapshot(ctx, keys.GetBranchKey(), optsA)
	require.NoError(t, err)

	// Delete some artifacts from the local cache, so we can test fetching artifacts
	// from both the local and remote cache
	snapMetadata, err := loader.GetSnapshot(ctx, keys, true)
	require.NoError(t, err)
	for i, f := range snapMetadata.GetFiles() {
		if i%2 == 0 {
			deleted := fc.DeleteFile(ctx, f)
			require.True(t, deleted)
		}
	}
	for _, f := range snapMetadata.GetChunkedFiles() {
		for i, c := range f.GetChunks() {
			if i%2 == 1 {
				deleted := fc.DeleteFile(ctx, &repb.FileNode{Digest: c.Digest})
				require.True(t, deleted)
			}
		}
	}

	// Test unpacking snapshot
	originalOpts := optsA
	for i := 0; i < 3; i++ {
		forkWorkDir := testfs.MakeDirAll(t, workDir, fmt.Sprintf("VM-%d", i))
		unpacked := mustUnpack(t, ctx, loader, keys, forkWorkDir, originalOpts)
		forkCOW := unpacked.ChunkedFiles["scratchfs"]
		writeRandomRange(t, forkCOW)
		forkOpts := makeFakeSnapshot(t, forkWorkDir, true)
		forkOpts.ChunkedFiles = map[string]*copy_on_write.COWStore{
			"scratchfs": forkCOW,
		}
		err := loader.CacheSnapshot(ctx, keys.GetBranchKey(), forkOpts)
		require.NoError(t, err)
		originalOpts = forkOpts
	}
}

func TestRemoteSnapshotFetching_RemoteEviction(t *testing.T) {
	flags.Set(t, "executor.enable_remote_snapshot_sharing", true)

	env := setupEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env)
	require.NoError(t, err)
	loader, err := snaploader.New(env)
	require.NoError(t, err)
	workDir := testfs.MakeTempDir(t)

	// Save a snapshot - should cache all artifacts in both the local and remote
	// cache
	workDirA := testfs.MakeDirAll(t, workDir, "VM-A")
	const chunkSize = 512 * 1024
	const fileSize = 13 + (chunkSize * 10) // ~5 MB total, with uneven size
	originalImagePath := makeRandomFile(t, workDirA, "scratchfs.ext4", fileSize)
	cowA, err := copy_on_write.ConvertFileToCOW(ctx, env, originalImagePath, chunkSize, workDirA, "", true)
	require.NoError(t, err)
	writeRandomRange(t, cowA)
	task := &repb.ExecutionTask{}
	keys, err := snaploader.SnapshotKeySet(task, "config-hash", "")
	require.NoError(t, err)
	optsA := makeFakeSnapshot(t, workDirA, true)
	optsA.ChunkedFiles = map[string]*copy_on_write.COWStore{
		"scratchfs": cowA,
	}
	err = loader.CacheSnapshot(ctx, keys.GetBranchKey(), optsA)
	require.NoError(t, err)

	// Delete some artifacts from the remote cache (arbitrarily the first one for simplicity)
	snapMetadata, err := loader.GetSnapshot(ctx, keys, true)
	require.NoError(t, err)
	for _, f := range snapMetadata.GetFiles() {
		rn := digest.NewResourceName(f.GetDigest(), "", rspb.CacheType_CAS, repb.DigestFunction_BLAKE3).ToProto()
		err = env.GetCache().Delete(ctx, rn)
		require.NoError(t, err)
		break
	}
	for _, f := range snapMetadata.GetChunkedFiles() {
		for _, c := range f.GetChunks() {
			rn := digest.NewResourceName(c.GetDigest(), "", rspb.CacheType_CAS, repb.DigestFunction_BLAKE3).ToProto()
			err = env.GetCache().Delete(ctx, rn)
			require.NoError(t, err)
			break
		}
	}

	// Even though the assets still exist in the local cache, the remote cache
	// should serve as the source of truth on whether a snapshot is valid
	_, err = loader.GetSnapshot(ctx, keys, true)
	require.Error(t, err)
}

func TestGetSnapshot_CacheIsolation(t *testing.T) {
	for _, enableRemote := range []bool{true, false} {
		flags.Set(t, "executor.enable_remote_snapshot_sharing", enableRemote)

		env := setupEnv(t)
		auth := testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1", "US2", "GR2"))
		env.SetAuthenticator(auth)
		ctx, err := auth.WithAuthenticatedUser(context.Background(), "US1")
		require.NoError(t, err)
		loader, err := snaploader.New(env)
		require.NoError(t, err)
		workDir := testfs.MakeTempDir(t)

		// Save a snapshot with groupID and instance name
		workDirA := testfs.MakeDirAll(t, workDir, "VM-A")
		const chunkSize = 512 * 1024
		const fileSize = 13 + (chunkSize * 10) // ~5 MB total, with uneven size
		originalImagePath := makeRandomFile(t, workDirA, "scratchfs.ext4", fileSize)
		cowA, err := copy_on_write.ConvertFileToCOW(ctx, env, originalImagePath, chunkSize, workDirA, "remote-A", enableRemote)
		require.NoError(t, err)
		writeRandomRange(t, cowA)
		optsA := makeFakeSnapshot(t, workDirA, enableRemote)
		optsA.ChunkedFiles = map[string]*copy_on_write.COWStore{
			"scratchfs": cowA,
		}
		originalKeys := keysWithInstanceName(t, "remote-A")
		err = loader.CacheSnapshot(ctx, originalKeys.GetBranchKey(), optsA)
		require.NoError(t, err)

		// Fetching snapshot with same group id and instance name should succeed
		workDirB := testfs.MakeDirAll(t, workDir, "VM-B")
		_ = mustUnpack(t, ctx, loader, originalKeys, workDirB, optsA)

		// Fetching snapshot with same group id different instance name should fail
		keysNewInstanceName := keysWithInstanceName(t, "remote-C")
		_, err = loader.GetSnapshot(ctx, keysNewInstanceName, enableRemote)
		require.Error(t, err)

		// Fetching snapshot with different group id same instance name should fail
		ctxNewGroup, err := auth.WithAuthenticatedUser(context.Background(), "US2")
		require.NoError(t, err)
		_, err = loader.GetSnapshot(ctxNewGroup, originalKeys, enableRemote)
		require.Error(t, err)
	}
}

func keysWithInstanceName(t *testing.T, instanceName string) *fcpb.SnapshotKeySet {
	task := &repb.ExecutionTask{
		ExecuteRequest: &repb.ExecuteRequest{
			InstanceName: instanceName,
		},
	}
	key, err := snaploader.SnapshotKeySet(task, "config-hash", "")
	require.NoError(t, err)
	return key
}

func makeFakeSnapshot(t *testing.T, workDir string, remoteEnabled bool) *snaploader.CacheSnapshotOptions {
	return &snaploader.CacheSnapshotOptions{
		MemSnapshotPath:     makeRandomFile(t, workDir, "mem", 100_000),
		VMStateSnapshotPath: makeRandomFile(t, workDir, "vmstate", 1_000),
		KernelImagePath:     makeRandomFile(t, workDir, "kernel", 1_000),
		InitrdImagePath:     makeRandomFile(t, workDir, "initrd", 1_000),
		ContainerFSPath:     makeRandomFile(t, workDir, "containerfs", 1_000),
		Remote:              remoteEnabled,
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
func mustUnpack(t *testing.T, ctx context.Context, loader snaploader.Loader, snapshotKeySet *fcpb.SnapshotKeySet, outDir string, originalSnapshot *snaploader.CacheSnapshotOptions) *snaploader.UnpackedSnapshot {
	snap, err := loader.GetSnapshot(ctx, snapshotKeySet, true /*enableRemote*/)
	require.NoError(t, err)
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
