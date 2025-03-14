package vfs_server_test

import (
	"context"
	"os"
	"runtime"
	"syscall"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/google/go-cmp/cmp"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

func testEnv(t *testing.T) (*testenv.TestEnv, context.Context) {
	env := testenv.GetTestEnv(t)
	ctx := context.Background()
	ctx, err := prefix.AttachUserPrefixToContext(ctx, env)
	require.NoError(t, err)
	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	require.NoError(t, err)
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()

	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	filecacheRootDir := testfs.MakeTempDir(t)
	fileCacheMaxSizeBytes := int64(10e9)
	fc, err := filecache.NewFileCache(filecacheRootDir, fileCacheMaxSizeBytes, false)
	require.NoError(t, err)
	fc.WaitForDirectoryScanToComplete()
	env.SetFileCache(fc)
	return env, ctx
}

func setFile(t *testing.T, env *testenv.TestEnv, ctx context.Context, instanceName, data string) *repb.Digest {
	dataBytes := []byte(data)
	hashString := hash.String(data)
	d := &repb.Digest{
		Hash:      hashString,
		SizeBytes: int64(len(dataBytes)),
	}
	r := &rspb.ResourceName{
		Digest:       d,
		CacheType:    rspb.CacheType_CAS,
		InstanceName: instanceName,
	}
	env.GetCache().Set(ctx, r, dataBytes)
	t.Logf("Added digest %s/%d to cache (content: %q)", d.GetHash(), d.GetSizeBytes(), data)
	return d
}

func requireSyscallError(t *testing.T, err error, errno syscall.Errno) {
	s, ok := gstatus.FromError(err)
	if !ok {
		assert.FailNow(t, "Expected an RPC status error", "error: %s", err)
	}
	for _, d := range s.Details() {
		if se, ok := d.(*vfspb.SyscallError); ok {
			require.Equal(t, errno, syscall.Errno(se.Errno))
			return
		}
	}
	assert.FailNow(t, "RPC status error did not contain a syscall error number", "RPC error: %s", err)
}

func newServer(t *testing.T) (*vfs_server.Server, string) {
	env := testenv.GetTestEnv(t)
	tmpDir := testfs.MakeTempDir(t)

	server := vfs_server.New(env, tmpDir)
	return server, tmpDir
}

func newServerWithEnv(t *testing.T) (context.Context, *testenv.TestEnv, *vfs_server.Server, string) {
	env, ctx := testEnv(t)
	tmpDir := testfs.MakeTempDir(t)

	server := vfs_server.New(env, tmpDir)
	return ctx, env, server, tmpDir
}

func writeToVFS(t *testing.T, server *vfs_server.Server, name string, content string) uint64 {
	ctx := context.Background()

	rsp, err := server.Lookup(ctx, &vfspb.LookupRequest{Name: name})
	var id, handleID uint64
	if err == nil {
		openRsp, err := server.Open(ctx, &vfspb.OpenRequest{
			Id:    rsp.GetId(),
			Flags: uint32(os.O_TRUNC | os.O_RDWR),
		})
		require.NoError(t, err)
		id = rsp.GetId()
		handleID = openRsp.GetHandleId()
	} else {
		createRsp, err := server.Create(ctx, &vfspb.CreateRequest{
			Name:  name,
			Mode:  0644,
			Flags: uint32(os.O_CREATE | os.O_RDWR),
		})
		require.NoError(t, err)
		id = createRsp.GetId()
		handleID = createRsp.GetHandleId()
	}
	defer func() {
		_, err := server.Release(ctx, &vfspb.ReleaseRequest{
			HandleId: handleID,
		})
		require.NoError(t, err, "release %s", name)
	}()
	_, err = server.Write(ctx, &vfspb.WriteRequest{
		HandleId: handleID,
		Data:     []byte(content),
	})
	require.NoError(t, err, "write %s", name)
	return id
}

func readFromVFS(t *testing.T, server *vfs_server.Server, name string) string {
	ctx := context.Background()
	rsp, err := server.Lookup(ctx, &vfspb.LookupRequest{Name: name})
	if err != nil {
		return ""
	}

	f, err := server.Open(ctx, &vfspb.OpenRequest{
		Id:    rsp.GetId(),
		Flags: uint32(os.O_RDONLY)},
	)
	require.NoError(t, err, "open %s", name)
	defer func() {
		_, err := server.Release(ctx, &vfspb.ReleaseRequest{
			HandleId: f.GetHandleId(),
		})
		require.NoError(t, err, "release %s", name)
	}()
	res, err := server.Read(ctx, &vfspb.ReadRequest{
		HandleId: f.GetHandleId(),
		NumBytes: 10_000,
	})
	require.NoError(t, err, "read %s", name)
	return string(res.Data)
}

func TestGetLayout(t *testing.T) {
	server, _ := newServer(t)
	ctx := context.Background()

	fileNode1 := &repb.FileNode{
		Name:   "afile.txt",
		Digest: &repb.Digest{Hash: digest.EmptySha256, SizeBytes: 123},
	}
	fileNode2 := &repb.FileNode{
		Name:         "anotherfile.txt",
		Digest:       &repb.Digest{Hash: digest.EmptySha256, SizeBytes: 456},
		IsExecutable: true,
	}
	symlinkNode := &repb.SymlinkNode{
		Name:   "asymlink",
		Target: "somefile.txt",
	}

	subDirFileNode1 := &repb.FileNode{
		Name:         "subfile.txt",
		Digest:       &repb.Digest{Hash: digest.EmptySha256, SizeBytes: 111},
		IsExecutable: true,
	}
	subDirFileNode2 := &repb.FileNode{
		Name:   "anothersubfile.txt",
		Digest: &repb.Digest{Hash: digest.EmptySha256, SizeBytes: 222},
	}

	subDir := &repb.Directory{
		Files: []*repb.FileNode{subDirFileNode1, subDirFileNode2},
	}
	subDirDigest, err := digest.ComputeForMessage(subDir, repb.DigestFunction_SHA256)
	require.NoError(t, err)
	subDirNode := &repb.DirectoryNode{
		Name:   "adirectory",
		Digest: subDirDigest,
	}

	inputTree := &repb.Tree{
		Root: &repb.Directory{
			Files:       []*repb.FileNode{fileNode1, fileNode2},
			Symlinks:    []*repb.SymlinkNode{symlinkNode},
			Directories: []*repb.DirectoryNode{subDirNode},
		},
		Children: []*repb.Directory{subDir},
	}

	err = server.Prepare(ctx, &container.FileSystemLayout{
		Inputs: inputTree,
	})
	require.NoError(t, err)

	rsp, err := server.GetDirectoryContents(ctx, &vfspb.GetDirectoryContentsRequest{})
	require.NoError(t, err)

	expectedRsp := &vfspb.GetDirectoryContentsResponse{
		Nodes: []*vfspb.Node{
			{Name: "adirectory", Attrs: &vfspb.Attrs{Size: 1000, Perm: 0755}, Mode: syscall.S_IFDIR},
			{Name: "afile.txt", Attrs: &vfspb.Attrs{Size: 123, Perm: 0644, Immutable: true, Nlink: 1}, Mode: syscall.S_IFREG},
			{Name: "anotherfile.txt", Attrs: &vfspb.Attrs{Size: 456, Perm: 0755, Immutable: true, Nlink: 1}, Mode: syscall.S_IFREG},
			{Name: "asymlink", Attrs: &vfspb.Attrs{Size: 1000, Perm: 0644}, Mode: syscall.S_IFLNK},
		},
	}
	require.Empty(t, cmp.Diff(expectedRsp, rsp, protocmp.Transform(),
		protocmp.IgnoreFields(&vfspb.Node{}, "id"), protocmp.IgnoreFields(&vfspb.Attrs{}, "atime_nanos", "mtime_nanos")))

	subdirLookupRsp, err := server.Lookup(ctx, &vfspb.LookupRequest{Name: "adirectory"})
	require.NoError(t, err)

	rsp, err = server.GetDirectoryContents(ctx, &vfspb.GetDirectoryContentsRequest{Id: subdirLookupRsp.GetId()})
	require.NoError(t, err)

	expectedRsp = &vfspb.GetDirectoryContentsResponse{
		Nodes: []*vfspb.Node{
			{Name: "anothersubfile.txt", Attrs: &vfspb.Attrs{Size: 222, Perm: 0644, Immutable: true, Nlink: 1}, Mode: syscall.S_IFREG},
			{Name: "subfile.txt", Attrs: &vfspb.Attrs{Size: 111, Perm: 0755, Immutable: true, Nlink: 1}, Mode: syscall.S_IFREG},
		},
	}
	require.Empty(t, cmp.Diff(expectedRsp, rsp, protocmp.Transform(),
		protocmp.IgnoreFields(&vfspb.Node{}, "id"), protocmp.IgnoreFields(&vfspb.Attrs{}, "atime_nanos", "mtime_nanos")))
}

func TestLookupNonExistentFile(t *testing.T) {
	server, _ := newServer(t)

	ctx := context.Background()
	_, err := server.Lookup(ctx, &vfspb.LookupRequest{Name: "file"})
	requireSyscallError(t, err, syscall.ENOENT)
}

func TestFileHandles(t *testing.T) {
	server, _ := newServer(t)
	ctx := context.Background()

	err := server.Prepare(ctx, &container.FileSystemLayout{Inputs: &repb.Tree{}})
	require.NoError(t, err)

	testFile := "test.file"
	createRsp, err := server.Create(ctx, &vfspb.CreateRequest{
		Name:  testFile,
		Flags: uint32(os.O_CREATE | os.O_RDWR),
		Mode:  0644,
	})
	require.NoError(t, err)
	handleID := createRsp.HandleId

	// File should have size 0 and should have the right perms.
	getAttrRsp, err := server.GetAttr(ctx, &vfspb.GetAttrRequest{Id: createRsp.GetId()})
	require.NoError(t, err)
	require.EqualValues(t, 0, getAttrRsp.GetAttrs().Size)
	require.EqualValues(t, 0644, getAttrRsp.GetAttrs().GetPerm())

	_, err = server.Allocate(ctx, &vfspb.AllocateRequest{HandleId: handleID, NumBytes: 3000})
	if runtime.GOOS == "linux" {
		require.NoError(t, err)

		getAttrRsp, err = server.GetAttr(ctx, &vfspb.GetAttrRequest{Id: createRsp.GetId()})
		require.NoError(t, err)
		require.EqualValues(t, 3000, getAttrRsp.GetAttrs().Size)

		// Overlapping request. Total size should grow by 1000.
		_, err = server.Allocate(ctx, &vfspb.AllocateRequest{HandleId: handleID, Offset: 2000, NumBytes: 2000})
		require.NoError(t, err)

		getAttrRsp, err = server.GetAttr(ctx, &vfspb.GetAttrRequest{Id: createRsp.GetId()})
		require.NoError(t, err)
		require.EqualValues(t, 4000, getAttrRsp.GetAttrs().Size)
	} else {
		requireSyscallError(t, err, syscall.ENOSYS)

		data := make([]byte, 4000)
		writeRsp, err := server.Write(ctx, &vfspb.WriteRequest{HandleId: handleID, Data: data})
		require.NoError(t, err)
		require.EqualValues(t, 4000, writeRsp.GetNumBytes())
	}

	// This should truncate the file. Perms should not be affected.
	setAttrRsp, err := server.SetAttr(ctx, &vfspb.SetAttrRequest{
		Id:      createRsp.GetId(),
		SetSize: &vfspb.SetAttrRequest_SetSize{Size: 2000}})
	require.NoError(t, err)
	require.EqualValues(t, 2000, setAttrRsp.GetAttrs().Size)
	require.EqualValues(t, 0644, setAttrRsp.GetAttrs().Perm)

	// Change perms.
	setAttrRsp, err = server.SetAttr(ctx, &vfspb.SetAttrRequest{
		Id:       createRsp.GetId(),
		SetPerms: &vfspb.SetAttrRequest_SetPerms{Perms: 0444}})
	require.NoError(t, err)
	require.EqualValues(t, 2000, setAttrRsp.GetAttrs().Size)
	require.EqualValues(t, 0444, setAttrRsp.GetAttrs().Perm)

	// Test basic read/write ops.
	writeRsp, err := server.Write(ctx, &vfspb.WriteRequest{HandleId: handleID, Offset: 30, Data: []byte("hello")})
	require.NoError(t, err)
	require.EqualValues(t, 5, writeRsp.GetNumBytes())

	// Read "ell" from the previous written data.
	readRsp, err := server.Read(ctx, &vfspb.ReadRequest{HandleId: handleID, Offset: 31, NumBytes: 3})
	require.NoError(t, err)
	require.Equal(t, []byte("ell"), readRsp.GetData())
}

func TestDirOps(t *testing.T) {
	server, _ := newServer(t)
	ctx := context.Background()

	err := server.Prepare(ctx, &container.FileSystemLayout{Inputs: &repb.Tree{}})
	require.NoError(t, err)

	mkdirRsp, err := server.Mkdir(ctx, &vfspb.MkdirRequest{Name: "dir", Perms: 0700})
	require.NoError(t, err)

	getAttrRsp, err := server.GetAttr(ctx, &vfspb.GetAttrRequest{Id: mkdirRsp.GetId()})
	require.NoError(t, err)
	require.EqualValues(t, 0700, getAttrRsp.GetAttrs().GetPerm())

	// Creating subdir should succeed.
	_, err = server.Mkdir(ctx, &vfspb.MkdirRequest{ParentId: mkdirRsp.GetId(), Name: "subdir", Perms: 0700})
	require.NoError(t, err)

	// Deleting non-empty dir should fail.
	_, err = server.Rmdir(ctx, &vfspb.RmdirRequest{Name: "dir"})
	requireSyscallError(t, err, syscall.ENOTEMPTY)

	_, err = server.Rmdir(ctx, &vfspb.RmdirRequest{ParentId: mkdirRsp.GetId(), Name: "subdir"})
	require.NoError(t, err)
}

func TestFilenameOps(t *testing.T) {
	server, _ := newServer(t)
	ctx := context.Background()

	err := server.Prepare(ctx, &container.FileSystemLayout{Inputs: &repb.Tree{}})
	require.NoError(t, err)

	testFile := "a.file"
	writeToVFS(t, server, testFile, "some data")

	newName := "b.file"
	_, err = server.Rename(ctx, &vfspb.RenameRequest{OldName: testFile, NewName: newName})
	require.NoError(t, err)

	// Old file shouldn't exist anymore.
	_, err = server.Lookup(ctx, &vfspb.LookupRequest{Name: testFile})
	requireSyscallError(t, err, syscall.ENOENT)

	lookupRsp, err := server.Lookup(ctx, &vfspb.LookupRequest{Name: newName})
	require.NoError(t, err)

	getAttrRsp, err := server.GetAttr(ctx, &vfspb.GetAttrRequest{Id: lookupRsp.GetId()})
	require.NoError(t, err)
	require.EqualValues(t, 0644, getAttrRsp.GetAttrs().GetPerm())

	_, err = server.Unlink(ctx, &vfspb.UnlinkRequest{Name: newName})
	require.NoError(t, err)

	// File shouldn't exist anymore.
	_, err = server.Lookup(ctx, &vfspb.LookupRequest{Name: newName})
	requireSyscallError(t, err, syscall.ENOENT)
}

func TestHardlink(t *testing.T) {
	server, _ := newServer(t)
	ctx := context.Background()

	err := server.Prepare(ctx, &container.FileSystemLayout{Inputs: &repb.Tree{}})
	require.NoError(t, err)

	fileID := writeToVFS(t, server, "src", "hello")
	_, err = server.Link(ctx, &vfspb.LinkRequest{Name: "dst", TargetId: fileID})
	require.NoError(t, err)

	content := readFromVFS(t, server, "dst")
	require.Equal(t, "hello", content, "hardlink content should match source file")

	// Update src with new contents
	writeToVFS(t, server, "src", "world")

	content = readFromVFS(t, server, "dst")
	require.Equal(t, "world", content, "hardlink content should match updated source file")

	// Unlink src; hardlink should still be readable
	_, err = server.Unlink(ctx, &vfspb.UnlinkRequest{Name: "src"})
	require.NoError(t, err)

	content = readFromVFS(t, server, "dst")
	require.Equal(t, "world", content, "hardlink should be readable after unlinking source file")
}

func TestFileLocking(t *testing.T) {
	server, _ := newServer(t)
	ctx := context.Background()

	err := server.Prepare(ctx, &container.FileSystemLayout{Inputs: &repb.Tree{}})
	require.NoError(t, err)

	var nodeID uint64
	// Init two different file handle IDs referring to the same inode.
	var id1, id2 uint64
	{
		req := &vfspb.CreateRequest{
			Name:  "lock",
			Flags: uint32(os.O_CREATE),
			Mode:  0644,
		}
		res, err := server.Create(ctx, req)
		require.NoError(t, err)
		id1 = res.HandleId
		nodeID = res.GetId()
	}
	{
		req := &vfspb.OpenRequest{
			Id:    nodeID,
			Flags: uint32(os.O_RDWR),
		}
		res, err := server.Open(ctx, req)
		require.NoError(t, err)
		id2 = res.HandleId
	}
	require.NotEqual(t, id1, id2, "sanity check: file handle IDs should be different")

	// Acquire exclusive flock (blocking) on fd 1, should succeed.
	{
		_, err := server.SetLkw(ctx, flock(id1, syscall.F_WRLCK))
		require.NoError(t, err)
	}
	// Acquire exclusive flock (blocking) on fd 1 again, should succeed.
	// Verify this behavior with the following shell code:
	// ( flock -x 100 && flock -x 100 && echo OK ) 100>/tmp/lock
	{
		_, err := server.SetLkw(ctx, flock(id1, syscall.F_WRLCK))
		require.NoError(t, err)
	}
	// Acquire exclusive lock with flock (non-blocking) on fd 2, should fail
	// since fd 1 is locked and points to the same inode.
	{
		_, err := server.SetLk(ctx, flock(id2, syscall.F_WRLCK))
		require.Error(t, err)
	}
	// Unlock fd 1, should succeed.
	{
		_, err := server.SetLk(ctx, flock(id1, syscall.F_UNLCK))
		require.NoError(t, err)
	}
	// Unlock fd 1 again, should succeed.
	// Verify this behavior with the following shell code:
	// ( flock -x 100 && flock -u 100 && flock -u 100 && echo OK ) 100>/tmp/lock
	{
		_, err := server.SetLk(ctx, flock(id1, syscall.F_UNLCK))
		require.NoError(t, err)
	}
	// Try locking fd 2 again, should succeed.
	{
		_, err := server.SetLk(ctx, flock(id2, syscall.F_WRLCK))
		require.NoError(t, err)
	}
}

// flock returns a flock(2) setlk request for the given file handle ID, pid, and
// lock state (typ), which can be one of the syscall package constants F_WRLCK,
// F_RDLCK, or F_UNLCK.
func flock(id uint64, typ uint32) *vfspb.SetLkRequest {
	return &vfspb.SetLkRequest{
		HandleId: id,
		Flags:    fuse.FUSE_LK_FLOCK,
		FileLock: &vfspb.FileLock{
			Typ: typ,
			// Note: start and end are not needed, since these are only
			// supported by the fcntl(2) API, but we are using flock(2). Also,
			// pid is not needed since it is redundant, as file handle IDs are
			// already isolated by pid.
		},
	}
}
