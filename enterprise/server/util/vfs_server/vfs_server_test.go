package vfs_server_test

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
	gstatus "google.golang.org/grpc/status"
)

type FakeLazyFileProvider struct {
	contents map[string]string
}

func (f *FakeLazyFileProvider) Place(relPath, fullPath string) error {
	data, ok := f.contents[relPath]
	if !ok {
		return status.NotFoundErrorf("file not in contents map %q", relPath)
	}
	if err := os.WriteFile(fullPath, []byte(data), 0644); err != nil {
		return err
	}
	return nil
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
	tmpDir, err := ioutil.TempDir("", "vfs_test")
	require.NoError(t, err)

	server := vfs_server.New(env, tmpDir)
	return server, tmpDir
}

func TestOpenNonExistentFile(t *testing.T) {
	server, _ := newServer(t)

	ctx := context.Background()
	_, err := server.Open(ctx, &vfspb.OpenRequest{Path: "some/file"})
	requireSyscallError(t, err, syscall.ENOENT)
}

func TestLazyLoadFile(t *testing.T) {
	server, _ := newServer(t)

	lazyFilePath := "some/file/path"
	lazyFileContents := "this is a file, or is it...?"
	p := &FakeLazyFileProvider{contents: map[string]string{
		lazyFilePath: lazyFileContents,
	}}
	err := server.Prepare([]string{lazyFilePath}, p)
	require.NoError(t, err)

	ctx := context.Background()
	rsp, err := server.Open(ctx, &vfspb.OpenRequest{Path: lazyFilePath})
	require.NoError(t, err)

	readRsp, err := server.Read(ctx, &vfspb.ReadRequest{HandleId: rsp.HandleId, Offset: 0, NumBytes: 1000})
	require.NoError(t, err)
	require.Equal(t, lazyFileContents, string(readRsp.GetData()))
}

func TestFileHandles(t *testing.T) {
	server, _ := newServer(t)
	ctx := context.Background()

	testFile := "test.file"
	openRsp, err := server.Open(ctx, &vfspb.OpenRequest{
		Path:  testFile,
		Flags: uint32(os.O_CREATE | os.O_RDWR),
		Mode:  0644,
	})
	require.NoError(t, err)
	handleID := openRsp.HandleId

	// File should have size 0 and should have the right perms.
	getAttrRsp, err := server.GetAttr(ctx, &vfspb.GetAttrRequest{Path: testFile})
	require.NoError(t, err)
	require.EqualValues(t, 0, getAttrRsp.GetAttrs().Size)
	require.EqualValues(t, 0644, getAttrRsp.GetAttrs().GetPerm())

	_, err = server.Allocate(ctx, &vfspb.AllocateRequest{HandleId: handleID, NumBytes: 3000})
	if runtime.GOOS == "linux" {
		require.NoError(t, err)

		getAttrRsp, err = server.GetAttr(ctx, &vfspb.GetAttrRequest{Path: testFile})
		require.NoError(t, err)
		require.EqualValues(t, 3000, getAttrRsp.GetAttrs().Size)

		// Overlapping request. Total size should grow by 1000.
		_, err = server.Allocate(ctx, &vfspb.AllocateRequest{HandleId: handleID, Offset: 2000, NumBytes: 2000})
		require.NoError(t, err)

		getAttrRsp, err = server.GetAttr(ctx, &vfspb.GetAttrRequest{Path: testFile})
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
		Path:    testFile,
		SetSize: &vfspb.SetAttrRequest_SetSize{Size: 2000}})
	require.NoError(t, err)
	require.EqualValues(t, 2000, setAttrRsp.GetAttrs().Size)
	require.EqualValues(t, 0644, setAttrRsp.GetAttrs().Perm)

	// Change perms.
	setAttrRsp, err = server.SetAttr(ctx, &vfspb.SetAttrRequest{
		Path:     testFile,
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

	// Creation should fail when parent dir doesn't exist.
	_, err := server.Mkdir(ctx, &vfspb.MkdirRequest{Path: "dir/subdir", Perms: 0777})
	requireSyscallError(t, err, syscall.ENOENT)

	_, err = server.Mkdir(ctx, &vfspb.MkdirRequest{Path: "dir", Perms: 0700})
	require.NoError(t, err)

	getAttrRsp, err := server.GetAttr(ctx, &vfspb.GetAttrRequest{Path: "dir"})
	require.NoError(t, err)
	require.EqualValues(t, 0700, getAttrRsp.GetAttrs().GetPerm())

	// Creating subdir should succeed.
	_, err = server.Mkdir(ctx, &vfspb.MkdirRequest{Path: "dir/subdir", Perms: 0700})
	require.NoError(t, err)

	// Deleting non-empty dir should fail.
	_, err = server.Rmdir(ctx, &vfspb.RmdirRequest{Path: "dir"})
	requireSyscallError(t, err, syscall.ENOTEMPTY)

	_, err = server.Rmdir(ctx, &vfspb.RmdirRequest{Path: "dir/subdir"})
	require.NoError(t, err)
}

func TestFilenameOps(t *testing.T) {
	server, tmpDir := newServer(t)
	ctx := context.Background()

	testFile := "a.file"
	err := os.WriteFile(filepath.Join(tmpDir, testFile), []byte("some data"), 0600)
	require.NoError(t, err)

	newName := "b.file"
	_, err = server.Rename(ctx, &vfspb.RenameRequest{OldPath: testFile, NewPath: newName})
	require.NoError(t, err)

	// Old file shouldn't exist anymore.
	_, err = server.GetAttr(ctx, &vfspb.GetAttrRequest{Path: testFile})
	requireSyscallError(t, err, syscall.ENOENT)

	getAttrRsp, err := server.GetAttr(ctx, &vfspb.GetAttrRequest{Path: newName})
	require.NoError(t, err)
	require.EqualValues(t, 0600, getAttrRsp.GetAttrs().GetPerm())

	_, err = server.Unlink(ctx, &vfspb.UnlinkRequest{Path: newName})
	require.NoError(t, err)

	// File shouldn't exist anymore.
	_, err = server.GetAttr(ctx, &vfspb.GetAttrRequest{Path: newName})
	requireSyscallError(t, err, syscall.ENOENT)
}

func TestSymlinkOps(t *testing.T) {
	server, tmpDir := newServer(t)
	ctx := context.Background()

	// Basic link with an absolute target path.
	{
		symlink := "alink.abs"
		symlinkTarget := "/some/file/path"
		_, err := server.Symlink(ctx, &vfspb.SymlinkRequest{Path: symlink, Target: symlinkTarget})
		require.NoError(t, err)

		// Symlink target should be rewritten to be under the tmp dir.
		hostTarget, err := os.Readlink(filepath.Join(tmpDir, symlink))
		require.NoError(t, err)
		require.Equal(t, filepath.Join(tmpDir, symlinkTarget), hostTarget)
	}

	// Basic link with a relative target path.
	{
		symlink := "alink.rel"
		symlinkTarget := "some/file/path"
		_, err := server.Symlink(ctx, &vfspb.SymlinkRequest{Path: symlink, Target: symlinkTarget})
		require.NoError(t, err)

		// Symlink target should be written as is.
		hostTarget, err := os.Readlink(filepath.Join(tmpDir, symlink))
		require.NoError(t, err)
		require.Equal(t, symlinkTarget, hostTarget)
	}

	// Absolute link that points outside workspace should be prohibited.
	{
		symlink := "alink.abs"
		symlinkTarget := "/path/../../top.secret"
		_, err := server.Symlink(ctx, &vfspb.SymlinkRequest{Path: symlink, Target: symlinkTarget})
		require.Error(t, err)
		require.True(t, status.IsPermissionDeniedError(err), "wanted PermissionDenied got %s", err)
	}

	// Relative link that points outside workspace should be prohibited.
	{
		symlink := "alink.rel"
		symlinkTarget := "../top.secret"
		_, err := server.Symlink(ctx, &vfspb.SymlinkRequest{Path: symlink, Target: symlinkTarget})
		require.Error(t, err)
		require.True(t, status.IsPermissionDeniedError(err), "wanted PermissionDenied got %s", err)
	}

	// Moving around a symlink shouldn't allow it to point to outside the workspace.
	{
		_, err := server.Mkdir(ctx, &vfspb.MkdirRequest{Path: "dir", Perms: 0700})
		require.NoError(t, err)

		// Create the symlink should succeed since it's still under the workspace.
		symlink := "dir/alink.rel"
		symlinkTarget := "../top.secret"
		_, err = server.Symlink(ctx, &vfspb.SymlinkRequest{Path: symlink, Target: symlinkTarget})
		require.NoError(t, err)

		// Moving the symlink one level up would make it reference something outside the workspace.
		_, err = server.Rename(ctx, &vfspb.RenameRequest{OldPath: "dir/alink.rel", NewPath: "alink.rel"})
		require.Error(t, err)
		require.True(t, status.IsPermissionDeniedError(err), "wanted PermissionDenied got %s", err)

		// Moving within the same directory should be okay though.
		_, err = server.Rename(ctx, &vfspb.RenameRequest{OldPath: "dir/alink.rel", NewPath: "dir/blink.rel"})
		require.NoError(t, err)
	}
}
