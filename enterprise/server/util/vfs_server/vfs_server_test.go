package vfs_server_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfscommon"
	"github.com/buildbuddy-io/buildbuddy/server/cache/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-cmp/cmp"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
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
	ctx, err := prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
	require.NoError(t, err)
	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	require.NoError(t, err)
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()

	conn, err := testenv.LocalGRPCConn(t, ctx, lis)
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

func newServer(t *testing.T) (*vfs_server.Server, environment.Env) {
	env := testenv.GetTestEnv(t)
	tmpDir := testfs.MakeTempDir(t)

	server, err := vfs_server.New(env, tmpDir)
	require.NoError(t, err)
	return server, env
}

func newServerWithEnv(t *testing.T) (context.Context, *testenv.TestEnv, *vfs_server.Server, string) {
	env, ctx := testEnv(t)
	tmpDir := testfs.MakeTempDir(t)

	server, err := vfs_server.New(env, tmpDir)
	require.NoError(t, err)
	return ctx, env, server, tmpDir
}

func writeToVFS(t *testing.T, server *vfs_server.Server, name string, content string) uint64 {
	ctx := context.Background()

	rsp, err := server.Lookup(ctx, &vfspb.LookupRequest{ParentId: vfscommon.RootInodeId, Name: name})
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
			ParentId: vfscommon.RootInodeId,
			Name:     name,
			Mode:     0644,
			Flags:    uint32(os.O_CREATE | os.O_RDWR),
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
	rsp, err := server.Lookup(ctx, &vfspb.LookupRequest{ParentId: vfscommon.RootInodeId, Name: name})
	require.NoError(t, err)

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

func prepare(t *testing.T, env environment.Env, server *vfs_server.Server, tree *repb.Tree) {
	tf, err := dirtools.NewTreeFetcher(t.Context(), env, "", repb.DigestFunction_SHA256, tree, &dirtools.DownloadTreeOpts{})
	require.NoError(t, err)
	_, err = server.Prepare(t.Context(), &container.FileSystemLayout{Inputs: tree}, tf)
	require.NoError(t, err)
}

type rawTestCodec struct{}

func (rawTestCodec) Name() string { return "proto" }

func (rawTestCodec) Marshal(v any) ([]byte, error) {
	if data, ok := v.(*[]byte); ok {
		return *data, nil
	}
	return proto.Marshal(v.(proto.Message))
}

func (rawTestCodec) Unmarshal(data []byte, v any) error {
	if out, ok := v.(*[]byte); ok {
		*out = append((*out)[:0], data...)
		return nil
	}
	return proto.Unmarshal(data, v.(proto.Message))
}

type rawTreeServer struct {
	repb.UnimplementedContentAddressableStorageServer
	responses [][]byte
	err       error
	canceled  chan struct{}
}

func (s *rawTreeServer) GetTree(req *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	if values := md.Get("task-context"); len(values) != 1 || values[0] != "executor" {
		return gstatus.Error(codes.PermissionDenied, "missing executor task context")
	}
	for _, data := range s.responses {
		if err := stream.SendMsg(&data); err != nil {
			return err
		}
	}
	if s.canceled != nil {
		<-stream.Context().Done()
		close(s.canceled)
		return stream.Context().Err()
	}
	return s.err
}

func TestGetTreeProxyPreservesWireResponses(t *testing.T) {
	dir := &repb.Directory{}
	for i := range 20_000 {
		dir.Files = append(dir.Files, &repb.FileNode{
			Name: fmt.Sprintf("file-%05d", i), Digest: &repb.Digest{Hash: hash.String("contents"), SizeBytes: 8},
		})
	}
	directoryBytes, err := proto.Marshal(&repb.GetTreeResponse{Directories: []*repb.Directory{dir}})
	require.NoError(t, err)
	var responses [][]byte
	for i := range 3 {
		tokenBytes, err := proto.Marshal(&repb.GetTreeResponse{NextPageToken: fmt.Sprintf("page-%d", i)})
		require.NoError(t, err)
		// Valid protobuf, but decoding/re-encoding would reorder these fields.
		responses = append(responses, append(tokenBytes, directoryBytes...))
	}
	for _, code := range []codes.Code{codes.OK, codes.Unavailable, codes.Canceled} {
		t.Run(code.String(), func(t *testing.T) {
			ctx := t.Context()
			var canceled chan struct{}
			if code == codes.Canceled {
				canceled = make(chan struct{})
			}
			upstream := grpc.NewServer(grpc.ForceServerCodec(rawTestCodec{}))
			repb.RegisterContentAddressableStorageServer(upstream, &rawTreeServer{
				responses: responses, err: gstatus.Error(code, "upstream error"), canceled: canceled,
			})
			upstreamListener := bufconn.Listen(1024 * 1024)
			t.Cleanup(upstream.Stop)
			go upstream.Serve(upstreamListener)
			conn, err := testenv.LocalGRPCConn(t, ctx, upstreamListener)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			env := testenv.GetTestEnv(t)
			server, err := vfs_server.New(env, t.TempDir())
			require.NoError(t, err)
			env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
			server.PrepareContentProxy(metadata.NewOutgoingContext(ctx, metadata.Pairs("task-context", "executor")))
			proxyListener := bufconn.Listen(1024 * 1024)
			require.NoError(t, server.Start(proxyListener))
			t.Cleanup(server.Stop)
			proxyConn, err := testenv.LocalGRPCConn(t, ctx, proxyListener)
			require.NoError(t, err)
			t.Cleanup(func() { proxyConn.Close() })
			client := repb.NewContentAddressableStorageClient(proxyConn)
			requestCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			stream, err := client.GetTree(requestCtx, &repb.GetTreeRequest{}, grpc.ForceCodec(rawTestCodec{}))
			require.NoError(t, err)
			for _, expected := range responses {
				var actual []byte
				require.NoError(t, stream.RecvMsg(&actual))
				require.True(t, bytes.Equal(expected, actual), "proxy must preserve serialized response bytes")
			}
			if code == codes.Canceled {
				cancel()
				select {
				case <-canceled:
				case <-time.After(5 * time.Second):
					t.Fatal("upstream RPC was not canceled")
				}
				return
			}
			var extra []byte
			err = stream.RecvMsg(&extra)
			if code == codes.OK {
				require.ErrorIs(t, err, io.EOF)
			} else {
				require.Equal(t, code, gstatus.Code(err))
			}
			// Ordinary clients still decode the unchanged wire format.
			typedStream, err := client.GetTree(ctx, &repb.GetTreeRequest{})
			require.NoError(t, err)
			for i := range responses {
				rsp, err := typedStream.Recv()
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("page-%d", i), rsp.GetNextPageToken())
				require.Len(t, rsp.GetDirectories()[0].GetFiles(), len(dir.Files))
			}
			_, err = typedStream.Recv()
			if code == codes.OK {
				require.ErrorIs(t, err, io.EOF)
			} else {
				require.Equal(t, code, gstatus.Code(err))
			}
		})
	}
}

func BenchmarkPrepare(b *testing.B) {
	for _, dirs := range []int{1, 100} {
		b.Run(fmt.Sprintf("100KFiles/%dDirs", dirs), func(b *testing.B) {
			tree := &repb.Tree{Root: &repb.Directory{}}
			for i := range dirs {
				dir := &repb.Directory{}
				for j := range 100_000 / dirs {
					dir.Files = append(dir.Files, &repb.FileNode{
						Name:   fmt.Sprintf("file-%06d", j),
						Digest: &repb.Digest{Hash: hash.String(fmt.Sprintf("%d/%d", i, j)), SizeBytes: 4096},
					})
				}
				d, err := digest.ComputeForMessage(dir, repb.DigestFunction_SHA256)
				require.NoError(b, err)
				tree.Root.Directories = append(tree.Root.Directories, &repb.DirectoryNode{Name: fmt.Sprintf("dir-%d", i), Digest: d})
				tree.Children = append(tree.Children, dir)
			}
			env := testenv.GetTestEnv(b)
			workspace := b.TempDir()
			layout := &container.FileSystemLayout{Inputs: tree, DigestFunction: repb.DigestFunction_SHA256}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				server, err := vfs_server.New(env, workspace)
				require.NoError(b, err)
				_, err = server.Prepare(b.Context(), layout, nil)
				require.NoError(b, err)
			}
		})
	}
}

func TestPrepareSharedDirectories(t *testing.T) {
	server, env := newServer(t)
	dir := &repb.Directory{Files: []*repb.FileNode{
		{Name: "file", Digest: &repb.Digest{Hash: digest.EmptySha256}},
		{Name: "executable", Digest: &repb.Digest{Hash: digest.EmptySha256}, IsExecutable: true},
	}}
	d, err := digest.ComputeForMessage(dir, repb.DigestFunction_SHA256)
	require.NoError(t, err)
	tree := &repb.Tree{
		Root: &repb.Directory{Directories: []*repb.DirectoryNode{
			{Name: "a", Digest: d}, {Name: "b", Digest: d},
		}},
		Children: []*repb.Directory{dir},
	}
	prepare(t, env, server, tree)
	ctx := t.Context()
	lookup := func(parentID uint64, name string) *vfspb.LookupResponse {
		rsp, err := server.Lookup(ctx, &vfspb.LookupRequest{ParentId: parentID, Name: name})
		require.NoError(t, err)
		return rsp
	}
	a := lookup(vfscommon.RootInodeId, "a")
	b := lookup(vfscommon.RootInodeId, "b")
	fileA := lookup(a.GetId(), "file")
	fileB := lookup(b.GetId(), "file")
	require.NotEqual(t, fileA.GetId(), fileB.GetId())
	require.EqualValues(t, 0644, fileA.GetAttrs().GetPerm())
	require.EqualValues(t, 0755, lookup(a.GetId(), "executable").GetAttrs().GetPerm())
	_, err = server.SetAttr(ctx, &vfspb.SetAttrRequest{
		Id: fileA.GetId(), SetPerms: &vfspb.SetAttrRequest_SetPerms{Perms: 0600},
	})
	require.NoError(t, err)
	require.EqualValues(t, 0600, lookup(a.GetId(), "file").GetAttrs().GetPerm())
	require.EqualValues(t, 0644, lookup(b.GetId(), "file").GetAttrs().GetPerm())
	require.EqualValues(t, 0644, fileA.GetAttrs().GetPerm(), "previously returned attrs must remain immutable")
	require.NotZero(t, fileA.GetAttrs().GetMtimeNanos())
	require.Equal(t, fileA.GetAttrs().GetMtimeNanos(), lookup(a.GetId(), "file").GetAttrs().GetMtimeNanos())
	require.EqualValues(t, 4, server.ComputeStats().GetCasFilesCount())

	// Mutations must also work before an input's attributes have been accessed.
	_, err = server.Unlink(ctx, &vfspb.UnlinkRequest{ParentId: b.GetId(), Name: "executable"})
	require.NoError(t, err)
	_, err = server.Lookup(ctx, &vfspb.LookupRequest{ParentId: b.GetId(), Name: "executable"})
	requireSyscallError(t, err, syscall.ENOENT)
	require.EqualValues(t, 1, lookup(a.GetId(), "executable").GetAttrs().GetNlink())
}

type evictingFileCache struct {
	interfaces.FileCache
	once sync.Once
}

func (fc *evictingFileCache) Open(ctx context.Context, node *repb.FileNode) (*os.File, error) {
	fc.once.Do(func() {
		fc.FileCache.DeleteFile(ctx, node)
	})
	return fc.FileCache.Open(ctx, node)
}

func TestCASFileFetchedOnDemand(t *testing.T) {
	ctx, env, server, _ := newServerWithEnv(t)
	d := setFile(t, env, ctx, "", "only fetched after open")
	fileNode := &repb.FileNode{Name: "input.txt", Digest: d}
	layout := &container.FileSystemLayout{
		DigestFunction: repb.DigestFunction_SHA256,
		Inputs: &repb.Tree{Root: &repb.Directory{
			Files: []*repb.FileNode{fileNode},
		}},
	}

	_, err := server.Prepare(ctx, layout, nil)
	require.NoError(t, err)
	require.False(t, env.GetFileCache().ContainsFile(ctx, fileNode))

	require.Equal(t, "only fetched after open", readFromVFS(t, server, "input.txt"))
	require.True(t, env.GetFileCache().ContainsFile(ctx, fileNode))
	stats := server.ComputeStats()
	require.EqualValues(t, 1, stats.GetCasFilesAccessedCount())
	require.EqualValues(t, 1, stats.GetFileDownloadCount())
	require.Equal(t, d.GetSizeBytes(), stats.GetFileDownloadSizeBytes())
}

type failingByteStreamClient struct {
	bspb.ByteStreamClient
	err      error
	failures int
	attempts int
}

func (c *failingByteStreamClient) Read(ctx context.Context, req *bspb.ReadRequest, opts ...grpc.CallOption) (bspb.ByteStream_ReadClient, error) {
	c.attempts++
	if c.attempts <= c.failures {
		return nil, c.err
	}
	return c.ByteStreamClient.Read(ctx, req, opts...)
}

func TestCASFileDownloadRetried(t *testing.T) {
	ctx, env, server, _ := newServerWithEnv(t)
	contents := "download succeeds after retrying"
	d := setFile(t, env, ctx, "", contents)
	client := &failingByteStreamClient{
		ByteStreamClient: env.GetByteStreamClient(),
		err:              status.UnavailableError("cache unavailable"),
		failures:         2,
	}
	env.SetByteStreamClient(client)
	_, err := server.Prepare(ctx, &container.FileSystemLayout{
		DigestFunction: repb.DigestFunction_SHA256,
		Inputs: &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{
			{Name: "input.txt", Digest: d},
		}}},
	}, nil)
	require.NoError(t, err)

	// GetBlob's existing retries recover the download without poisoning the
	// action result or its contents.
	require.Equal(t, contents, readFromVFS(t, server, "input.txt"))
	require.Equal(t, 3, client.attempts)
	require.NoError(t, server.TaskError())
}

func TestFailedPrefetchRetriedFromCAS(t *testing.T) {
	ctx, env, server, _ := newServerWithEnv(t)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	contents := "downloaded without the failed prefetcher"
	d := &repb.Digest{Hash: hash.String(contents), SizeBytes: int64(len(contents))}
	layout := &container.FileSystemLayout{
		DigestFunction: repb.DigestFunction_SHA256,
		Inputs: &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{
			{Name: "a.txt", Digest: d},
			{Name: "b.txt", Digest: d},
		}}},
	}

	// Prefetch fails because the input is initially unavailable in CAS.
	fetcher, err := dirtools.NewTreeFetcher(ctx, env, "", repb.DigestFunction_SHA256, layout.Inputs, &dirtools.DownloadTreeOpts{})
	require.NoError(t, err)
	_, err = fetcher.Start()
	require.NoError(t, err)
	_, err = fetcher.Wait()
	require.Error(t, err)
	setFile(t, env, ctx, "", contents)
	_, err = server.Prepare(ctx, layout, fetcher)
	require.NoError(t, err)

	// Every reader must observe the prefetch failure and fall back to CAS,
	// including readers that arrive after an earlier reader consumed the error.
	require.Equal(t, contents, readFromVFS(t, server, "a.txt"))
	require.Equal(t, contents, readFromVFS(t, server, "b.txt"))
	require.NoError(t, server.TaskError())
}

func TestCASFileDownloadErrorRetainedUntilNextTask(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		err          error
		code         codes.Code
		wantAttempts int
	}{
		{name: "missing", err: status.NotFoundError("blob evicted"), code: codes.FailedPrecondition, wantAttempts: 1},
		{name: "unavailable", err: status.UnavailableError("cache unavailable"), code: codes.Unavailable, wantAttempts: 4},
		{name: "timeout", err: status.DeadlineExceededError("download timed out"), code: codes.DeadlineExceeded, wantAttempts: 4},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, env, server, _ := newServerWithEnv(t)
			d := setFile(t, env, ctx, "", "input contents")
			client := &failingByteStreamClient{
				ByteStreamClient: env.GetByteStreamClient(),
				err:              testCase.err,
				failures:         100,
			}
			env.SetByteStreamClient(client)
			layout := &container.FileSystemLayout{
				DigestFunction: repb.DigestFunction_SHA256,
				Inputs: &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{
					{Name: "input.txt", Digest: d},
				}}},
			}
			_, err := server.Prepare(ctx, layout, nil)
			require.NoError(t, err)
			node, err := server.Lookup(ctx, &vfspb.LookupRequest{ParentId: vfscommon.RootInodeId, Name: "input.txt"})
			require.NoError(t, err)

			// Preserve the final gRPC status without retrying missing blobs or
			// multiplying the CAS downloader's retries for transient failures.
			_, err = server.Open(ctx, &vfspb.OpenRequest{Id: node.GetId()})
			require.Equal(t, testCase.code, gstatus.Code(err))
			require.Equal(t, testCase.wantAttempts, client.attempts)
			require.Equal(t, err, server.TaskError())
			require.Contains(t, err.Error(), "input.txt")
			if testCase.code == codes.FailedPrecondition {
				failure := gstatus.Convert(err).Details()[0].(*errdetails.PreconditionFailure)
				require.Equal(t, "MISSING", failure.GetViolations()[0].GetType())
				require.Contains(t, failure.GetViolations()[0].GetSubject(), d.GetHash())
			}

			// A later successful open cannot undo the error already seen by the
			// action, but a recycled workspace must start without that error.
			client.failures = 0
			require.Equal(t, "input contents", readFromVFS(t, server, "input.txt"))
			require.Equal(t, err, server.TaskError())
			_, err = server.Prepare(ctx, layout, nil)
			require.NoError(t, err)
			require.NoError(t, server.TaskError())
		})
	}
}

func TestCASFileDownloadCanceled(t *testing.T) {
	ctx, _, server, _ := newServerWithEnv(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	_, err := server.Prepare(ctx, &container.FileSystemLayout{
		DigestFunction: repb.DigestFunction_SHA256,
		Inputs: &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{
			{Name: "input.txt", Digest: &repb.Digest{Hash: hash.String("input"), SizeBytes: 5}},
		}}},
	}, nil)
	require.NoError(t, err)
	node, err := server.Lookup(ctx, &vfspb.LookupRequest{ParentId: vfscommon.RootInodeId, Name: "input.txt"})
	require.NoError(t, err)

	// Cancellation must stop retries promptly and remain a gRPC cancellation.
	cancel()
	start := time.Now()
	_, err = server.Open(ctx, &vfspb.OpenRequest{Id: node.GetId()})
	require.Equal(t, codes.Canceled, gstatus.Code(err))
	require.Equal(t, err, server.TaskError())
	require.Less(t, time.Since(start), time.Second)
}

type sparseInputFetcher struct {
	path     string
	contents []byte
}

func TestLocalNamespaceMetadataDoesNotFetchInputs(t *testing.T) {
	ctx := t.Context()
	root := testfs.MakeTempDir(t)
	fetcher := &sparseInputFetcher{path: filepath.Join(root, "unfetched"), contents: []byte("input contents")}
	server, err := vfs_server.NewLocal(root, fetcher.Open)
	require.NoError(t, err)
	defer server.Stop()
	_, err = server.Prepare(ctx, &container.FileSystemLayout{
		DigestFunction: repb.DigestFunction_SHA256,
		Inputs: &repb.Tree{Root: &repb.Directory{
			Files:    []*repb.FileNode{{Name: "input", Digest: &repb.Digest{Hash: hash.String(string(fetcher.contents)), SizeBytes: int64(len(fetcher.contents))}, IsExecutable: true}},
			Symlinks: []*repb.SymlinkNode{{Name: "link", Target: "input"}},
		}},
	}, fetcher)
	require.NoError(t, err)
	client := vfs_server.NewDirectClient(server)
	listing, err := client.GetDirectoryContents(ctx, &vfspb.GetDirectoryContentsRequest{Id: vfscommon.RootInodeId})
	require.NoError(t, err)
	require.Len(t, listing.GetNodes(), 2)
	file, err := client.Lookup(ctx, &vfspb.LookupRequest{ParentId: vfscommon.RootInodeId, Name: "input"})
	require.NoError(t, err)
	attrs, err := client.GetAttr(ctx, &vfspb.GetAttrRequest{Id: file.GetId()})
	require.NoError(t, err)
	require.EqualValues(t, len(fetcher.contents), attrs.GetAttrs().GetSize())
	require.EqualValues(t, 0755, attrs.GetAttrs().GetPerm())
	link, err := client.Lookup(ctx, &vfspb.LookupRequest{ParentId: vfscommon.RootInodeId, Name: "link"})
	require.NoError(t, err)
	require.Equal(t, "input", link.GetSymlinkTarget())
	_, err = os.Stat(fetcher.path)
	require.True(t, os.IsNotExist(err), "metadata operations must not fetch file contents")
}

func (f *sparseInputFetcher) Fetch(ctx context.Context, node *repb.FileNode) error {
	file, err := os.Create(f.path)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Truncate(int64(len(f.contents)))
}

func (f *sparseInputFetcher) Open(ctx context.Context, node *repb.FileNode) (*os.File, error) {
	return os.Open(f.path)
}

func (f *sparseInputFetcher) FetchRange(ctx context.Context, node *repb.FileNode, offset, length int64) error {
	if offset >= int64(len(f.contents)) {
		return nil
	}
	file, err := os.OpenFile(f.path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteAt(f.contents[offset:min(offset+length, int64(len(f.contents)))], offset)
	return err
}

func TestRangedCASFileDataPaths(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	fetcher := &sparseInputFetcher{path: filepath.Join(root, "cas"), contents: []byte("0123456789abcdefghijklmnopqrstuvwxyz")}
	server, err := vfs_server.NewLocal(root, fetcher.Open)
	require.NoError(t, err)
	defer server.Stop()
	layout := &container.FileSystemLayout{
		DigestFunction: repb.DigestFunction_SHA256,
		Inputs: &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{{
			Name: "input", Digest: &repb.Digest{Hash: hash.String(string(fetcher.contents)), SizeBytes: int64(len(fetcher.contents))},
		}}}},
	}
	_, err = server.Prepare(ctx, layout, fetcher)
	require.NoError(t, err)
	node, err := server.Lookup(ctx, &vfspb.LookupRequest{ParentId: vfscommon.RootInodeId, Name: "input"})
	require.NoError(t, err)
	h, err := server.Open(ctx, &vfspb.OpenRequest{Id: node.GetId()})
	require.NoError(t, err)
	for _, tc := range []struct {
		offset int64
		length int32
		want   string
	}{
		{5, 4, "5678"}, {34, 10, "yz"}, {36, 4, ""}, {0, 0, ""},
	} {
		rsp, err := server.Read(ctx, &vfspb.ReadRequest{HandleId: h.GetHandleId(), Offset: tc.offset, NumBytes: tc.length})
		require.NoError(t, err)
		require.Equal(t, tc.want, string(rsp.GetData()))
	}
	for _, tc := range []struct {
		whence uint32
		want   uint64
	}{
		{unix.SEEK_DATA, 10}, {unix.SEEK_HOLE, uint64(len(fetcher.contents))},
	} {
		rsp, err := server.Lseek(ctx, &vfspb.LseekRequest{HandleId: h.GetHandleId(), Offset: 10, Whence: tc.whence})
		require.NoError(t, err)
		require.Equal(t, tc.want, rsp.GetOffset())
	}
	out, err := server.Create(ctx, &vfspb.CreateRequest{ParentId: vfscommon.RootInodeId, Name: "output", Flags: syscall.O_CREAT | syscall.O_RDWR, Mode: 0644})
	require.NoError(t, err)
	_, err = server.CopyFileRange(ctx, &vfspb.CopyFileRangeRequest{ReadHandleId: h.GetHandleId(), ReadHandleOffset: 10, WriteHandleId: out.GetHandleId(), NumBytes: 10})
	require.NoError(t, err)
	rsp, err := server.Read(ctx, &vfspb.ReadRequest{HandleId: out.GetHandleId(), NumBytes: 10})
	require.NoError(t, err)
	require.Equal(t, "abcdefghij", string(rsp.GetData()))
	layout.OutputFiles = []string{"input"}
	destination := t.TempDir()
	require.NoError(t, server.MaterializeOutputs(ctx, layout, destination))
	file, err := os.Open(filepath.Join(destination, "input"))
	require.NoError(t, err)
	defer file.Close()
	data, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, fetcher.contents, data)
}

func TestCASFileRefetchedIfEvictedBeforeOpen(t *testing.T) {
	ctx, env, server, _ := newServerWithEnv(t)
	contents := "refetched after eviction"
	d := setFile(t, env, ctx, "", contents)
	fileNode := &repb.FileNode{Name: "input.txt", Digest: d}

	tmpPath := filepath.Join(testfs.MakeTempDir(t), "input.txt")
	require.NoError(t, os.WriteFile(tmpPath, []byte(contents), 0644))
	require.NoError(t, env.GetFileCache().AddFile(ctx, fileNode, tmpPath))
	env.SetFileCache(&evictingFileCache{FileCache: env.GetFileCache()})

	_, err := server.Prepare(ctx, &container.FileSystemLayout{
		DigestFunction: repb.DigestFunction_SHA256,
		Inputs: &repb.Tree{Root: &repb.Directory{
			Files: []*repb.FileNode{fileNode},
		}},
	}, nil)
	require.NoError(t, err)

	require.Equal(t, contents, readFromVFS(t, server, "input.txt"))
}

func TestPrefetchedFileRefetchedIfEvictedBeforeOpen(t *testing.T) {
	ctx, env, server, _ := newServerWithEnv(t)
	contents := "refetched after prefetch eviction"
	d := setFile(t, env, ctx, "", contents)
	fileNode := &repb.FileNode{Name: "input.txt", Digest: d}
	layout := &container.FileSystemLayout{
		DigestFunction: repb.DigestFunction_SHA256,
		Inputs: &repb.Tree{Root: &repb.Directory{
			Files: []*repb.FileNode{fileNode},
		}},
	}
	tf, err := dirtools.NewTreeFetcher(ctx, env, "", repb.DigestFunction_SHA256, layout.Inputs, &dirtools.DownloadTreeOpts{})
	require.NoError(t, err)
	_, err = tf.Start()
	require.NoError(t, err)
	_, err = tf.Wait()
	require.NoError(t, err)
	require.True(t, env.GetFileCache().ContainsFile(ctx, fileNode))
	env.SetFileCache(&evictingFileCache{FileCache: env.GetFileCache()})

	_, err = server.Prepare(ctx, layout, tf)
	require.NoError(t, err)
	require.Equal(t, contents, readFromVFS(t, server, "input.txt"))
}

func TestMaterializeOutputs(t *testing.T) {
	ctx, _, server, _ := newServerWithEnv(t)
	layout := &container.FileSystemLayout{
		DigestFunction:   repb.DigestFunction_SHA256,
		Inputs:           &repb.Tree{Root: &repb.Directory{}},
		WorkingDirectory: "work",
		OutputFiles:      []string{"out/result.txt"},
	}
	_, err := server.Prepare(ctx, layout, nil)
	require.NoError(t, err)

	workRsp, err := server.Lookup(ctx, &vfspb.LookupRequest{ParentId: vfscommon.RootInodeId, Name: "work"})
	require.NoError(t, err)
	outRsp, err := server.Lookup(ctx, &vfspb.LookupRequest{ParentId: workRsp.GetId(), Name: "out"})
	require.NoError(t, err)
	createRsp, err := server.Create(ctx, &vfspb.CreateRequest{
		ParentId: outRsp.GetId(),
		Name:     "result.txt",
		Flags:    uint32(os.O_CREATE | os.O_RDWR),
		Mode:     0755,
	})
	require.NoError(t, err)
	_, err = server.Write(ctx, &vfspb.WriteRequest{HandleId: createRsp.GetHandleId(), Data: []byte("result")})
	require.NoError(t, err)
	_, err = server.Release(ctx, &vfspb.ReleaseRequest{HandleId: createRsp.GetHandleId()})
	require.NoError(t, err)

	outputRoot := testfs.MakeTempDir(t)
	require.NoError(t, server.MaterializeOutputs(ctx, layout, outputRoot))
	b, err := os.ReadFile(filepath.Join(outputRoot, "work", "out", "result.txt"))
	require.NoError(t, err)
	require.Equal(t, "result", string(b))
}

func TestMaterializeOutputsOutsideWorkingDirectory(t *testing.T) {
	ctx, _, server, _ := newServerWithEnv(t)
	layout := &container.FileSystemLayout{
		DigestFunction:   repb.DigestFunction_SHA256,
		Inputs:           &repb.Tree{Root: &repb.Directory{}},
		WorkingDirectory: "out/Default",
		OutputPaths:      []string{"../../gen/result.txt"},
	}
	_, err := server.Prepare(ctx, layout, nil)
	require.NoError(t, err)

	genRsp, err := server.Lookup(ctx, &vfspb.LookupRequest{ParentId: vfscommon.RootInodeId, Name: "gen"})
	require.NoError(t, err)
	createRsp, err := server.Create(ctx, &vfspb.CreateRequest{
		ParentId: genRsp.GetId(),
		Name:     "result.txt",
		Flags:    uint32(os.O_CREATE | os.O_RDWR),
		Mode:     0644,
	})
	require.NoError(t, err)
	_, err = server.Write(ctx, &vfspb.WriteRequest{HandleId: createRsp.GetHandleId(), Data: []byte("result")})
	require.NoError(t, err)
	_, err = server.Release(ctx, &vfspb.ReleaseRequest{HandleId: createRsp.GetHandleId()})
	require.NoError(t, err)

	outputRoot := testfs.MakeTempDir(t)
	require.NoError(t, server.MaterializeOutputs(ctx, layout, outputRoot))
	b, err := os.ReadFile(filepath.Join(outputRoot, "gen", "result.txt"))
	require.NoError(t, err)
	require.Equal(t, "result", string(b))
}

func TestPrepareRejectsEscapingOutputPath(t *testing.T) {
	ctx, _, server, _ := newServerWithEnv(t)
	_, err := server.Prepare(ctx, &container.FileSystemLayout{
		Inputs:           &repb.Tree{Root: &repb.Directory{}},
		WorkingDirectory: "work",
		OutputFiles:      []string{"../../escape"},
	}, nil)
	require.Error(t, err)
}

func TestGetLayout(t *testing.T) {
	server, env := newServer(t)
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

	require.NoError(t, err)
	prepare(t, env, server, inputTree)
	require.NoError(t, err)

	rsp, err := server.GetDirectoryContents(ctx, &vfspb.GetDirectoryContentsRequest{Id: vfscommon.RootInodeId})
	require.NoError(t, err)

	expectedRsp := &vfspb.GetDirectoryContentsResponse{
		Nodes: []*vfspb.Node{
			{Name: "adirectory", Attrs: &vfspb.Attrs{Size: 1000, Perm: 0755, Nlink: 1}, Mode: syscall.S_IFDIR},
			{Name: "afile.txt", Attrs: &vfspb.Attrs{Size: 123, Perm: 0644, Immutable: true, Nlink: 1}, Mode: syscall.S_IFREG},
			{Name: "anotherfile.txt", Attrs: &vfspb.Attrs{Size: 456, Perm: 0755, Immutable: true, Nlink: 1}, Mode: syscall.S_IFREG},
			{Name: "asymlink", Attrs: &vfspb.Attrs{Size: 1000, Perm: 0644}, Mode: syscall.S_IFLNK},
		},
	}
	require.Empty(t, cmp.Diff(expectedRsp, rsp, protocmp.Transform(),
		protocmp.IgnoreFields(&vfspb.Node{}, "id"), protocmp.IgnoreFields(&vfspb.Attrs{}, "atime_nanos", "mtime_nanos")))

	subdirLookupRsp, err := server.Lookup(ctx, &vfspb.LookupRequest{ParentId: vfscommon.RootInodeId, Name: "adirectory"})
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
	server, env := newServer(t)
	ctx := context.Background()

	prepare(t, env, server, &repb.Tree{})

	testFile := "test.file"
	createRsp, err := server.Create(ctx, &vfspb.CreateRequest{
		ParentId: vfscommon.RootInodeId,
		Name:     testFile,
		Flags:    uint32(os.O_CREATE | os.O_RDWR),
		Mode:     0644,
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
	server, env := newServer(t)
	ctx := context.Background()

	prepare(t, env, server, &repb.Tree{})

	mkdirRsp, err := server.Mkdir(ctx, &vfspb.MkdirRequest{ParentId: vfscommon.RootInodeId, Name: "dir", Perms: 0700})
	require.NoError(t, err)

	getAttrRsp, err := server.GetAttr(ctx, &vfspb.GetAttrRequest{Id: mkdirRsp.GetId()})
	require.NoError(t, err)
	require.EqualValues(t, 0700, getAttrRsp.GetAttrs().GetPerm())

	// Creating subdir should succeed.
	_, err = server.Mkdir(ctx, &vfspb.MkdirRequest{ParentId: mkdirRsp.GetId(), Name: "subdir", Perms: 0700})
	require.NoError(t, err)

	// Deleting non-empty dir should fail.
	_, err = server.Rmdir(ctx, &vfspb.RmdirRequest{ParentId: vfscommon.RootInodeId, Name: "dir"})
	requireSyscallError(t, err, syscall.ENOTEMPTY)

	_, err = server.Rmdir(ctx, &vfspb.RmdirRequest{ParentId: mkdirRsp.GetId(), Name: "subdir"})
	require.NoError(t, err)
}

func TestFilenameOps(t *testing.T) {
	server, env := newServer(t)
	ctx := context.Background()

	prepare(t, env, server, &repb.Tree{})

	testFile := "a.file"
	writeToVFS(t, server, testFile, "some data")

	newName := "b.file"
	_, err := server.Rename(ctx, &vfspb.RenameRequest{
		OldParentId: vfscommon.RootInodeId,
		OldName:     testFile,
		NewParentId: vfscommon.RootInodeId,
		NewName:     newName,
	})
	require.NoError(t, err)

	// Old file shouldn't exist anymore.
	_, err = server.Lookup(ctx, &vfspb.LookupRequest{Name: testFile})
	requireSyscallError(t, err, syscall.ENOENT)

	lookupRsp, err := server.Lookup(ctx, &vfspb.LookupRequest{ParentId: vfscommon.RootInodeId, Name: newName})
	require.NoError(t, err)

	getAttrRsp, err := server.GetAttr(ctx, &vfspb.GetAttrRequest{Id: lookupRsp.GetId()})
	require.NoError(t, err)
	require.EqualValues(t, 0644, getAttrRsp.GetAttrs().GetPerm())

	_, err = server.Unlink(ctx, &vfspb.UnlinkRequest{ParentId: vfscommon.RootInodeId, Name: newName})
	require.NoError(t, err)

	// File shouldn't exist anymore.
	_, err = server.Lookup(ctx, &vfspb.LookupRequest{Name: newName})
	requireSyscallError(t, err, syscall.ENOENT)
}

func TestHardlink(t *testing.T) {
	server, env := newServer(t)
	ctx := context.Background()

	prepare(t, env, server, &repb.Tree{})

	fileID := writeToVFS(t, server, "src", "hello")
	_, err := server.Link(ctx, &vfspb.LinkRequest{
		ParentId: vfscommon.RootInodeId,
		Name:     "dst",
		TargetId: fileID,
	})
	require.NoError(t, err)

	content := readFromVFS(t, server, "dst")
	require.Equal(t, "hello", content, "hardlink content should match source file")

	// Update src with new contents
	writeToVFS(t, server, "src", "world")

	content = readFromVFS(t, server, "dst")
	require.Equal(t, "world", content, "hardlink content should match updated source file")

	// Unlink src; hardlink should still be readable
	_, err = server.Unlink(ctx, &vfspb.UnlinkRequest{ParentId: vfscommon.RootInodeId, Name: "src"})
	require.NoError(t, err)

	content = readFromVFS(t, server, "dst")
	require.Equal(t, "world", content, "hardlink should be readable after unlinking source file")
}

func TestFileLocking(t *testing.T) {
	server, env := newServer(t)
	ctx := context.Background()

	prepare(t, env, server, &repb.Tree{})

	var nodeID uint64
	// Init two different file handle IDs referring to the same inode.
	var id1, id2 uint64
	{
		req := &vfspb.CreateRequest{
			ParentId: vfscommon.RootInodeId,
			Name:     "lock",
			Flags:    uint32(os.O_CREATE),
			Mode:     0644,
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
