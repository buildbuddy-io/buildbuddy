package cachetools_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/rpcutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/testing/protocmp"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"

	"google.golang.org/grpc/metadata"
)

var (
	testInstance = "test-instance-name-1"

	fakeTreeRoot = rspb.ResourceName{
		Digest: &repb.Digest{
			Hash:      "fake-tree-root",
			SizeBytes: 22182,
		},
		InstanceName:   testInstance,
		DigestFunction: repb.DigestFunction_BLAKE3,
		CacheType:      rspb.CacheType_CAS,
	}
)

type resourceAndTreeCache struct {
	rn   *rspb.ResourceName
	data *capb.TreeCache
}

func setUpFakeData(getTreeResponse *repb.GetTreeResponse, fileCacheContents []*resourceAndTreeCache, remoteContents []*resourceAndTreeCache) (*digest.CASResourceName, *fakeCasClient, *fakeFilecache, *fakeBytestreamClient) {

	cas := &fakeCasClient{
		treeDigest: fakeTreeRoot.GetDigest(),
		response:   getTreeResponse,
	}
	fc := &fakeFilecache{
		files: make(map[string][]byte),
	}
	for _, f := range fileCacheContents {
		fileData, _ := f.data.MarshalVT()
		rn, err := digest.CASResourceNameFromProto(f.rn)
		if err != nil {
			panic(fmt.Sprintf("failed to convert resource name to CAS: %s", err))
		}
		fileNode, _ := cachetools.MakeFileNode(rn)
		fc.Write(context.Background(), fileNode, fileData)
	}
	fc.writeCount = 0

	bsDataMap := make(map[string][]byte)
	for _, f := range remoteContents {
		bsData, _ := f.data.MarshalVT()
		rn, err := digest.CASResourceNameFromProto(f.rn)
		if err != nil {
			panic(fmt.Sprintf("failed to convert resource name to CAS: %s", err))
		}
		dlString := rn.DownloadString()
		bsDataMap[dlString] = bsData
	}
	bs := &fakeBytestreamClient{
		mu:   &sync.Mutex{},
		data: bsDataMap,
	}

	rn, err := digest.CASResourceNameFromProto(&fakeTreeRoot)
	if err != nil {
		panic(fmt.Sprintf("failed to convert resource name to CAS: %s", err))
	}
	return rn, cas, fc, bs
}

func makeDigest(name string) *repb.Digest {
	return &repb.Digest{
		Hash:      name,
		SizeBytes: 1,
	}
}

func makeResource(name string) *rspb.ResourceName {
	return &rspb.ResourceName{
		Digest:         makeDigest(name),
		InstanceName:   testInstance,
		CacheType:      rspb.CacheType_CAS,
		DigestFunction: repb.DigestFunction_BLAKE3,
	}
}

func makeSubtreeResource(r *rspb.ResourceName) *repb.SubtreeResourceName {
	return &repb.SubtreeResourceName{
		Digest:         r.GetDigest(),
		InstanceName:   r.GetInstanceName(),
		DigestFunction: r.GetDigestFunction(),
	}
}

func makeTreeCache(contents []*capb.DirectoryWithDigest, splits []*rspb.ResourceName) *resourceAndTreeCache {
	cache := &capb.TreeCache{
		Children:          contents,
		TreeCacheChildren: splits,
	}
	digest, _ := digest.ComputeForMessage(cache, repb.DigestFunction_BLAKE3)
	return &resourceAndTreeCache{
		&rspb.ResourceName{
			Digest:         digest,
			DigestFunction: repb.DigestFunction_BLAKE3,
			InstanceName:   testInstance,
			CacheType:      rspb.CacheType_CAS,
		},
		cache,
	}
}

func makeDirectory(name string, children []string) *capb.DirectoryWithDigest {
	childDirNodes := []*repb.DirectoryNode{}
	for i, child := range children {
		childDirNodes = append(childDirNodes, &repb.DirectoryNode{
			Name:   fmt.Sprintf("child_dir_%d", i),
			Digest: makeDigest(child),
		})
	}

	return &capb.DirectoryWithDigest{
		ResourceName: makeResource(name),
		Directory: &repb.Directory{
			Directories: childDirNodes,
			Files: []*repb.FileNode{
				{
					Name: name,
				},
			},
		},
	}
}

func checkDirectoriesMatch(t *testing.T, expected []string, actual []*repb.Directory) {
	// bit of a kludge - we're going to check everything's here by looking for
	// a file with the same name as the expected directory.
	actualDirNamesFromFiles := []string{}
	for _, d := range actual {
		actualDirNamesFromFiles = append(actualDirNamesFromFiles, d.Files[0].GetName())
	}

	assert.ElementsMatch(t, expected, actualDirNamesFromFiles)
}

func TestBasicGetTree(t *testing.T) {
	flags.Set(t, "cache.request_cached_subtree_digests", false)
	a := makeDirectory("a", []string{"b", "c"})
	b := makeDirectory("b", nil)
	c := makeDirectory("c", nil)
	ctx := context.Background()
	root, cas, fc, bs := setUpFakeData(&repb.GetTreeResponse{
		Directories: []*repb.Directory{a.Directory, b.Directory, c.Directory},
	}, nil, nil)

	tree, err := cachetools.GetAndMaybeCacheTreeFromRootDirectoryDigest(ctx, cas, root, fc, bs)
	assert.NoError(t, err)
	assert.Equal(t, tree.GetRoot(), a.Directory)
	checkDirectoriesMatch(t, []string{"b", "c"}, tree.GetChildren())
	assert.Equal(t, 0, fc.readCount)
	assert.Equal(t, 0, fc.writeCount)
	assert.Equal(t, 0, bs.readCount)
}

func TestBasicGetTree_subtreesEnabled(t *testing.T) {
	flags.Set(t, "cache.request_cached_subtree_digests", true)
	a := makeDirectory("a", []string{"b", "c"})
	b := makeDirectory("b", nil)
	c := makeDirectory("c", nil)
	ctx := context.Background()
	root, cas, fc, bs := setUpFakeData(&repb.GetTreeResponse{
		Directories: []*repb.Directory{a.Directory, b.Directory, c.Directory},
	}, nil, nil)

	tree, err := cachetools.GetAndMaybeCacheTreeFromRootDirectoryDigest(ctx, cas, root, fc, bs)
	assert.NoError(t, err)
	assert.Equal(t, tree.GetRoot(), a.Directory)
	checkDirectoriesMatch(t, []string{"b", "c"}, tree.GetChildren())
	assert.Equal(t, 0, fc.readCount)
	assert.Equal(t, 0, fc.writeCount)
	assert.Equal(t, 0, bs.readCount)
}

func TestBasicSubtrees_allLocal(t *testing.T) {
	flags.Set(t, "cache.request_cached_subtree_digests", true)
	a := makeDirectory("a", []string{"b", "c"})
	b := makeDirectory("b", nil)
	bCache := makeTreeCache([]*capb.DirectoryWithDigest{b}, []*rspb.ResourceName{})
	c := makeDirectory("c", nil)
	cCache := makeTreeCache([]*capb.DirectoryWithDigest{c}, []*rspb.ResourceName{})
	ctx := context.Background()
	root, cas, fc, bs := setUpFakeData(&repb.GetTreeResponse{
		Directories: []*repb.Directory{a.Directory},
		Subtrees: []*repb.SubtreeResourceName{
			makeSubtreeResource(bCache.rn),
			makeSubtreeResource(cCache.rn),
		},
	}, []*resourceAndTreeCache{
		bCache,
		cCache,
	}, nil)

	tree, err := cachetools.GetAndMaybeCacheTreeFromRootDirectoryDigest(ctx, cas, root, fc, bs)
	assert.NoError(t, err)
	assert.Equal(t, tree.GetRoot(), a.Directory)
	checkDirectoriesMatch(t, []string{"b", "c"}, tree.GetChildren())
	assert.Equal(t, 2, fc.readCount)
	assert.Equal(t, 0, fc.writeCount)
	assert.Equal(t, 0, bs.readCount)
}

func TestBasicSubtrees_allRemote(t *testing.T) {
	flags.Set(t, "cache.request_cached_subtree_digests", true)
	a := makeDirectory("a", []string{"b", "c"})
	b := makeDirectory("b", nil)
	bCache := makeTreeCache([]*capb.DirectoryWithDigest{b}, []*rspb.ResourceName{})
	c := makeDirectory("c", nil)
	cCache := makeTreeCache([]*capb.DirectoryWithDigest{c}, []*rspb.ResourceName{})
	ctx := context.Background()
	root, cas, fc, bs := setUpFakeData(&repb.GetTreeResponse{
		Directories: []*repb.Directory{a.Directory},
		Subtrees: []*repb.SubtreeResourceName{
			makeSubtreeResource(bCache.rn),
			makeSubtreeResource(cCache.rn),
		},
	},
		nil,
		[]*resourceAndTreeCache{
			bCache,
			cCache,
		})

	tree, err := cachetools.GetAndMaybeCacheTreeFromRootDirectoryDigest(ctx, cas, root, fc, bs)
	assert.NoError(t, err)
	assert.Equal(t, tree.GetRoot(), a.Directory)
	checkDirectoriesMatch(t, []string{"b", "c"}, tree.GetChildren())
	assert.Equal(t, 0, fc.readCount)
	assert.Equal(t, 2, fc.writeCount)
	assert.Equal(t, 2, bs.readCount)
}

func TestBasicSubtrees_mixedWithLocalSplit(t *testing.T) {
	flags.Set(t, "cache.request_cached_subtree_digests", true)
	a := makeDirectory("a", []string{"b", "c"})
	d := makeDirectory("d", nil)
	dCache := makeTreeCache([]*capb.DirectoryWithDigest{d}, []*rspb.ResourceName{})
	b := makeDirectory("b", []string{"d"})
	bCache := makeTreeCache([]*capb.DirectoryWithDigest{b}, []*rspb.ResourceName{dCache.rn})
	c := makeDirectory("c", nil)
	cCache := makeTreeCache([]*capb.DirectoryWithDigest{c}, []*rspb.ResourceName{})
	ctx := context.Background()
	root, cas, fc, bs := setUpFakeData(&repb.GetTreeResponse{
		Directories: []*repb.Directory{a.Directory},
		Subtrees: []*repb.SubtreeResourceName{
			makeSubtreeResource(bCache.rn),
			makeSubtreeResource(cCache.rn),
		},
	},
		[]*resourceAndTreeCache{
			bCache,
			dCache,
		},
		[]*resourceAndTreeCache{
			cCache,
		})

	tree, err := cachetools.GetAndMaybeCacheTreeFromRootDirectoryDigest(ctx, cas, root, fc, bs)
	assert.NoError(t, err)
	assert.Equal(t, tree.GetRoot(), a.Directory)
	checkDirectoriesMatch(t, []string{"b", "c", "d"}, tree.GetChildren())
	assert.Equal(t, 2, fc.readCount)
	assert.Equal(t, 1, fc.writeCount)
	assert.Equal(t, 1, bs.readCount)
}

func TestBasicSubtrees_mixedWithRemoteSplit(t *testing.T) {
	flags.Set(t, "cache.request_cached_subtree_digests", true)
	a := makeDirectory("a", []string{"b", "c"})
	d := makeDirectory("d", nil)
	dCache := makeTreeCache([]*capb.DirectoryWithDigest{d}, []*rspb.ResourceName{})
	b := makeDirectory("b", []string{"d"})
	bCache := makeTreeCache([]*capb.DirectoryWithDigest{b}, []*rspb.ResourceName{dCache.rn})
	c := makeDirectory("c", nil)
	cCache := makeTreeCache([]*capb.DirectoryWithDigest{c}, []*rspb.ResourceName{})
	ctx := context.Background()
	root, cas, fc, bs := setUpFakeData(&repb.GetTreeResponse{
		Directories: []*repb.Directory{a.Directory},
		Subtrees: []*repb.SubtreeResourceName{
			makeSubtreeResource(bCache.rn),
			makeSubtreeResource(cCache.rn),
		},
	},
		[]*resourceAndTreeCache{
			bCache,
		},
		[]*resourceAndTreeCache{
			cCache,
			dCache,
		})

	tree, err := cachetools.GetAndMaybeCacheTreeFromRootDirectoryDigest(ctx, cas, root, fc, bs)
	assert.NoError(t, err)
	assert.Equal(t, tree.GetRoot(), a.Directory)
	checkDirectoriesMatch(t, []string{"b", "c", "d"}, tree.GetChildren())
	assert.Equal(t, 1, fc.readCount)
	assert.Equal(t, 2, fc.writeCount)
	assert.Equal(t, 2, bs.readCount)
}

type getTreeStreamer struct {
	data *repb.GetTreeResponse
	err  error
	done bool
}

// CloseSend implements remote_execution.ContentAddressableStorage_GetTreeClient.
func (g *getTreeStreamer) CloseSend() error {
	panic("unimplemented")
}

// Context implements remote_execution.ContentAddressableStorage_GetTreeClient.
func (g *getTreeStreamer) Context() context.Context {
	panic("unimplemented")
}

// Header implements remote_execution.ContentAddressableStorage_GetTreeClient.
func (g *getTreeStreamer) Header() (metadata.MD, error) {
	panic("unimplemented")
}

// Recv implements remote_execution.ContentAddressableStorage_GetTreeClient.
func (g *getTreeStreamer) Recv() (*repb.GetTreeResponse, error) {
	if g.err != nil {
		return nil, g.err
	} else if g.done {
		return nil, io.EOF
	} else {
		g.done = true
		return g.data, nil
	}
}

// RecvMsg implements remote_execution.ContentAddressableStorage_GetTreeClient.
func (g *getTreeStreamer) RecvMsg(m any) error {
	panic("unimplemented")
}

// SendMsg implements remote_execution.ContentAddressableStorage_GetTreeClient.
func (g *getTreeStreamer) SendMsg(m any) error {
	panic("unimplemented")
}

// Trailer implements remote_execution.ContentAddressableStorage_GetTreeClient.
func (g *getTreeStreamer) Trailer() metadata.MD {
	panic("unimplemented")
}

type fakeCasClient struct {
	treeDigest *repb.Digest
	response   *repb.GetTreeResponse
}

// BatchReadBlobs implements remote_execution.ContentAddressableStorageClient.
func (f *fakeCasClient) BatchReadBlobs(ctx context.Context, in *repb.BatchReadBlobsRequest, opts ...grpc.CallOption) (*repb.BatchReadBlobsResponse, error) {
	panic("unimplemented")
}

// BatchUpdateBlobs implements remote_execution.ContentAddressableStorageClient.
func (f *fakeCasClient) BatchUpdateBlobs(ctx context.Context, in *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
	panic("unimplemented")
}

// FindMissingBlobs implements remote_execution.ContentAddressableStorageClient.
func (f *fakeCasClient) FindMissingBlobs(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
	panic("unimplemented")
}

// GetTree implements remote_execution.ContentAddressableStorageClient.
func (f *fakeCasClient) GetTree(ctx context.Context, in *repb.GetTreeRequest, opts ...grpc.CallOption) (repb.ContentAddressableStorage_GetTreeClient, error) {
	if f.treeDigest.GetHash() != in.GetRootDigest().GetHash() || f.treeDigest.GetSizeBytes() != in.GetRootDigest().GetSizeBytes() {
		return &getTreeStreamer{
			err: status.NotFoundErrorf("not found: %s", f.treeDigest),
		}, nil
	}
	return &getTreeStreamer{
		data: f.response,
	}, nil
}

type fakeFilecache struct {
	mu         sync.Mutex
	files      map[string][]byte
	readCount  int
	writeCount int
}

func toFileNode(r *rspb.ResourceName) *repb.FileNode {
	return &repb.FileNode{
		Digest:       r.GetDigest(),
		IsExecutable: false,
	}
}

func key(f *repb.FileNode) string {
	return fmt.Sprintf("%s/%d/%t", f.GetDigest().GetHash(), f.GetDigest().GetSizeBytes(), f.GetIsExecutable())
}

func (fc *fakeFilecache) FastLinkFile(ctx context.Context, f *repb.FileNode, outputPath string) bool {
	panic("unimplemented")
}

func (fc *fakeFilecache) DeleteFile(ctx context.Context, f *repb.FileNode) bool {
	panic("unimplemented")
}

func (fc *fakeFilecache) AddFile(ctx context.Context, f *repb.FileNode, existingFilePath string) error {
	panic("unimplemented")
}

func (fc *fakeFilecache) ContainsFile(ctx context.Context, node *repb.FileNode) bool {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	_, ok := fc.files[key(node)]
	return ok
}

func (fc *fakeFilecache) Open(ctx context.Context, f *repb.FileNode) (*os.File, error) {
	panic("unimplemented")
}

func (fc *fakeFilecache) WaitForDirectoryScanToComplete() {
	panic("unimplemented")
}

func (fc *fakeFilecache) Read(ctx context.Context, node *repb.FileNode) ([]byte, error) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	data, ok := fc.files[key(node)]
	if !ok {
		return nil, status.NotFoundErrorf("not found: %s", key(node))
	}
	fc.readCount++
	return data, nil
}

func (fc *fakeFilecache) Write(ctx context.Context, node *repb.FileNode, b []byte) (n int, err error) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.writeCount++
	fc.files[key(node)] = b
	return len(b), nil
}

func (fc *fakeFilecache) Writer(ctx context.Context, node *repb.FileNode, digestFunction repb.DigestFunction_Value) (interfaces.CommittedWriteCloser, error) {
	panic("unimplemented")
}

func (fc *fakeFilecache) TempDir() string {
	panic("unimplemented")
}

type fakeBytestreamClient struct {
	mu        *sync.Mutex
	data      map[string][]byte
	readCount int
}

// QueryWriteStatus implements bytestream.ByteStreamClient.
func (f *fakeBytestreamClient) QueryWriteStatus(ctx context.Context, in *bspb.QueryWriteStatusRequest, opts ...grpc.CallOption) (*bspb.QueryWriteStatusResponse, error) {
	panic("unimplemented")
}

// Read implements bytestream.ByteStreamClient.
func (f *fakeBytestreamClient) Read(ctx context.Context, in *bspb.ReadRequest, opts ...grpc.CallOption) (bspb.ByteStream_ReadClient, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readCount++
	return &bsReadStreamer{
		mu:   f.mu,
		req:  in,
		data: f.data,
	}, nil
}

// Write implements bytestream.ByteStreamClient.
func (f *fakeBytestreamClient) Write(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
	panic("unimplemented")
}

type bsReadStreamer struct {
	mu   *sync.Mutex
	req  *bspb.ReadRequest
	data map[string][]byte
	done bool
}

// CloseSend implements bytestream.ByteStream_ReadClient.
func (b *bsReadStreamer) CloseSend() error {
	panic("unimplemented")
}

// Context implements bytestream.ByteStream_ReadClient.
func (b *bsReadStreamer) Context() context.Context {
	panic("unimplemented")
}

// Header implements bytestream.ByteStream_ReadClient.
func (b *bsReadStreamer) Header() (metadata.MD, error) {
	panic("unimplemented")
}

// Recv implements bytestream.ByteStream_ReadClient.
func (b *bsReadStreamer) Recv() (*bspb.ReadResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.data[b.req.GetResourceName()]
	if !ok {
		return nil, status.NotFoundErrorf("not found: %s", b.req.GetResourceName())
	}
	if b.done {
		return nil, io.EOF
	} else {
		b.done = true
		return &bspb.ReadResponse{Data: data}, nil
	}
}

// RecvMsg implements bytestream.ByteStream_ReadClient.
func (b *bsReadStreamer) RecvMsg(m any) error {
	panic("unimplemented")
}

// SendMsg implements bytestream.ByteStream_ReadClient.
func (b *bsReadStreamer) SendMsg(m any) error {
	panic("unimplemented")
}

// Trailer implements bytestream.ByteStream_ReadClient.
func (b *bsReadStreamer) Trailer() metadata.MD {
	panic("unimplemented")
}

func TestUploadReaderAndGetBlob(t *testing.T) {
	for _, tc := range []struct {
		name string

		inputSize  int64
		uploadSize int64
		getSize    int64

		expectUploadError bool
		expectGetError    bool
		expectedGetSize   int64
	}{
		{
			name: "simple upload and get",

			inputSize:  128,
			uploadSize: 128,
			getSize:    128,

			expectUploadError: false,
			expectGetError:    false,
			expectedGetSize:   128,
		},
		{
			name: "upload with incorrect size fails",

			inputSize:  128,
			uploadSize: 120,
			getSize:    128,

			expectUploadError: true,
			expectGetError:    true,
			expectedGetSize:   128,
		},
		{
			name: "get with incorrect size still succeeds",

			inputSize:  128,
			uploadSize: 128,
			getSize:    120,

			expectUploadError: false,
			expectGetError:    false,
			expectedGetSize:   128,
		},
		{
			name: "writing large payload succeeds",

			inputSize:  2 * 1024 * 1024,
			uploadSize: 2 * 1024 * 1024,
			getSize:    2 * 1024 * 1024,

			expectUploadError: false,
			expectGetError:    false,
			expectedGetSize:   2 * 1024 * 1024,
		},
	} {
		for _, useZstd := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/use_zstd_%t", tc.name, useZstd), func(t *testing.T) {
				te := testenv.GetTestEnv(t)
				_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
				testcache.Setup(t, te, localGRPClis)
				go runServer()

				rn, buf := testdigest.RandomCASResourceBuf(t, tc.inputSize)

				ctx := context.Background()
				{
					uploadDigest := &repb.Digest{
						Hash:      rn.Digest.Hash,
						SizeBytes: tc.uploadSize,
					}
					upRN := digest.NewCASResourceName(uploadDigest, rn.InstanceName, rn.DigestFunction)
					if useZstd {
						upRN.SetCompressor(repb.Compressor_ZSTD)
					}
					d, uploadedBytes, err := cachetools.UploadFromReader(ctx, te.GetByteStreamClient(), upRN, bytes.NewReader(buf))
					if tc.expectUploadError {
						require.Error(t, err)
						require.Nil(t, d)
						require.Zero(t, uploadedBytes)
					} else {
						require.NoError(t, err)
						require.NotNil(t, d)
						require.Empty(t, cmp.Diff(upRN.GetDigest(), d, protocmp.Transform()))
						require.Greater(t, uploadedBytes, int64(0))
						require.LessOrEqual(t, uploadedBytes, tc.uploadSize)
					}
				}

				{
					getDigest := &repb.Digest{
						Hash:      rn.Digest.Hash,
						SizeBytes: tc.uploadSize,
					}
					getRN := digest.NewCASResourceName(getDigest, rn.InstanceName, rn.DigestFunction)
					if useZstd {
						getRN.SetCompressor(repb.Compressor_ZSTD)
					}
					out := &bytes.Buffer{}
					err := cachetools.GetBlob(ctx, te.GetByteStreamClient(), getRN, out)
					if tc.expectGetError {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
						require.Equal(t, tc.expectedGetSize, int64(out.Len()))
						require.Empty(t, cmp.Diff(buf[:9], out.Bytes()[:9]))
						require.Empty(t, cmp.Diff(buf[len(buf)-9:], out.Bytes()[out.Len()-9:]))
					}
				}
			})
		}
	}
}

func TestUploadReader_BlobExists(t *testing.T) {
	for _, useZstd := range []bool{false, true} {
		t.Run(fmt.Sprintf("use_zstd_%t", useZstd), func(t *testing.T) {
			te := testenv.GetTestEnv(t)
			_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
			testcache.Setup(t, te, localGRPClis)
			go runServer()

			uploadSize := int64(2 * 1024 * 1024)
			rn, buf := testdigest.RandomCASResourceBuf(t, uploadSize)
			casRN := digest.NewCASResourceName(rn.Digest, rn.InstanceName, rn.DigestFunction)
			if useZstd {
				casRN.SetCompressor(repb.Compressor_ZSTD)
			}

			ctx := context.Background()
			{
				d, uploadedBytes, err := cachetools.UploadFromReader(ctx, te.GetByteStreamClient(), casRN, bytes.NewReader(buf))
				require.NoError(t, err)
				require.NotNil(t, d)
				require.Empty(t, cmp.Diff(casRN.GetDigest(), d, protocmp.Transform()))
				require.Greater(t, uploadedBytes, int64(0))
				require.LessOrEqual(t, uploadedBytes, uploadSize)
			}

			{
				out := &bytes.Buffer{}
				err := cachetools.GetBlob(ctx, te.GetByteStreamClient(), casRN, out)

				require.NoError(t, err)
				require.Equal(t, uploadSize, int64(out.Len()))
				require.Empty(t, cmp.Diff(buf[:9], out.Bytes()[:9]))
				require.Empty(t, cmp.Diff(buf[len(buf)-9:], out.Bytes()[out.Len()-9:]))
			}

			// Second upload succeeds
			{
				d, _, err := cachetools.UploadFromReader(ctx, te.GetByteStreamClient(), casRN, bytes.NewReader(buf))
				require.NoError(t, err)
				require.NotNil(t, d)
				require.Empty(t, cmp.Diff(casRN.GetDigest(), d, protocmp.Transform()))
			}

			// The blob is still available in the CAS
			{
				out := &bytes.Buffer{}
				err := cachetools.GetBlob(ctx, te.GetByteStreamClient(), casRN, out)

				require.NoError(t, err)
				require.Equal(t, uploadSize, int64(out.Len()))
				require.Empty(t, cmp.Diff(buf[:9], out.Bytes()[:9]))
				require.Empty(t, cmp.Diff(buf[len(buf)-9:], out.Bytes()[out.Len()-9:]))
			}
		})
	}
}

func TestConcurrentMutationDuringUpload(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int64
	}{
		{
			name: "payload greater than gRPC max size",
			size: rpcutil.GRPCMaxSizeBytes + 1,
		},
		{
			name: "payload less than gRPC max size",
			size: 16,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := testenv.GetTestEnv(t)
			_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
			testcache.Setup(t, te, localGRPClis)
			go runServer()

			b := make([]byte, tc.size)
			df := repb.DigestFunction_SHA256
			d, err := digest.Compute(bytes.NewReader(b), df)
			require.NoError(t, err)
			// Overwrite the first byte after we already computed the digest,
			// simulating a concurrent mutation.
			b[0] = 'x'
			ctx := context.Background()
			ul := cachetools.NewBatchCASUploader(ctx, te, "", df)
			_ = ul.Upload(d, cachetools.NewBytesReadSeekCloser(b))
			err = ul.Wait()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "concurrent mutation detected")
			assert.True(t, status.IsDataLossError(err), "want DataLossError, got %+#v", err)
		})
	}
}

func TestUploadWriterAndGetBlob(t *testing.T) {
	for _, tc := range []struct {
		name string

		inputSize  int64
		uploadSize int64
		getSize    int64

		expectUploadError bool
		expectGetError    bool
		expectedGetSize   int64
	}{
		{
			name: "simple upload and get",

			inputSize:  128,
			uploadSize: 128,
			getSize:    128,

			expectUploadError: false,
			expectGetError:    false,
			expectedGetSize:   128,
		},
		{
			name: "upload with incorrect size fails",

			inputSize:  128,
			uploadSize: 120,
			getSize:    128,

			expectUploadError: true,
			expectGetError:    true,
			expectedGetSize:   128,
		},
		{
			name: "get with incorrect size still succeeds",

			inputSize:  128,
			uploadSize: 128,
			getSize:    120,

			expectUploadError: false,
			expectGetError:    false,
			expectedGetSize:   128,
		},
		{
			name: "writing large payload succeeds",

			inputSize:  2 * 1024 * 1024,
			uploadSize: 2 * 1024 * 1024,
			getSize:    2 * 1024 * 1024,

			expectUploadError: false,
			expectGetError:    false,
			expectedGetSize:   2 * 1024 * 1024,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := testenv.GetTestEnv(t)
			_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
			testcache.Setup(t, te, localGRPClis)
			go runServer()

			rn, buf := testdigest.RandomCASResourceBuf(t, tc.inputSize)

			ctx := context.Background()
			{
				uploadDigest := &repb.Digest{
					Hash:      rn.Digest.Hash,
					SizeBytes: tc.uploadSize,
				}
				upRN := digest.NewCASResourceName(uploadDigest, rn.InstanceName, rn.DigestFunction)

				uw, err := cachetools.NewUploadWriter(ctx, te.GetByteStreamClient(), upRN)
				require.NoError(t, err)

				written, err := io.Copy(uw, bytes.NewReader(buf))
				require.NoError(t, err)
				require.Greater(t, written, int64(0))

				err = uw.Commit()
				if tc.expectUploadError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.LessOrEqual(t, written, tc.uploadSize)
				}

				err = uw.Close()
				require.NoError(t, err)
			}

			{
				getDigest := &repb.Digest{
					Hash:      rn.Digest.Hash,
					SizeBytes: tc.uploadSize,
				}
				getRN := digest.NewCASResourceName(getDigest, rn.InstanceName, rn.DigestFunction)
				out := &bytes.Buffer{}
				err := cachetools.GetBlob(ctx, te.GetByteStreamClient(), getRN, out)
				if tc.expectGetError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, tc.expectedGetSize, int64(out.Len()))
					require.Empty(t, cmp.Diff(buf[:9], out.Bytes()[:9]))
					require.Empty(t, cmp.Diff(buf[len(buf)-9:], out.Bytes()[out.Len()-9:]))
				}
			}
		})
	}
}

func TestUploadWriter_BlobExists(t *testing.T) {
	te := testenv.GetTestEnv(t)
	_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, localGRPClis)
	go runServer()

	uploadSize := int64(2 * 1024 * 1024)
	rn, buf := testdigest.RandomCASResourceBuf(t, uploadSize)
	casRN := digest.NewCASResourceName(rn.Digest, rn.InstanceName, rn.DigestFunction)

	ctx := context.Background()
	{
		uw, err := cachetools.NewUploadWriter(ctx, te.GetByteStreamClient(), casRN)
		require.NoError(t, err)

		n, err := uw.Write(buf)
		require.NoError(t, err)
		require.Equal(t, uploadSize, int64(n))

		err = uw.Commit()
		require.NoError(t, err)

		err = uw.Close()
		require.NoError(t, err)
	}

	{
		out := &bytes.Buffer{}
		err := cachetools.GetBlob(ctx, te.GetByteStreamClient(), casRN, out)

		require.NoError(t, err)
		require.Equal(t, uploadSize, int64(out.Len()))
		require.Empty(t, cmp.Diff(buf[:9], out.Bytes()[:9]))
		require.Empty(t, cmp.Diff(buf[len(buf)-9:], out.Bytes()[out.Len()-9:]))
	}

	// Second upload succeeds
	{
		uw, err := cachetools.NewUploadWriter(ctx, te.GetByteStreamClient(), casRN)
		require.NoError(t, err)

		n, err := uw.Write(buf)
		require.NoError(t, err)
		require.Equal(t, uploadSize, int64(n))

		err = uw.Commit()
		require.NoError(t, err)

		err = uw.Close()
		require.NoError(t, err)
	}

	// The blob is still available in the CAS
	{
		out := &bytes.Buffer{}
		err := cachetools.GetBlob(ctx, te.GetByteStreamClient(), casRN, out)

		require.NoError(t, err)
		require.Equal(t, uploadSize, int64(out.Len()))
		require.Empty(t, cmp.Diff(buf[:9], out.Bytes()[:9]))
		require.Empty(t, cmp.Diff(buf[len(buf)-9:], out.Bytes()[out.Len()-9:]))
	}
}

func TestUploadWriter_NoWritesAfterCommit(t *testing.T) {
	rn, buf := testdigest.RandomCASResourceBuf(t, 2*1024*1024)
	te := testenv.GetTestEnv(t)
	_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, localGRPClis)
	go runServer()
	ctx := context.Background()
	casrn := digest.NewCASResourceName(rn.Digest, rn.InstanceName, rn.DigestFunction)

	uw, err := cachetools.NewUploadWriter(ctx, te.GetByteStreamClient(), casrn)
	require.NoError(t, err)
	written, err := uw.Write(buf)
	require.NoError(t, err)
	require.Equal(t, len(buf), written)

	// The blob is not available before commit
	{
		out := &bytes.Buffer{}
		err = cachetools.GetBlob(ctx, te.GetByteStreamClient(), casrn, out)
		require.Error(t, err)
	}

	err = uw.Commit()
	require.NoError(t, err)

	// The blob is available post commit
	{
		out := &bytes.Buffer{}
		err = cachetools.GetBlob(ctx, te.GetByteStreamClient(), casrn, out)
		require.NoError(t, err)
		require.Equal(t, len(buf), out.Len())
		require.Empty(t, cmp.Diff(buf[0:9], out.Bytes()[0:9]))
		require.Empty(t, cmp.Diff(buf[len(buf)-9:], out.Bytes()[len(buf)-9:]))
	}

	// Cannot Write after commit
	written, err = uw.Write(buf)
	require.Error(t, err)
	require.Equal(t, 0, written)

	// Committing after commit is a no-op
	err = uw.Commit()
	require.NoError(t, err)

	err = uw.Close()
	require.NoError(t, err)

	// Cannot Write after close
	written, err = uw.Write(buf)
	require.Error(t, err)
	require.Equal(t, 0, written)

	// Cannot close again after close
	err = uw.Close()
	require.Error(t, err)
}

func TestUploadWriter_CanCloseBeforeCommit(t *testing.T) {
	rn, buf := testdigest.RandomCASResourceBuf(t, 2*1024*1024)
	te := testenv.GetTestEnv(t)
	_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, localGRPClis)
	go runServer()
	ctx := context.Background()
	casrn := digest.NewCASResourceName(rn.Digest, rn.InstanceName, rn.DigestFunction)

	uw, err := cachetools.NewUploadWriter(ctx, te.GetByteStreamClient(), casrn)
	require.NoError(t, err)
	written, err := uw.Write(buf)
	require.NoError(t, err)
	require.Equal(t, len(buf), written)

	err = uw.Close()
	require.NoError(t, err)

	// Blob is not available since we did not commit
	out := &bytes.Buffer{}
	err = cachetools.GetBlob(ctx, te.GetByteStreamClient(), casrn, out)
	require.Error(t, err)
}

func TestUploadWriter_CancelContext(t *testing.T) {
	te := testenv.GetTestEnv(t)
	_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, localGRPClis)
	go runServer()

	half := 1 * 1024 * 1024
	full := 2 * 1024 * 1024
	rn, buf := testdigest.RandomCASResourceBuf(t, int64(full))
	casRN := digest.NewCASResourceName(rn.Digest, rn.InstanceName, rn.DigestFunction)
	bsClient := te.GetByteStreamClient()
	ctx, cancel := context.WithCancel(context.Background())
	uw, err := cachetools.NewUploadWriter(ctx, bsClient, casRN)
	require.NoError(t, err)

	written, err := uw.Write(buf[:half])
	require.NoError(t, err)
	require.Equal(t, half, written)

	cancel()

	_, err = uw.Write(buf[half:])
	require.Error(t, err)
	require.ErrorContains(t, err, "context canceled")
}
