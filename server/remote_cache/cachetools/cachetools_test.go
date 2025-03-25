package cachetools_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

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
		dlString, _ := rn.DownloadString()
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
