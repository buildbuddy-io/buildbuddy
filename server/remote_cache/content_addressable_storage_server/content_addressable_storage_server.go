package content_addressable_storage_server

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/namespace"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gstatus "google.golang.org/grpc/status"
)

const gRPCMaxSize = int64(4194304 - 2000)

type ContentAddressableStorageServer struct {
	env   environment.Env
	cache interfaces.Cache
}

func NewContentAddressableStorageServer(env environment.Env) (*ContentAddressableStorageServer, error) {
	cache := env.GetCache()
	if cache == nil {
		return nil, fmt.Errorf("A cache is required to enable the ContentAddressableStorageServer")
	}
	return &ContentAddressableStorageServer{
		env:   env,
		cache: cache,
	}, nil
}

func (s *ContentAddressableStorageServer) getCache(ctx context.Context, instanceName string) (interfaces.Cache, error) {
	return namespace.CASCache(ctx, s.cache, instanceName)
}

// Determine if blobs are present in the CAS.
//
// Clients can use this API before uploading blobs to determine which ones are
// already present in the CAS and do not need to be uploaded again.
//
// There are no method-specific errors.
func (s *ContentAddressableStorageServer) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	rsp := &repb.FindMissingBlobsResponse{}
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}
	cache, err := s.getCache(ctx, req.GetInstanceName())
	if err != nil {
		return nil, err
	}
	digestsToLookup := make([]*repb.Digest, 0, len(req.GetBlobDigests()))
	for _, d := range req.GetBlobDigests() {
		if d.GetHash() == digest.EmptySha256 {
			continue
		}
		if d.GetHash() == digest.EmptyHash {
			continue
		}
		digestsToLookup = append(digestsToLookup, d)
	}
	foundMap, err := cache.ContainsMulti(ctx, digestsToLookup)
	if err != nil {
		return nil, err
	}
	for _, d := range digestsToLookup {
		found, ok := foundMap[d]
		if !ok || !found {
			rsp.MissingBlobDigests = append(rsp.MissingBlobDigests, d)
		}
	}
	return rsp, nil
}

// Upload many blobs at once.
//
// The server may enforce a limit of the combined total size of blobs
// to be uploaded using this API. This limit may be obtained using the
// [Capabilities][build.bazel.remote.execution.v2.Capabilities] API.
// Requests exceeding the limit should either be split into smaller
// chunks or uploaded using the
// [ByteStream API][google.bytestream.ByteStream], as appropriate.
//
// This request is equivalent to calling a Bytestream `Write` request
// on each individual blob, in parallel. The requests may succeed or fail
// independently.
//
// Errors:
//
// * `INVALID_ARGUMENT`: The client attempted to upload more than the
//   server supported limit.
//
// Individual requests may return the following errors, additionally:
//
// * `RESOURCE_EXHAUSTED`: There is insufficient disk quota to store the blob.
// * `INVALID_ARGUMENT`: The
// [Digest][build.bazel.remote.execution.v2.Digest] does not match the
// provided data.
func (s *ContentAddressableStorageServer) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	rsp := &repb.BatchUpdateBlobsResponse{}
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}

	canWrite, err := capabilities.IsGranted(ctx, s.env, akpb.ApiKey_CACHE_WRITE_CAPABILITY)
	if err != nil {
		return nil, err
	}
	if !canWrite {
		// For read-only API keys, pretend the write succeeded.
		for _, uploadRequest := range req.Requests {
			rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
				Digest: uploadRequest.GetDigest(),
				Status: &statuspb.Status{Code: int32(codes.OK)},
			})
		}
		return rsp, nil
	}

	cache, err := s.getCache(ctx, req.GetInstanceName())
	if err != nil {
		return nil, err
	}
	rsp.Responses = make([]*repb.BatchUpdateBlobsResponse_Response, 0, len(req.Requests))

	ht := hit_tracker.NewHitTracker(ctx, s.env, false)
	kvs := make(map[*repb.Digest][]byte, len(req.Requests))
	for _, uploadRequest := range req.Requests {
		uploadDigest := uploadRequest.GetDigest()
		_, err := digest.Validate(uploadDigest)
		if err != nil {
			return nil, err
		}
		uploadTracker := ht.TrackUpload(uploadDigest)
		// defers are preetty cheap: https://tpaschalis.github.io/defer-internals/
		// so doing 100-1000 or so in this loop is fine.
		defer uploadTracker.Close()

		if uploadDigest.GetHash() == digest.EmptySha256 {
			rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
				Digest: uploadDigest,
				Status: &statuspb.Status{Code: int32(codes.OK)},
			})
			// Skip putting this in the kv map -- we don't want to attempt to
			// write empty files.
			continue
		}
		checksum := sha256.New()
		checksum.Write(uploadRequest.GetData())
		computedDigest := fmt.Sprintf("%x", checksum.Sum(nil))
		if computedDigest != uploadDigest.GetHash() {
			err := status.DataLossErrorf("Uploaded bytes checksum (%q) did not match digest (%q).", computedDigest, uploadDigest.GetHash())
			rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
				Digest: uploadDigest,
				Status: gstatus.Convert(err).Proto(),
			})
			continue

		}
		if int64(len(uploadRequest.GetData())) != uploadDigest.GetSizeBytes() {
			err := status.DataLossErrorf("%d bytes were uploaded but %d were expected.", len(uploadRequest.GetData()), uploadDigest.GetSizeBytes())
			rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
				Digest: uploadDigest,
				Status: gstatus.Convert(err).Proto(),
			})
			continue
		}
		kvs[uploadDigest] = uploadRequest.GetData()
	}

	if err := cache.SetMulti(ctx, kvs); err != nil {
		return nil, err
	}
	for uploadDigest := range kvs {
		rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
			Digest: uploadDigest,
			Status: &statuspb.Status{Code: int32(codes.OK)},
		})
	}
	return rsp, nil
}

// Download many blobs at once.
//
// The server may enforce a limit of the combined total size of blobs
// to be downloaded using this API. This limit may be obtained using the
// [Capabilities][build.bazel.remote.execution.v2.Capabilities] API.
// Requests exceeding the limit should either be split into smaller
// chunks or downloaded using the
// [ByteStream API][google.bytestream.ByteStream], as appropriate.
//
// This request is equivalent to calling a Bytestream `Read` request
// on each individual blob, in parallel. The requests may succeed or fail
// independently.
//
// Errors:
//
// * `INVALID_ARGUMENT`: The client attempted to read more than the
//   server supported limit.
//
// Every error on individual read will be returned in the corresponding digest
// status.
func (s *ContentAddressableStorageServer) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	rsp := &repb.BatchReadBlobsResponse{}
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}
	cache, err := s.getCache(ctx, req.GetInstanceName())
	if err != nil {
		return nil, err
	}
	cacheRequest := make([]*repb.Digest, 0, len(req.Digests))
	rsp.Responses = make([]*repb.BatchReadBlobsResponse_Response, 0, len(req.Digests))
	ht := hit_tracker.NewHitTracker(ctx, s.env, false)
	for _, readDigest := range req.GetDigests() {
		_, err := digest.Validate(readDigest)
		if err != nil {
			return nil, err
		}
		downloadTracker := ht.TrackDownload(readDigest)
		// defers are preetty cheap: https://tpaschalis.github.io/defer-internals/
		// so doing 100-1000 or so in this loop is fine.
		defer downloadTracker.Close()
		if readDigest.GetHash() != digest.EmptySha256 {
			cacheRequest = append(cacheRequest, readDigest)
		}
	}
	cacheRsp, err := cache.GetMulti(ctx, cacheRequest)
	for _, d := range req.GetDigests() {
		if d.GetHash() == digest.EmptySha256 {
			rsp.Responses = append(rsp.Responses, &repb.BatchReadBlobsResponse_Response{
				Digest: d,
				Status: &statuspb.Status{Code: int32(codes.OK)},
			})
			continue
		}

		data, ok := cacheRsp[d]
		blobRsp := &repb.BatchReadBlobsResponse_Response{
			Digest: d,
			Data:   data,
		}
		if !ok || os.IsNotExist(err) {
			blobRsp.Status = &statuspb.Status{Code: int32(codes.NotFound)}
		} else if d.GetSizeBytes() != int64(len(data)) {
			log.Debugf("Digest %s, but data len: %d", d, len(data))
			blobRsp.Status = &statuspb.Status{Code: int32(codes.NotFound)}
		} else if err != nil {
			blobRsp.Status = &statuspb.Status{Code: int32(codes.Internal)}
		} else {
			blobRsp.Status = &statuspb.Status{Code: int32(codes.OK)}
		}
		rsp.Responses = append(rsp.Responses, blobRsp)
	}
	return rsp, nil
}

type dirStack struct {
	dirSizes map[*repb.Directory]int64
	dirs     []*repb.Directory
	lock     sync.Mutex
}

func NewDirStack(token string) (*dirStack, error) {
	newDirStack := &dirStack{
		lock:     sync.Mutex{},
		dirs:     make([]*repb.Directory, 0),
		dirSizes: make(map[*repb.Directory]int64, 0),
	}
	if token != "" {
		tree := &repb.TreeToken{}
		protoBytes, err := base64.StdEncoding.DecodeString(token)
		if err != nil {
			return nil, err
		}
		if err := proto.Unmarshal(protoBytes, tree); err != nil {
			return nil, err
		}
		for _, sizedDirectory := range tree.GetSizedDirectories() {
			sd := sizedDirectory
			newDirStack.dirs = append(newDirStack.dirs, sd.GetDirectory())
			newDirStack.dirSizes[sd.GetDirectory()] = sizedDirectory.GetSizeBytes()
		}
	}
	return newDirStack, nil
}
func (d *dirStack) Empty() bool {
	d.lock.Lock()
	empty := len(d.dirs) == 0
	d.lock.Unlock()
	return empty
}

func (d *dirStack) Push(dir *repb.Directory, sizeBytes int64) {
	d.lock.Lock()
	d.dirs = append(d.dirs, dir)
	d.dirSizes[dir] = sizeBytes
	d.lock.Unlock()
}
func (d *dirStack) Pop() (*repb.Directory, int64) {
	d.lock.Lock()
	dirsLength := len(d.dirs)
	if dirsLength == 0 {
		return nil, 0
	}
	lastElementIndex := dirsLength - 1
	result := d.dirs[lastElementIndex]
	d.dirs = d.dirs[:lastElementIndex]
	sizeBytes, ok := d.dirSizes[result]
	if !ok {
		return nil, 0
	}
	d.lock.Unlock()
	return result, sizeBytes
}
func (d *dirStack) SerializeToToken() (string, error) {
	tree := &repb.TreeToken{}
	for _, dir := range d.dirs {
		sizeBytes, ok := d.dirSizes[dir]
		if !ok {
			return "", status.InternalError("Unable to serialize tree token")
		}
		tree.SizedDirectories = append(tree.SizedDirectories, &repb.SizedDirectory{
			Directory: dir,
			SizeBytes: sizeBytes,
		})
	}
	protoBytes, err := proto.Marshal(tree)
	if err != nil {
		return "", err
	}

	token := base64.StdEncoding.EncodeToString(protoBytes)
	return token, nil
}

func (s *ContentAddressableStorageServer) fetchDir(ctx context.Context, cache interfaces.Cache, reqDigest *repb.Digest) (*repb.Directory, error) {
	_, err := digest.Validate(reqDigest)
	if err != nil {
		return nil, err
	}
	// Fetch the "Directory" object which enumerates all the blobs in the directory
	blob, err := cache.Get(ctx, reqDigest)
	if err != nil {
		return nil, err
	}

	dir := &repb.Directory{}
	if err := proto.Unmarshal(blob, dir); err != nil {
		return nil, err
	}
	return dir, nil
}

type DirectoryWithDigest struct {
	Directory *repb.Directory
	Digest    *repb.Digest
}

func (s *ContentAddressableStorageServer) fetchDirectory(ctx context.Context, cache interfaces.Cache, dir *repb.Directory) ([]*DirectoryWithDigest, error) {
	if len(dir.Directories) == 0 {
		return nil, nil
	}
	subdirDigests := make([]*repb.Digest, 0, len(dir.Directories))
	for _, dirNode := range dir.Directories {
		d := dirNode.GetDigest()
		if d.GetHash() == digest.EmptySha256 {
			continue
		}
		subdirDigests = append(subdirDigests, d)
	}
	rspMap, err := cache.GetMulti(ctx, subdirDigests)
	if err != nil {
		return nil, err
	}
	children := make([]*DirectoryWithDigest, 0, len(subdirDigests))
	for _, d := range subdirDigests {
		blob, ok := rspMap[d]
		if !ok {
			return nil, status.NotFoundErrorf("Digest %s not found in cache.", d.GetHash())
		}
		subDir := &repb.Directory{}
		if err := proto.Unmarshal(blob, subDir); err != nil {
			return nil, err
		}
		children = append(children, &DirectoryWithDigest{Directory: subDir, Digest: d})
	}
	return children, nil
}

// GetTree fetches the entire directory tree rooted at a node.
//
// This request must be targeted at a
// [Directory][build.bazel.remote.execution.v2.Directory] stored in the
// [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage]
// (CAS). The server will enumerate the `Directory` tree recursively and
// return every node descended from the root.
//
// The exact traversal order is unspecified and, unless retrieving subsequent
// pages from an earlier request, is not guaranteed to be stable across
// multiple invocations of `GetTree`.
//
// If part of the tree is missing from the CAS, the server will return the
// portion present and omit the rest.
//
// GetTree is called by the remote executors to download the list of all
// directories that are inputs to an action. For some actions with tens of
// thousands of inputs (think of a node_modules directory at a big company),
// this can be a significant number of directories. To do this as fast as
// possible, GetTree recursively walks the directory tree, spawning new
// goroutines on each branch. When downloading all of the directories within
// a directory, GetMulti is used to make one parallel request to the cache.
//
// Errors:
//
// * `NOT_FOUND`: The requested tree root is not present in the CAS.
func (s *ContentAddressableStorageServer) GetTree(req *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	rpcStart := time.Now()
	if req.RootDigest == nil {
		return fmt.Errorf("RootDigest is required to GetTree")
	}
	if req.GetRootDigest().GetHash() == digest.EmptySha256 {
		return nil
	}

	ctx, err := prefix.AttachUserPrefixToContext(stream.Context(), s.env)
	if err != nil {
		return err
	}
	cache, err := s.getCache(ctx, req.GetInstanceName())
	if err != nil {
		return err
	}
	rootDir, err := s.fetchDir(ctx, cache, req.GetRootDigest())
	if err != nil {
		return err
	}

	mu := &sync.Mutex{}
	rsp := &repb.GetTreeResponse{}
	rspSizeBytes := int64(0)
	dirCount := 0
	fetchCount := 0
	fetchDuration := time.Duration(0)

	finishDir := func(dirWithDigest *DirectoryWithDigest) error {
		mu.Lock()
		defer mu.Unlock()

		dir := dirWithDigest.Directory
		d := dirWithDigest.Digest

		if rspSizeBytes+d.GetSizeBytes() > gRPCMaxSize {
			if err := stream.Send(rsp); err != nil {
				return err
			}
			rsp = &repb.GetTreeResponse{}
			rspSizeBytes = 0
		}
		rspSizeBytes += d.GetSizeBytes()
		rsp.Directories = append(rsp.Directories, dir)
		dirCount += 1
		return nil
	}

	eg, egCtx := errgroup.WithContext(ctx)
	var fetch func(dirWithDigest *DirectoryWithDigest) error
	fetch = func(dirWithDigest *DirectoryWithDigest) error {
		if err := finishDir(dirWithDigest); err != nil {
			return err
		}
		if len(dirWithDigest.Directory.Directories) == 0 {
			return nil
		}

		start := time.Now()
		children, err := s.fetchDirectory(egCtx, cache, dirWithDigest.Directory)
		if err != nil {
			return err
		}
		mu.Lock()
		fetchDuration += time.Since(start)
		fetchCount += 1
		mu.Unlock()
		for _, childDirWithDigest := range children {
			childDirWithDigest := childDirWithDigest
			eg.Go(func() error {
				return fetch(childDirWithDigest)
			})
		}
		return nil
	}

	eg.Go(func() error {
		return fetch(&DirectoryWithDigest{
			Directory: rootDir,
			Digest:    req.GetRootDigest(),
		})
	})

	if err := eg.Wait(); err != nil {
		return err
	}
	log.Debugf("GetTree fetched %d dirs from cache across %d calls in cumulative %s (total time: %s)", dirCount, fetchCount, fetchDuration, time.Since(rpcStart))
	if rspSizeBytes > 0 {
		return stream.Send(rsp)
	}
	return nil
}
