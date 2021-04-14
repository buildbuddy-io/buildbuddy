package content_addressable_storage_server

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/namespace"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
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

func (s *ContentAddressableStorageServer) getCache(instanceName string) interfaces.Cache {
	return namespace.CASCache(s.cache, instanceName)
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
	cache := s.getCache(req.GetInstanceName())
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

	cache := s.getCache(req.GetInstanceName())
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
			// Skip putting this in the kv map -- we don't want the
			// cache to be queried for empty files.
			continue
		}
		kvs[uploadDigest] = uploadRequest.GetData()
	}

	if err := cache.SetMulti(ctx, kvs); err != nil {
		return nil, err
	}
	for uploadDigest, _ := range kvs {
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
	cache := s.getCache(req.GetInstanceName())
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
		if !ok || os.IsNotExist(err) || d.GetSizeBytes() != int64(len(data)) {
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
	lock     sync.Mutex
	dirs     []*repb.Directory
	dirSizes map[*repb.Directory]int64
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

// Fetch the entire directory tree rooted at a node.
//
// This request must be targeted at a
// [Directory][build.bazel.remote.execution.v2.Directory] stored in the
// [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage]
// (CAS). The server will enumerate the `Directory` tree recursively and
// return every node descended from the root.
//
// The GetTreeRequest.page_token parameter can be used to skip ahead in
// the stream (e.g. when retrying a partially completed and aborted request),
// by setting it to a value taken from GetTreeResponse.next_page_token of the
// last successfully processed GetTreeResponse).
//
// The exact traversal order is unspecified and, unless retrieving subsequent
// pages from an earlier request, is not guaranteed to be stable across
// multiple invocations of `GetTree`.
//
// If part of the tree is missing from the CAS, the server will return the
// portion present and omit the rest.
//
// Errors:
//
// * `NOT_FOUND`: The requested tree root is not present in the CAS.
func (s *ContentAddressableStorageServer) GetTree(req *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
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
	cache := s.getCache(req.GetInstanceName())
	dirStack, err := NewDirStack(req.GetPageToken())
	if err != nil {
		return status.InvalidArgumentErrorf("Unparseable tree token: %s", err)
	}

	maxPageSize := int32(500)
	if req.GetPageSize() < maxPageSize && req.GetPageSize() > 0 {
		maxPageSize = req.GetPageSize()
	}

	if dirStack.Empty() {
		rootDir, err := s.fetchDir(ctx, cache, req.GetRootDigest())
		if err != nil {
			return err
		}
		dirStack.Push(rootDir, req.GetRootDigest().GetSizeBytes())
	}

	rsp := &repb.GetTreeResponse{}
	rspSizeBytes := int64(0)
	finishPage := func(lastResponse bool) error {
		if !lastResponse {
			token, err := dirStack.SerializeToToken()
			if err != nil {
				return err
			}
			rsp.NextPageToken = token
		}
		return stream.Send(rsp)
	}

	for dir, sizeBytes := dirStack.Pop(); dir != nil; dir, sizeBytes = dirStack.Pop() {
		if sizeBytes > gRPCMaxSize {
			return status.InternalError("Directory size exceeds RPC max. Get out!")
		}
		if rspSizeBytes+sizeBytes > gRPCMaxSize {
			// Response is *already* full, so put this dir back on
			// the stack for the next call to GetTree and finish
			// this page.
			dirStack.Push(dir, sizeBytes)
			return finishPage(dirStack.Empty())
		}
		rspSizeBytes += sizeBytes
		rsp.Directories = append(rsp.Directories, dir)
		for _, dirNode := range dir.Directories {
			dig := dirNode.GetDigest()
			if dig.GetHash() == digest.EmptySha256 {
				continue
			}
			subDir, err := s.fetchDir(ctx, cache, dig)
			if err != nil {
				return err
			}
			dirStack.Push(subDir, dig.GetSizeBytes())
		}
		if int32(len(rsp.Directories)) == maxPageSize {
			return finishPage(dirStack.Empty())
		}
	}
	return finishPage(true)
}
