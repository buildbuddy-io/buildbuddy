package content_addressable_storage_server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	remote_cache_config "github.com/buildbuddy-io/buildbuddy/server/remote_cache/config"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gstatus "google.golang.org/grpc/status"
)

const (
	gRPCMaxSize = int64(4194304 - 2000)

	// minTreeCacheLevel is the minimum level at which the tree may be
	// cached. Level 0 is the root of the tree.
	minTreeCacheLevel = 1

	// minTreeCacheDescendents is the minimum number of descendents a node
	// must have in order to be cached.
	minTreeCacheDescendents = 10
)

var (
	enableTreeCaching = flag.Bool("cache.enable_tree_caching", true, "If true, cache GetTree responses (full and partial)")
	treeCacheSeed     = flag.String("cache.tree_cache_seed", "treecache-07012022", "If set, hash this with digests before caching / reading from tree cache")
)

type ContentAddressableStorageServer struct {
	env   environment.Env
	cache interfaces.Cache
}

func Register(env environment.Env) error {
	// OPTIONAL CACHE API -- only enable if configured.
	if env.GetCache() == nil {
		return nil
	}
	casServer, err := NewContentAddressableStorageServer(env)
	if err != nil {
		return status.InternalErrorf("Error initializing ContentAddressableStorageServer: %s", err)
	}
	env.SetCASServer(casServer)
	return nil
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
	digestsToLookup := make([]*resource.ResourceName, 0, len(req.GetBlobDigests()))
	for _, d := range req.GetBlobDigests() {
		if d.GetHash() == digest.EmptySha256 {
			continue
		}
		if d.GetHash() == digest.EmptyHash {
			continue
		}
		rn := digest.NewCASResourceName(d, req.GetInstanceName()).ToProto()
		digestsToLookup = append(digestsToLookup, rn)
	}
	missing, err := s.cache.FindMissing(ctx, digestsToLookup)
	if err != nil {
		return nil, err
	}
	for _, d := range missing {
		rsp.MissingBlobDigests = append(rsp.MissingBlobDigests, d)
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
//   - `INVALID_ARGUMENT`: The client attempted to upload more than the
//     server supported limit.
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

	canWrite, err := capabilities.IsGranted(ctx, s.env, akpb.ApiKey_CACHE_WRITE_CAPABILITY|akpb.ApiKey_CAS_WRITE_CAPABILITY)
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

	rsp.Responses = make([]*repb.BatchUpdateBlobsResponse_Response, 0, len(req.Requests))

	ht := hit_tracker.NewHitTracker(ctx, s.env, false)
	kvs := make(map[*resource.ResourceName][]byte, len(req.Requests))
	for _, uploadRequest := range req.Requests {
		uploadDigest := uploadRequest.GetDigest()
		_, err := digest.Validate(uploadDigest)
		if err != nil {
			return nil, err
		}
		uploadTracker := ht.TrackUpload(uploadDigest)
		// defers are preetty cheap: https://tpaschalis.github.io/defer-internals/
		// so doing 100-1000 or so in this loop is fine.
		bytesWrittenToCache := 0
		defer func() {
			uploadTracker.CloseWithBytesTransferred(int64(bytesWrittenToCache), int64(len(uploadRequest.GetData())), uploadRequest.GetCompressor())
		}()

		if uploadDigest.GetHash() == digest.EmptySha256 {
			rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
				Digest: uploadDigest,
				Status: &statuspb.Status{Code: int32(codes.OK)},
			})
			// Skip putting this in the kv map -- we don't want to attempt to
			// write empty files.
			continue
		}
		if !s.supportsCompressor(uploadRequest.Compressor) {
			err := status.UnimplementedErrorf("Unsupported compressor %s", uploadRequest.Compressor)
			rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
				Digest: uploadDigest,
				Status: gstatus.Convert(err).Proto(),
			})
			continue
		}
		checksum := sha256.New()
		decompressedData := uploadRequest.GetData()
		if uploadRequest.Compressor == repb.Compressor_ZSTD {
			decompressedData, err = zstdDecompress(uploadRequest.GetData(), uploadRequest.GetDigest().GetSizeBytes())
			if err != nil {
				rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
					Digest: uploadDigest,
					Status: gstatus.Convert(err).Proto(),
				})
				continue
			}
		}
		checksum.Write(decompressedData)
		computedDigest := fmt.Sprintf("%x", checksum.Sum(nil))
		if computedDigest != uploadDigest.GetHash() {
			err := status.DataLossErrorf("Uploaded bytes checksum (%q) did not match digest (%q).", computedDigest, uploadDigest.GetHash())
			rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
				Digest: uploadDigest,
				Status: gstatus.Convert(err).Proto(),
			})
			continue
		}
		if int64(len(decompressedData)) != uploadDigest.GetSizeBytes() {
			err := status.DataLossErrorf("Uploaded blob size (%d) did not match expected size (%d).", len(decompressedData), uploadDigest.GetSizeBytes())
			rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
				Digest: uploadDigest,
				Status: gstatus.Convert(err).Proto(),
			})
			continue
		}

		rn := digest.NewCASResourceName(uploadDigest, req.GetInstanceName())
		// If cache doesn't support compression type, store decompressed data
		dataToCache := decompressedData
		if s.cache.SupportsCompressor(uploadRequest.GetCompressor()) {
			rn.SetCompressor(uploadRequest.GetCompressor())
			// If cache supports saving compressed data, pass through compressed data from the request
			dataToCache = uploadRequest.GetData()
		}
		kvs[rn.ToProto()] = dataToCache
		bytesWrittenToCache = len(dataToCache)
	}

	if err := s.cache.SetMulti(ctx, kvs); err != nil {
		return nil, err
	}
	for uploadDigest := range kvs {
		rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
			Digest: uploadDigest.GetDigest(),
			Status: &statuspb.Status{Code: int32(codes.OK)},
		})
	}
	return rsp, nil
}

type downloadTrackerData struct {
	bytesReadFromCache      int
	bytesDownloadedToClient int
	compressor              repb.Compressor_Value
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
//   - `INVALID_ARGUMENT`: The client attempted to read more than the
//     server supported limit.
//
// Every error on individual read will be returned in the corresponding digest
// status.
func (s *ContentAddressableStorageServer) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	rsp := &repb.BatchReadBlobsResponse{}
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}

	type closeTrackerFunc func(data downloadTrackerData)
	closeTrackerFuncs := make([]closeTrackerFunc, 0, len(req.Digests))
	closeTrackerData := make([]downloadTrackerData, 0, len(req.Digests))
	ht := hit_tracker.NewHitTracker(ctx, s.env, false)

	cacheRequest := make([]*resource.ResourceName, 0, len(req.Digests))
	rsp.Responses = make([]*repb.BatchReadBlobsResponse_Response, 0, len(req.Digests))
	clientAcceptsZstd := remote_cache_config.ZstdTranscodingEnabled() && clientAcceptsCompressor(req.AcceptableCompressors, repb.Compressor_ZSTD)
	readZstd := clientAcceptsZstd && s.cache.SupportsCompressor(repb.Compressor_ZSTD)

	for _, readDigest := range req.GetDigests() {
		_, err := digest.Validate(readDigest)
		if err != nil {
			return nil, err
		}
		downloadTracker := ht.TrackDownload(readDigest)
		closeTrackerFuncs = append(closeTrackerFuncs, func(data downloadTrackerData) {
			downloadTracker.CloseWithBytesTransferred(int64(data.bytesReadFromCache), int64(data.bytesDownloadedToClient), data.compressor)
		})

		if readDigest.GetHash() != digest.EmptySha256 {
			rn := digest.NewCASResourceName(readDigest, req.GetInstanceName())
			if readZstd {
				rn.SetCompressor(repb.Compressor_ZSTD)
			}
			cacheRequest = append(cacheRequest, rn.ToProto())
		}
	}
	cacheRsp, err := s.cache.GetMulti(ctx, cacheRequest)

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
		if clientAcceptsZstd {
			blobRsp.Compressor = repb.Compressor_ZSTD
		}
		bytesFromCache := len(data)
		bytesToClient := len(data)

		if !ok || os.IsNotExist(err) {
			blobRsp.Status = &statuspb.Status{Code: int32(codes.NotFound)}
		} else if d.GetSizeBytes() != int64(len(data)) && !readZstd {
			// We only expect the data length to be different from the digest if we read compressed data.
			// If we weren't reading compressed data, consider the data corrupted and return that it is not found
			blobRsp.Status = &statuspb.Status{Code: int32(codes.NotFound)}
		} else if err != nil {
			blobRsp.Status = &statuspb.Status{Code: int32(codes.Internal)}
		} else {
			blobRsp.Status = &statuspb.Status{Code: int32(codes.OK)}

			// If the cache doesn't support zstd compression but the client will accept it, compress data before sending
			if clientAcceptsZstd && !s.cache.SupportsCompressor(repb.Compressor_ZSTD) {
				blobRsp.Data = compression.CompressZstd(nil, blobRsp.Data)
				blobRsp.Compressor = repb.Compressor_ZSTD
				bytesToClient = len(blobRsp.Data)
			}
		}

		rsp.Responses = append(rsp.Responses, blobRsp)
		closeTrackerData = append(closeTrackerData, downloadTrackerData{
			bytesReadFromCache:      bytesFromCache,
			bytesDownloadedToClient: bytesToClient,
			compressor:              blobRsp.GetCompressor(),
		})
	}

	if len(closeTrackerFuncs) == len(rsp.Responses) {
		for i, closeFn := range closeTrackerFuncs {
			closeFn(closeTrackerData[i])
		}
	} else {
		alert.UnexpectedEvent("cas_batch_response_length_mismatch", "Unexpected batch read response length (%d expected, got %d)", len(closeTrackerFuncs), len(rsp.Responses))
	}

	return rsp, nil
}

func (s *ContentAddressableStorageServer) supportsCompressor(compressor repb.Compressor_Value) bool {
	return compressor == repb.Compressor_IDENTITY ||
		compressor == repb.Compressor_ZSTD && remote_cache_config.ZstdTranscodingEnabled()
}

func clientAcceptsCompressor(acceptableCompressors []repb.Compressor_Value, compressor repb.Compressor_Value) bool {
	// Per protocol, IDENTITY is always accepted.
	if compressor == repb.Compressor_IDENTITY {
		return true
	}
	for _, c := range acceptableCompressors {
		if c == compressor {
			return true
		}
	}
	return false
}

func zstdDecompress(data []byte, decompressedLength int64) ([]byte, error) {
	buf := make([]byte, decompressedLength)
	out, err := compression.DecompressZstd(buf, data)
	if err != nil {
		return nil, status.InternalErrorf("Failed to decompress zstd-compressed blob: %s", err)
	}
	return out, nil
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

func (s *ContentAddressableStorageServer) fetchDir(ctx context.Context, dirName *resource.ResourceName) (*repb.Directory, error) {
	_, err := digest.Validate(dirName.GetDigest())
	if err != nil {
		return nil, err
	}
	// Fetch the "Directory" object which enumerates all the blobs in the directory
	blob, err := s.cache.Get(ctx, dirName)
	if err != nil {
		return nil, err
	}

	dir := &repb.Directory{}
	if err := proto.Unmarshal(blob, dir); err != nil {
		return nil, err
	}
	return dir, nil
}

func makeTreeCacheDigest(d *repb.Digest) (*repb.Digest, error) {
	buf := bytes.NewBuffer([]byte(d.GetHash() + *treeCacheSeed))
	return digest.Compute(buf)
}

func (s *ContentAddressableStorageServer) fetchDirectory(ctx context.Context, remoteInstanceName string, dd *repb.DirectoryWithDigest) ([]*repb.DirectoryWithDigest, error) {
	dir := dd.Directory
	if len(dir.Directories) == 0 {
		return nil, nil
	}
	subdirDigests := make([]*resource.ResourceName, 0, len(dir.Directories))
	for _, dirNode := range dir.Directories {
		d := dirNode.GetDigest()
		if d.GetHash() == digest.EmptySha256 {
			continue
		}
		rn := digest.NewCASResourceName(d, remoteInstanceName).ToProto()
		subdirDigests = append(subdirDigests, rn)
	}
	rspMap, err := s.cache.GetMulti(ctx, subdirDigests)
	if err != nil {
		return nil, err
	}

	children := make([]*repb.DirectoryWithDigest, 0, len(subdirDigests))
	for _, rn := range subdirDigests {
		d := rn.GetDigest()
		blob, ok := rspMap[d]
		if !ok {
			return nil, digest.MissingDigestError(d)
		}
		subDir := &repb.Directory{}
		if err := proto.Unmarshal(blob, subDir); err != nil {
			return nil, err
		}
		if len(subDir.Directories) == 0 && len(subDir.Files) == 0 && len(subDir.Symlinks) == 0 {
			log.Warningf("Found empty directory for digest: %s blob: [%+v]", d.GetHash(), blob)
			return nil, status.NotFoundErrorf("Found empty directory for digest %s.", d.GetHash())
		}
		children = append(children, &repb.DirectoryWithDigest{Directory: subDir, Digest: d})
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
		return status.InvalidArgumentError("RootDigest is required to GetTree")
	}
	if req.GetRootDigest().GetHash() == digest.EmptySha256 {
		return nil
	}

	ctx, err := prefix.AttachUserPrefixToContext(stream.Context(), s.env)
	if err != nil {
		return err
	}
	rootDirRN := digest.NewCASResourceName(req.GetRootDigest(), req.GetInstanceName()).ToProto()
	rootDir, err := s.fetchDir(ctx, rootDirRN)
	if err != nil {
		return err
	}

	mu := &sync.Mutex{}
	rsp := &repb.GetTreeResponse{}
	rspSizeBytes := int64(0)
	dirCount := 0
	fetchCount := 0
	fetchDuration := time.Duration(0)

	finishDir := func(dirWithDigest *repb.DirectoryWithDigest) error {
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

	var fetch func(ctx context.Context, dirWithDigest *repb.DirectoryWithDigest, level int) ([]*repb.DirectoryWithDigest, error)
	fetch = func(ctx context.Context, dirWithDigest *repb.DirectoryWithDigest, level int) ([]*repb.DirectoryWithDigest, error) {
		if len(dirWithDigest.Directory.Directories) == 0 {
			return []*repb.DirectoryWithDigest{dirWithDigest}, nil
		}

		treeCacheDigest, err := makeTreeCacheDigest(dirWithDigest.Digest)
		if err != nil {
			return nil, err
		}
		if *enableTreeCaching {
			treeCacheRN := digest.NewACResourceName(treeCacheDigest, req.GetInstanceName()).ToProto()
			if blob, err := s.cache.Get(ctx, treeCacheRN); err == nil {
				treeCache := &repb.TreeCache{}
				if err := proto.Unmarshal(blob, treeCache); err == nil {
					if isComplete(treeCache.GetChildren()) {
						return treeCache.GetChildren(), nil
					} else {
						log.Warningf("Ignoring incomplete treeCache entry")
					}
				}
			}
		}

		start := time.Now()
		children, err := s.fetchDirectory(ctx, req.GetInstanceName(), dirWithDigest)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		fetchDuration += time.Since(start)
		fetchCount += 1
		mu.Unlock()

		allDescendents := make([]*repb.DirectoryWithDigest, 0, len(children))
		allDescendents = append(allDescendents, dirWithDigest)

		eg, egCtx := errgroup.WithContext(ctx)
		for _, childDirWithDigest := range children {
			childDirWithDigest := childDirWithDigest
			l := level
			eg.Go(func() error {
				grandChildren, err := fetch(egCtx, childDirWithDigest, l+1)
				if err != nil {
					return err
				}
				mu.Lock()
				defer mu.Unlock()
				allDescendents = append(allDescendents, grandChildren...)
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}

		if level > minTreeCacheLevel && len(allDescendents) > minTreeCacheDescendents && *enableTreeCaching {
			treeCache := &repb.TreeCache{
				Children: allDescendents,
			}
			if isComplete(treeCache.GetChildren()) {
				buf, err := proto.Marshal(treeCache)
				if err != nil {
					return nil, err
				}
				treeCacheRN := digest.NewACResourceName(treeCacheDigest, req.GetInstanceName()).ToProto()
				if err := s.cache.Set(ctx, treeCacheRN, buf); err != nil {
					log.Warningf("Error setting treeCache blob: %s", err)
				}
			} else {
				log.Warningf("Not caching incomplete tree cache")
			}
		}
		return allDescendents, nil
	}

	allDirs, err := fetch(ctx, &repb.DirectoryWithDigest{
		Directory: rootDir,
		Digest:    req.GetRootDigest(),
	}, 0)
	if err != nil {
		return err
	}
	for _, dir := range allDirs {
		if err := finishDir(dir); err != nil {
			return err
		}
	}

	log.Debugf("GetTree fetched %d dirs from cache across %d calls in cumulative %s (total time: %s)", dirCount, fetchCount, fetchDuration, time.Since(rpcStart))
	if rspSizeBytes > 0 {
		return stream.Send(rsp)
	}
	return nil
}

func isComplete(children []*repb.DirectoryWithDigest) bool {
	allDigests := make(map[string]struct{}, len(children))
	for _, child := range children {
		allDigests[child.GetDigest().GetHash()] = struct{}{}
	}
	for _, child := range children {
		dir := child.Directory
		if len(dir.Directories) == 0 && len(dir.Files) == 0 && len(dir.Symlinks) == 0 {
			log.Warningf("corrupted tree: empty dir: %+v, digest: %q", dir, child.GetDigest().GetHash())
			return false
		}
		for _, dirNode := range child.GetDirectory().GetDirectories() {
			if _, ok := allDigests[dirNode.GetDigest().GetHash()]; !ok {
				log.Warningf("incomplete tree: (missing digest: %q), allDigests: %+v", dirNode.GetDigest().GetHash(), allDigests)
				return false
			}
		}
	}
	return true
}
