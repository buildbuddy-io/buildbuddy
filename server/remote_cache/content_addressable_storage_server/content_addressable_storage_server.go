package content_addressable_storage_server

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/directory_size"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/rpcutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	remote_cache_config "github.com/buildbuddy-io/buildbuddy/server/remote_cache/config"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gstatus "google.golang.org/grpc/status"
)

const TreeCacheRemoteInstanceName = "_bb_treecache_"

var (
	enableTreeCaching         = flag.Bool("cache.enable_tree_caching", true, "If true, cache GetTree responses (full and partial)")
	treeCacheSeed             = flag.String("cache.tree_cache_seed", "treecache-09032024", "If set, hash this with digests before caching / reading from tree cache")
	minTreeCacheLevel         = flag.Int("cache.tree_cache_min_level", 1, "The min level at which the tree may be cached. 0 is the root")
	minTreeCacheDescendents   = flag.Int("cache.tree_cache_min_descendents", 3, "The min number of descendents a node must parent in order to be cached")
	maxTreeCacheSetDuration   = flag.Duration("cache.max_tree_cache_set_duration", time.Second, "The max amount of time to wait for unfinished tree cache entries to be set.")
	treeCacheWriteProbability = flag.Float64("cache.tree_cache_write_probability", .01, "Write to the tree cache with this probability")
	enableTreeCacheSplitting  = flag.Bool("cache.tree_cache_splitting", false, "If true, try to split up TreeCache entries to save space.")
	treeCacheSplittingMinSize = flag.Int("cache.tree_cache_splitting_min_size", 10000, "Minimum number of files in a subtree before we'll split it in the treecache.")
	getTreeSubtreeSupport     = flag.Bool("cache.get_tree_subtree_support", true, "If true, respect the 'send_cache_subtrees' field on GetTree")
	getTreeSubtreeMinDirCount = flag.Int("cache.get_tree_subtree_min_dir_count", 10, "The minimum number of directory children a subtree must have before we're willing to tell the client to cache it (inclusive).")
)

type ContentAddressableStorageServer struct {
	env   environment.Env
	cache interfaces.Cache
}

func Register(env *real_environment.RealEnv) error {
	// OPTIONAL CACHE API -- only enable if configured.
	if env.GetCache() == nil {
		return nil
	}
	casServer, err := NewContentAddressableStorageServer(env)
	if err != nil {
		return status.InternalErrorf("Error initializing ContentAddressableStorageServer: %s", err)
	}
	env.SetCASServer(casServer)

	conn, err := grpc_client.DialInternal(env, fmt.Sprintf("grpc://localhost:%d", grpc_server.GRPCPort()))
	casClient := repb.NewContentAddressableStorageClient(conn)
	if err != nil {
		return status.InternalErrorf("Error initializing ContentAddressableStorageClient: %s", err)
	}
	env.SetContentAddressableStorageClient(casClient)

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
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env.GetAuthenticator())
	if err != nil {
		return nil, err
	}
	digestsToLookup := make([]*rspb.ResourceName, 0, len(req.GetBlobDigests()))
	for _, d := range req.GetBlobDigests() {
		rn := digest.NewResourceName(d, req.GetInstanceName(), rspb.CacheType_CAS, req.GetDigestFunction())
		if rn.IsEmpty() {
			continue
		}
		digestsToLookup = append(digestsToLookup, rn.ToProto())
	}
	missing, err := s.cache.FindMissing(ctx, digestsToLookup)
	if err != nil {
		return nil, err
	}
	rsp.MissingBlobDigests = append(rsp.MissingBlobDigests, missing...)
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
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env.GetAuthenticator())
	if err != nil {
		return nil, err
	}

	canWrite, err := capabilities.IsGranted(ctx, s.env.GetAuthenticator(), akpb.ApiKey_CACHE_WRITE_CAPABILITY|akpb.ApiKey_CAS_WRITE_CAPABILITY)
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

	ht := s.env.GetHitTrackerFactory().NewCASHitTracker(ctx, bazel_request.GetRequestMetadata(ctx))
	kvs := make(map[*rspb.ResourceName][]byte, len(req.Requests))
	for _, uploadRequest := range req.Requests {
		rn := digest.NewResourceName(uploadRequest.GetDigest(), req.GetInstanceName(), rspb.CacheType_CAS, req.GetDigestFunction())
		if err := rn.Validate(); err != nil {
			return nil, err
		}
		uploadTracker := ht.TrackUpload(rn.GetDigest())
		// defers are preetty cheap: https://tpaschalis.github.io/defer-internals/
		// so doing 100-1000 or so in this loop is fine.
		bytesWrittenToCache := 0
		bytesFromClient := len(uploadRequest.GetData())
		defer func() {
			if err := uploadTracker.CloseWithBytesTransferred(int64(bytesWrittenToCache), int64(bytesFromClient), uploadRequest.GetCompressor(), "cas_server"); err != nil {
				log.Debugf("BatchUpdateBlobs: upload tracker CloseWithBytesTransferred error: %s", err)
			}
		}()

		if rn.IsEmpty() {
			rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
				Digest: rn.GetDigest(),
				Status: &statuspb.Status{Code: int32(codes.OK)},
			})
			// Skip putting this in the kv map -- we don't want to attempt to
			// write empty files.
			continue
		}
		if !s.supportsCompressor(uploadRequest.Compressor) {
			err := status.UnimplementedErrorf("Unsupported compressor %s", uploadRequest.Compressor)
			rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
				Digest: rn.GetDigest(),
				Status: gstatus.Convert(err).Proto(),
			})
			continue
		}
		checksum, err := digest.HashForDigestType(rn.GetDigestFunction())
		if err != nil {
			return nil, err
		}
		decompressedData := uploadRequest.GetData()
		if uploadRequest.Compressor == repb.Compressor_ZSTD {
			decompressedData, err = zstdDecompress(uploadRequest.GetData(), uploadRequest.GetDigest().GetSizeBytes())
			if err != nil {
				rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
					Digest: rn.GetDigest(),
					Status: gstatus.Convert(err).Proto(),
				})
				continue
			}
		}
		checksum.Write(decompressedData)
		computedDigest := fmt.Sprintf("%x", checksum.Sum(nil))
		if computedDigest != rn.GetDigest().GetHash() {
			err := status.DataLossErrorf("Uploaded bytes checksum (%q) did not match digest (%q).", computedDigest, rn.GetDigest().GetHash())
			rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
				Digest: rn.GetDigest(),
				Status: gstatus.Convert(err).Proto(),
			})
			continue
		}
		if int64(len(decompressedData)) != rn.GetDigest().GetSizeBytes() {
			err := status.DataLossErrorf("Uploaded blob size (%d) did not match expected size (%d).", len(decompressedData), rn.GetDigest().GetSizeBytes())
			rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
				Digest: rn.GetDigest(),
				Status: gstatus.Convert(err).Proto(),
			})
			continue
		}

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
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env.GetAuthenticator())
	if err != nil {
		return nil, err
	}

	type closeTrackerFunc func(data downloadTrackerData)
	closeTrackerFuncs := make([]closeTrackerFunc, 0, len(req.Digests))
	closeTrackerData := make([]downloadTrackerData, 0, len(req.Digests))
	ht := s.env.GetHitTrackerFactory().NewCASHitTracker(ctx, bazel_request.GetRequestMetadata(ctx))

	cacheRequest := make([]*rspb.ResourceName, 0, len(req.Digests))
	rsp.Responses = make([]*repb.BatchReadBlobsResponse_Response, 0, len(req.Digests))
	clientAcceptsZstd := remote_cache_config.ZstdTranscodingEnabled() && clientAcceptsCompressor(req.AcceptableCompressors, repb.Compressor_ZSTD)
	readZstd := clientAcceptsZstd && s.cache.SupportsCompressor(repb.Compressor_ZSTD)

	requestedResources := make([]*digest.ResourceName, 0, len(req.GetDigests()))
	for _, readDigest := range req.GetDigests() {
		rn := digest.NewResourceName(readDigest, req.GetInstanceName(), rspb.CacheType_CAS, req.GetDigestFunction())
		if err := rn.Validate(); err != nil {
			return nil, err
		}
		requestedResources = append(requestedResources, rn)
		downloadTracker := ht.TrackDownload(rn.GetDigest())

		if !rn.IsEmpty() {
			closeTrackerFuncs = append(closeTrackerFuncs, func(data downloadTrackerData) {
				if err := downloadTracker.CloseWithBytesTransferred(int64(data.bytesReadFromCache), int64(data.bytesDownloadedToClient), data.compressor, "cas_server"); err != nil {
					log.Debugf("BatchReadBlobs: download tracker CloseWithBytesTransferred error: %s", err)
				}
			})
			if readZstd {
				rn.SetCompressor(repb.Compressor_ZSTD)
			}
			cacheRequest = append(cacheRequest, rn.ToProto())
		}
	}
	cacheRsp, err := s.cache.GetMulti(ctx, cacheRequest)

	for _, rn := range requestedResources {
		if rn.IsEmpty() {
			rsp.Responses = append(rsp.Responses, &repb.BatchReadBlobsResponse_Response{
				Digest: rn.GetDigest(),
				Status: &statuspb.Status{Code: int32(codes.OK)},
			})
			continue
		}

		data, ok := cacheRsp[rn.GetDigest()]
		blobRsp := &repb.BatchReadBlobsResponse_Response{
			Digest: rn.GetDigest(),
			Data:   data,
		}
		if clientAcceptsZstd {
			blobRsp.Compressor = repb.Compressor_ZSTD
		}
		bytesFromCache := len(data)
		bytesToClient := len(data)

		if !ok || os.IsNotExist(err) {
			blobRsp.Status = &statuspb.Status{Code: int32(codes.NotFound)}
		} else if rn.GetDigest().GetSizeBytes() != int64(len(data)) && !readZstd {
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

	for i, closeFn := range closeTrackerFuncs {
		closeFn(closeTrackerData[i])
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

func (s *ContentAddressableStorageServer) fetchDir(ctx context.Context, dirName *digest.ResourceName) (*repb.Directory, error) {
	if err := dirName.Validate(); err != nil {
		return nil, err
	}
	// Fetch the "Directory" object which enumerates all the blobs in the directory
	blob, err := s.cache.Get(ctx, dirName.ToProto())
	if err != nil {
		if status.IsNotFoundError(err) {
			return nil, digest.MissingDigestError(dirName.GetDigest())
		}
		return nil, err
	}

	dir := &repb.Directory{}
	if err := proto.Unmarshal(blob, dir); err != nil {
		return nil, err
	}
	return dir, nil
}

func makeTreeCachePointer(directoryNode *rspb.ResourceName, digestFunction repb.DigestFunction_Value) (*digest.ResourceName, error) {
	buf := strings.NewReader(directoryNode.GetDigest().GetHash() + *treeCacheSeed)
	d, err := digest.Compute(buf, digestFunction)
	if err != nil {
		return nil, err
	}
	instanceName := fmt.Sprintf("%s/%d", TreeCacheRemoteInstanceName, d.GetSizeBytes())
	// N.B: This is a AC digest, not a CAS one like the pointer below.
	return digest.NewResourceName(d, instanceName, rspb.CacheType_AC, digestFunction), nil
}

func makeTreeCacheDigest(digestFunction repb.DigestFunction_Value, buf []byte) (*digest.ResourceName, error) {
	d, err := digest.Compute(bytes.NewReader(buf), digestFunction)
	if err != nil {
		return nil, err
	}
	instanceName := fmt.Sprintf("%s/%d", TreeCacheRemoteInstanceName, d.GetSizeBytes())
	// N.B: This is a CAS digest, not an AC one like the pointer above.
	return digest.NewResourceName(d, instanceName, rspb.CacheType_CAS, digestFunction), nil
}

func makeTreeCacheActionResult(blob *rspb.ResourceName) ([]byte, error) {
	ar := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{
				Path:   TreeCacheRemoteInstanceName,
				Digest: blob.GetDigest(),
			},
		},
	}
	return proto.Marshal(ar)
}

func (s *ContentAddressableStorageServer) cacheTreeNode(ctx context.Context, rootDir *capb.DirectoryWithDigest, treeCachePointer *digest.ResourceName, treeCache *capb.TreeCache) error {
	rootCache, childCaches, err := splitTree(rootDir, treeCache)
	if err != nil {
		return nil
	}

	allChildren := rootCache.GetChildren()
	for _, childCache := range childCaches {
		allChildren = append(allChildren, childCache.GetChildren()...)
	}
	if !isComplete(allChildren) {
		// incomplete tree cache error will be logged by `isComplete`.
		return nil
	}

	var childBytesWritten = 0
	if len(childCaches) > 0 {
		mu := &sync.Mutex{}
		eg, egCtx := errgroup.WithContext(ctx)
		for _, childCache := range childCaches {
			eg.Go(func() error {
				childBuf, err := proto.Marshal(childCache)
				if err != nil {
					return err
				}
				childBlob, err := makeTreeCacheDigest(treeCachePointer.GetDigestFunction(), childBuf)
				if err != nil {
					return err
				}
				rn := childBlob.ToProto()
				present, err := s.cache.Contains(egCtx, rn)
				if err != nil {
					return err
				}
				if !present {
					if err := s.cache.Set(egCtx, rn, childBuf); err != nil {
						return err
					}
					metrics.TreeCacheSplitWriteCount.Inc()
				}
				mu.Lock()
				defer mu.Unlock()
				if !present {
					childBytesWritten += len(childBuf)
				}
				rootCache.TreeCacheChildren = append(rootCache.TreeCacheChildren, childBlob.ToProto())
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return err
		}
	}

	buf, err := proto.Marshal(rootCache)
	if err != nil {
		return err
	}

	// We could store the entire tree cache entry in the action result, but
	// doing so would mean potentially storing a very large action. Instead
	// we write the tree cache entry contents to the CAS, and use a pointer
	// based only on the cached directory name to find it.
	treeCacheBlob, err := makeTreeCacheDigest(treeCachePointer.GetDigestFunction(), buf)
	if err != nil {
		return err
	}
	if err := s.cache.Set(ctx, treeCacheBlob.ToProto(), buf); err != nil {
		return err
	}
	pointerBuf, err := makeTreeCacheActionResult(treeCacheBlob.ToProto())
	if err != nil {
		return err
	}

	if err = s.cache.Set(ctx, treeCachePointer.ToProto(), pointerBuf); err == nil {
		metrics.TreeCacheSetCount.With(prometheus.Labels{
			metrics.TreeCacheSetStatus: "success",
		}).Inc()
		metrics.TreeCacheBytesTransferred.With(prometheus.Labels{
			metrics.TreeCacheOperation: "write",
		}).Add(float64(len(buf) + childBytesWritten))
	} else {
		if context.Cause(ctx) != nil && context.Cause(ctx) == context.DeadlineExceeded {
			metrics.TreeCacheSetCount.With(prometheus.Labels{
				metrics.TreeCacheSetStatus: "deadline_exceeded",
			}).Inc()
			log.Debugf("Could not set treeCache blob (extended context timed out): %s. TreeCache directory count: %d, split count: %d", context.Cause(ctx), len(treeCache.Children), len(childCaches))
		} else {
			metrics.TreeCacheSetCount.With(prometheus.Labels{
				metrics.TreeCacheSetStatus: "other_error",
			}).Inc()
			log.Debugf("Could not set treeCache blob (an error was thrown):  %s. TreeCache directory count: %d, split count: %d", err, len(treeCache.Children), len(childCaches))
		}
	}
	return nil
}

func (s *ContentAddressableStorageServer) lookupCachedTreeNodeInCAS(ctx context.Context, treeCacheRN *digest.ResourceName) ([]*capb.DirectoryWithDigest, int, error) {
	if buf, err := s.cache.Get(ctx, treeCacheRN.ToProto()); err == nil {
		treeCache := &capb.TreeCache{}
		if err := proto.Unmarshal(buf, treeCache); err == nil {
			if !*enableTreeCacheSplitting && len(treeCache.GetTreeCacheChildren()) > 0 {
				// Don't return split trees if splitting is disabled.
				return nil, 0, status.NotFoundErrorf("tree-cache-not-found")
			}

			// If we split out any directories, we need to also fetch all of those.
			bytesRead := len(buf)
			if len(treeCache.GetTreeCacheChildren()) > 0 {
				mu := &sync.Mutex{}
				eg, egCtx := errgroup.WithContext(ctx)
				for _, childTreeCache := range treeCache.GetTreeCacheChildren() {
					childRN := digest.ResourceNameFromProto(childTreeCache)
					eg.Go(func() error {
						extraKids, extraBytes, err := s.lookupCachedTreeNodeInCAS(egCtx, childRN)
						if err != nil {
							if status.IsNotFoundError(err) {
								metrics.TreeCacheSplitLookupCount.With(prometheus.Labels{
									metrics.TreeCacheSplitLookupStatus: "miss",
								}).Inc()
							} else {
								metrics.TreeCacheSplitLookupCount.With(prometheus.Labels{
									metrics.TreeCacheSplitLookupStatus: "failure",
								}).Inc()
							}

							return err
						}
						metrics.TreeCacheSplitLookupCount.With(prometheus.Labels{
							metrics.TreeCacheSplitLookupStatus: "hit",
						}).Inc()
						mu.Lock()
						defer mu.Unlock()
						treeCache.Children = append(treeCache.Children, extraKids...)
						bytesRead += extraBytes
						return nil
					})
				}
				if err := eg.Wait(); err != nil {
					return nil, 0, err
				}
			}
			return treeCache.GetChildren(), bytesRead, nil
		}
	}
	return nil, 0, status.NotFoundErrorf("tree-cache-not-found")
}

// Given a resource name for the AC pointer to a cached tree, this returns the
// cached tree and a direct pointer to its location in the CAS.  If the tree
// isn't found, a NotFoundError is returned instead.
func (s *ContentAddressableStorageServer) lookupCachedTreeNode(ctx context.Context, level int, treeCachePointer *digest.ResourceName) ([]*capb.DirectoryWithDigest, *digest.ResourceName, error) {
	levelLabel := fmt.Sprintf("%d", min(level, 12))

	if pointerBuf, err := s.cache.Get(ctx, treeCachePointer.ToProto()); err == nil {
		ar := &repb.ActionResult{}
		if err := proto.Unmarshal(pointerBuf, ar); err == nil {
			if len(ar.OutputFiles) >= 1 && ar.OutputFiles[0].Path == TreeCacheRemoteInstanceName {
				treeCacheRN := digest.NewResourceName(ar.OutputFiles[0].Digest, treeCachePointer.GetInstanceName(), rspb.CacheType_CAS, treeCachePointer.GetDigestFunction())
				children, bytesRead, err := s.lookupCachedTreeNodeInCAS(ctx, treeCacheRN)
				if err == nil {
					metrics.TreeCacheLookupCount.With(prometheus.Labels{
						metrics.TreeCacheLookupStatus: "hit",
						metrics.TreeCacheLookupLevel:  levelLabel,
					}).Inc()
					metrics.TreeCacheBytesTransferred.With(prometheus.Labels{
						metrics.TreeCacheOperation: "read",
					}).Add(float64(bytesRead))

					return children, treeCacheRN, err
				}
			}
		}
	}
	metrics.TreeCacheLookupCount.With(prometheus.Labels{
		metrics.TreeCacheLookupStatus: "miss",
		metrics.TreeCacheLookupLevel:  levelLabel,
	}).Inc()
	return nil, nil, status.NotFoundErrorf("tree-cache-not-found")
}

func (s *ContentAddressableStorageServer) fetchDirectory(ctx context.Context, remoteInstanceName string, digestFunction repb.DigestFunction_Value, dd *capb.DirectoryWithDigest) ([]*capb.DirectoryWithDigest, error) {
	dir := dd.Directory
	if len(dir.Directories) == 0 {
		return nil, nil
	}
	subdirDigests := make([]*rspb.ResourceName, 0, len(dir.Directories))
	for _, dirNode := range dir.Directories {
		rn := digest.NewResourceName(dirNode.GetDigest(), remoteInstanceName, rspb.CacheType_CAS, digestFunction)
		if rn.IsEmpty() {
			continue
		}
		subdirDigests = append(subdirDigests, rn.ToProto())
	}
	rspMap, err := s.cache.GetMulti(ctx, subdirDigests)
	if err != nil {
		return nil, err
	}

	children := make([]*capb.DirectoryWithDigest, 0, len(subdirDigests))
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
		children = append(children, &capb.DirectoryWithDigest{Directory: subDir, ResourceName: rn})
	}
	return children, nil
}

func compareSubtrees(a *digest.ResourceName, b *digest.ResourceName) int {
	if hashCompare := strings.Compare(a.GetDigest().GetHash(), b.GetDigest().GetHash()); hashCompare != 0 {
		return hashCompare
	}
	aSize := a.GetDigest().GetSizeBytes()
	bSize := b.GetDigest().GetSizeBytes()
	if aSize == bSize {
		return 0
	} else if aSize > bSize {
		return 1
	} else {
		return -1
	}
}

func dedupeSubtrees(a *digest.ResourceName, b *digest.ResourceName) bool {
	return a.GetDigest().GetHash() == b.GetDigest().GetHash() && a.GetDigest().GetSizeBytes() == b.GetDigest().GetSizeBytes()
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
	rootDirRN := digest.NewResourceName(req.GetRootDigest(), req.GetInstanceName(), rspb.CacheType_CAS, req.GetDigestFunction())
	if rootDirRN.IsEmpty() {
		return nil
	}

	ctx, err := prefix.AttachUserPrefixToContext(stream.Context(), s.env.GetAuthenticator())
	if err != nil {
		return err
	}
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

	finishDir := func(dirWithDigest *capb.DirectoryWithDigest) error {
		mu.Lock()
		defer mu.Unlock()

		dir := dirWithDigest.Directory
		size := dirWithDigest.GetResourceName().GetDigest().GetSizeBytes()

		if rspSizeBytes+size > rpcutil.GRPCMaxSizeBytes {
			if err := stream.Send(rsp); err != nil {
				return err
			}
			rsp = &repb.GetTreeResponse{}
			rspSizeBytes = 0
		}
		rspSizeBytes += size
		rsp.Directories = append(rsp.Directories, dir)
		dirCount += 1
		return nil
	}

	cacheCtx, cacheCancel := background.ExtendContextForFinalization(ctx, 1*time.Second)
	cacheEG, cacheEGCtx := errgroup.WithContext(cacheCtx)
	// Finalize tree cache sets; but don't wait forever.
	defer func() {
		go func() {
			cacheEG.Wait()
			cacheCancel()
		}()
	}()

	type fetchResult struct {
		// If the root node of the tree was found in the TreeCache, this value
		// will be non-nil and contain the CAS RN of the whole tree.
		cachedRoot *digest.ResourceName
		// The trees that should be sent directly to the client (they are not
		// part of a cached subtree).  Note that when cachedRoot is set, this
		// slice will contain the entire tree.
		mainDirectories []*capb.DirectoryWithDigest
		// Resource names pointing to subtrees that were found in the cache and
		// are considered big enough to be worth caching on the client side.
		cachedSubtrees []*digest.ResourceName
		// The digests of the directories contained in cachedSubtrees--this is
		// tracked so that we can validate the full tree after fetching.
		subtreeDirectories []*capb.DirectoryWithDigest
	}

	var fetch func(ctx context.Context, dirWithDigest *capb.DirectoryWithDigest, level int) (*fetchResult, error)
	fetch = func(ctx context.Context, dirWithDigest *capb.DirectoryWithDigest, level int) (*fetchResult, error) {
		if len(dirWithDigest.Directory.Directories) == 0 {
			return &fetchResult{
				mainDirectories: []*capb.DirectoryWithDigest{dirWithDigest},
			}, nil
		}

		treeCachePointer, err := makeTreeCachePointer(dirWithDigest.GetResourceName(), req.GetDigestFunction())
		if err != nil {
			return nil, err
		}
		if *enableTreeCaching && level >= *minTreeCacheLevel {
			if children, rn, err := s.lookupCachedTreeNode(ctx, level, treeCachePointer); err == nil {
				return &fetchResult{
					mainDirectories: children,
					cachedRoot:      rn,
				}, nil
			}
		}

		start := time.Now()
		children, err := s.fetchDirectory(ctx, req.GetInstanceName(), req.GetDigestFunction(), dirWithDigest)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		fetchDuration += time.Since(start)
		fetchCount += 1
		mu.Unlock()

		allDescendents := make([]*capb.DirectoryWithDigest, 0, len(children))
		allDescendents = append(allDescendents, dirWithDigest)
		allCachedSubtrees := make([]*digest.ResourceName, 0)
		allCachedSubtreeContents := make([]*capb.DirectoryWithDigest, 0)

		eg, egCtx := errgroup.WithContext(ctx)
		for _, childDirWithDigest := range children {
			childDirWithDigest := childDirWithDigest
			l := level
			eg.Go(func() error {
				grandchild, err := fetch(egCtx, childDirWithDigest, l+1)
				if err != nil {
					return err
				}
				mu.Lock()
				defer mu.Unlock()
				subtreesRequested := *getTreeSubtreeSupport && req.GetSendCachedSubtreeDigests()
				if subtreesRequested && grandchild.cachedRoot != nil && len(grandchild.mainDirectories) >= *getTreeSubtreeMinDirCount {
					// This grandchild was cached--we are guaranteed that cachedSubtrees
					// will be empty (TreeCache entries are always complete), so we
					// record the subtree root for our response.  The only purpose of
					// saving the subtree's contents is so that we can validate the
					// entire tree at the end of the request.
					allCachedSubtrees = append(allCachedSubtrees, grandchild.cachedRoot)
					allCachedSubtreeContents = append(allCachedSubtreeContents, grandchild.mainDirectories...)
				} else {
					// Three possibilities here:
					// 0.  Subtrees aren't being requested; all contents will be in
					//     mainDirectories and we copy up.
					// 1.  The grandchild is cached, but small: all directories will be in
					//     mainDirectories, and cachedSubtrees will be empty.  We copy its
					//     contents to our response instead of sending it as a subtree.
					// 2.  The grandchild is not cached, but some of its own children might
					//     have been cached, so we will happily send those subtree RNs back
					//     to the client--we copy those upward (recursively) in addition to
					//     all of the uncached content in mainDirectories.
					allDescendents = append(allDescendents, grandchild.mainDirectories...)
					if len(grandchild.cachedSubtrees) > 0 {
						allCachedSubtrees = append(allCachedSubtrees, grandchild.cachedSubtrees...)
						allCachedSubtreeContents = append(allCachedSubtreeContents, grandchild.subtreeDirectories...)
					}
				}
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}

		if *enableTreeCaching && level >= *minTreeCacheLevel && (len(allDescendents)+len(allCachedSubtreeContents)) >= *minTreeCacheDescendents {
			if r := rand.Float64(); r <= *treeCacheWriteProbability {
				treeCache := &capb.TreeCache{
					Children: make([]*capb.DirectoryWithDigest, len(allDescendents)),
				}
				copy(treeCache.Children, allDescendents)
				treeCache.Children = append(treeCache.Children, allCachedSubtreeContents...)
				cacheEG.Go(func() error {
					return s.cacheTreeNode(cacheEGCtx, dirWithDigest, treeCachePointer, treeCache)
				})
			}
		}
		return &fetchResult{
			cachedRoot:         nil,
			mainDirectories:    allDescendents,
			cachedSubtrees:     allCachedSubtrees,
			subtreeDirectories: allCachedSubtreeContents,
		}, nil
	}

	// We can't send back a "subtree" for the root element, so there's no use in
	// checking if it was cached or not--and the tree is guaranteed to have all
	// nodes in it in that case as well.  This is a bit of a weird edge: we
	// don't optimize anything if the caller is requesting an identical tree to
	// a previous run, which can actually happen in cases like
	// `runs_per_test=100`.
	// TODO(jdhollen): find a decent workaround for the above comment.
	result, err := fetch(ctx, &capb.DirectoryWithDigest{
		Directory:    rootDir,
		ResourceName: rootDirRN.ToProto(),
	}, 0)
	if err != nil {
		return err
	}
	for _, dir := range result.mainDirectories {
		if err := finishDir(dir); err != nil {
			return err
		}
	}

	if len(result.cachedSubtrees) > 0 {
		// Sort and dedupe cached subtrees in case we ever want to cache this response somewhere.
		slices.SortFunc(result.cachedSubtrees, compareSubtrees)
		result.cachedSubtrees = slices.CompactFunc(result.cachedSubtrees, dedupeSubtrees)
		rsp.Subtrees = make([]*repb.SubtreeResourceName, 0, len(result.cachedSubtrees))
		for _, st := range result.cachedSubtrees {
			rsp.Subtrees = append(rsp.Subtrees, &repb.SubtreeResourceName{
				Digest:         st.GetDigest(),
				InstanceName:   st.GetInstanceName(),
				DigestFunction: st.GetDigestFunction(),
				Compressor:     st.GetCompressor(),
			})
		}
		dirCount += len(result.subtreeDirectories)

		// Make sure we send all subtree data below.
		rspSizeBytes = 1
	}

	log.Debugf("GetTree fetched %d dirs from cache across %d calls (including %d cached subtrees) in cumulative %s (total time: %s)", dirCount, fetchCount, len(result.cachedSubtrees), fetchDuration, time.Since(rpcStart))
	if rspSizeBytes > 0 {
		return stream.Send(rsp)
	}
	return nil
}

type FileCountHelper interface {
	GetChildCount(string) int64
}

func isEligibleForSplitting(dir *capb.DirectoryWithDigest, name string, fch FileCountHelper) bool {
	rn := dir.GetResourceName()
	if digest.IsEmptyHash(rn.GetDigest(), rn.GetDigestFunction()) {
		return false
	}
	return *enableTreeCacheSplitting && name == "node_modules" && fch.GetChildCount(dir.GetResourceName().GetDigest().GetHash()) > int64(*treeCacheSplittingMinSize)
}

func makeTreeCacheFromSubtree(root *capb.DirectoryWithDigest, allDigests map[string]*capb.DirectoryWithDigest) (*capb.TreeCache, error) {
	out := &capb.TreeCache{}

	var traverse func(current *capb.DirectoryWithDigest) error
	traverse = func(current *capb.DirectoryWithDigest) error {
		rn := current.GetResourceName()
		if digest.IsEmptyHash(rn.GetDigest(), rn.GetDigestFunction()) {
			return nil
		}
		out.Children = append(out.Children, current)

		for _, child := range current.GetDirectory().GetDirectories() {
			if digest.IsEmptyHash(child.GetDigest(), rn.GetDigestFunction()) {
				continue
			}
			childDir, ok := allDigests[child.GetDigest().GetHash()]
			if !ok {
				return status.NotFoundError("incomplete subtree")
			}
			err := traverse(childDir)
			if err != nil {
				return err
			}
		}
		return nil
	}

	if err := traverse(root); err != nil {
		return nil, err
	}

	slices.SortFunc(out.Children, func(a *capb.DirectoryWithDigest, b *capb.DirectoryWithDigest) int {
		return strings.Compare(a.GetResourceName().GetDigest().GetHash(), b.GetResourceName().GetDigest().GetHash())
	})
	return out, nil
}

func splitTree(rootDir *capb.DirectoryWithDigest, cache *capb.TreeCache) (*capb.TreeCache, []*capb.TreeCache, error) {
	if !*enableTreeCacheSplitting {
		return cache, nil, nil
	}
	allDigests := make(map[string]*capb.DirectoryWithDigest, len(cache.GetChildren()))
	counter := directory_size.NewDirectorySizeCounter(rootDir.GetResourceName().GetDigestFunction())
	for _, child := range cache.GetChildren() {
		rn := child.GetResourceName()
		if digest.IsEmptyHash(rn.GetDigest(), rn.GetDigestFunction()) {
			continue
		}
		allDigests[rn.GetDigest().GetHash()] = child
		counter.AddDirWithDigest(child.GetResourceName().GetDigest(), child.GetDirectory())
	}

	rootTree := &capb.TreeCache{Children: []*capb.DirectoryWithDigest{rootDir}}
	childTrees := make([]*capb.TreeCache, 0)

	var traverseForSplitting func(current *capb.DirectoryWithDigest, name string) error
	traverseForSplitting = func(current *capb.DirectoryWithDigest, name string) error {
		if isEligibleForSplitting(current, name, counter) {
			childTree, err := makeTreeCacheFromSubtree(current, allDigests)
			if err != nil {
				return err
			}
			childTrees = append(childTrees, childTree)
			return nil
		} else {
			rn := current.GetResourceName()
			if digest.IsEmptyHash(rn.GetDigest(), rn.GetDigestFunction()) {
				return nil
			}
			rootTree.Children = append(rootTree.Children, current)
			for _, childDirNode := range current.GetDirectory().GetDirectories() {
				if digest.IsEmptyHash(childDirNode.GetDigest(), current.GetResourceName().GetDigestFunction()) {
					continue
				}
				childDir, ok := allDigests[childDirNode.GetDigest().GetHash()]
				if !ok {
					return status.NotFoundError("incomplete subtree")
				}
				if err := traverseForSplitting(childDir, childDirNode.GetName()); err != nil {
					return err
				}
			}
			return nil
		}
	}

	for _, childDirNode := range rootDir.GetDirectory().GetDirectories() {
		if digest.IsEmptyHash(childDirNode.GetDigest(), rootDir.GetResourceName().GetDigestFunction()) {
			continue
		}
		childDir, ok := allDigests[childDirNode.GetDigest().GetHash()]
		if !ok {
			return nil, nil, status.NotFoundError("incomplete subtree")
		}

		if err := traverseForSplitting(childDir, childDirNode.GetName()); err != nil {
			return nil, nil, err
		}
	}

	// TODO(jdhollen): Track split out digests + avoid re-splitting

	return rootTree, childTrees, nil
}

func isComplete(children []*capb.DirectoryWithDigest) bool {
	allDigests := make(map[string]*capb.DirectoryWithDigest, len(children))
	for _, child := range children {
		rn := child.GetResourceName()
		if digest.IsEmptyHash(rn.GetDigest(), rn.GetDigestFunction()) {
			continue
		}
		allDigests[rn.GetDigest().GetHash()] = child
	}
	for _, child := range allDigests {
		dir := child.Directory
		rn := child.GetResourceName()
		if len(dir.Directories) == 0 && len(dir.Files) == 0 && len(dir.Symlinks) == 0 {
			log.Warningf("corrupted tree: empty dir: %+v, digest: %q", dir, rn.GetDigest().GetHash())
			return false
		}
		for _, dirNode := range child.GetDirectory().GetDirectories() {
			grn := digest.NewResourceName(dirNode.GetDigest(), "", rspb.CacheType_CAS, rn.GetDigestFunction())
			if grn.IsEmpty() {
				continue
			}
			if _, ok := allDigests[dirNode.GetDigest().GetHash()]; !ok {
				log.Warningf("incomplete tree: (missing digest: %q), allDigests: %+v", dirNode.GetDigest().GetHash(), allDigests)
				return false
			}
		}
	}
	return true
}
