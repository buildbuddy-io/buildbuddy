package content_addressable_storage_server

import (
	"context"
	"fmt"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/golang/protobuf/proto"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

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

// Determine if blobs are present in the CAS.
//
// Clients can use this API before uploading blobs to determine which ones are
// already present in the CAS and do not need to be uploaded again.
//
// There are no method-specific errors.
func (s *ContentAddressableStorageServer) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	rsp := &repb.FindMissingBlobsResponse{}
	for _, blobDigest := range req.BlobDigests {
		hash, err := digest.Validate(blobDigest)
		if err != nil {
			return nil, err
		}
		if hash == digest.EmptySha256 {
			continue
		}
		ck, err := perms.UserPrefixCacheKey(ctx, s.env, hash)
		if err != nil {
			return nil, err
		}
		exists, err := s.cache.Contains(ctx, ck)
		if err != nil {
			return nil, err
		}
		if !exists {
			rsp.MissingBlobDigests = append(rsp.MissingBlobDigests, blobDigest)
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
	rsp.Responses = make([]*repb.BatchUpdateBlobsResponse_Response, 0, len(req.Requests))
	for _, uploadRequest := range req.Requests {
		hash, err := digest.Validate(uploadRequest.Digest)
		if err != nil {
			return nil, err
		}
		if hash == digest.EmptyHash {
			continue
		}
		ck, err := perms.UserPrefixCacheKey(ctx, s.env, uploadRequest.Digest.Hash)
		if err != nil {
			return nil, err
		}
		if err := s.cache.Set(ctx, ck, uploadRequest.Data); err != nil {
			return nil, err
		}
		rsp.Responses = append(rsp.Responses, &repb.BatchUpdateBlobsResponse_Response{
			Digest: uploadRequest.Digest,
			Status: &status.Status{Code: int32(codes.OK)},
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
	rsp.Responses = make([]*repb.BatchReadBlobsResponse_Response, 0, len(req.Digests))
	prefix, err := perms.UserPrefixCacheKey(ctx, s.env, "")
	if err != nil {
		return nil, err
	}
	for _, readDigest := range req.Digests {
		hash, err := digest.Validate(readDigest)
		if err != nil {
			return nil, err
		}
		blobRsp := &repb.BatchReadBlobsResponse_Response{Digest: readDigest}
		blob, err := s.cache.Get(ctx, prefix+hash)
		if err == nil {
			blobRsp.Data = blob
			blobRsp.Status = &status.Status{Code: int32(codes.OK)}
		} else if os.IsNotExist(err) {
			blobRsp.Status = &status.Status{Code: int32(codes.NotFound)}
		} else {
			blobRsp.Status = &status.Status{Code: int32(codes.Internal)}
		}
		rsp.Responses = append(rsp.Responses, blobRsp)
	}
	return rsp, nil
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
	prefix, err := perms.UserPrefixCacheKey(stream.Context(), s.env, "")
	if err != nil {
		return err
	}
	fetchDir := func(reqDigest *repb.Digest) (*repb.Directory, error) {
		hash, err := digest.Validate(reqDigest)
		if err != nil {
			return nil, err
		}
		// Fetch the "Directory" object which enumerates all the blobs in the directory
		blob, err := s.cache.Get(stream.Context(), prefix+hash)
		if err != nil {
			return nil, err
		}

		dir := &repb.Directory{}
		if err := proto.Unmarshal(blob, dir); err != nil {
			return nil, err
		}
		return dir, nil
	}

	rsp := &repb.GetTreeResponse{}
	var fetchDirFn func(dir *repb.Directory) error
	fetchDirFn = func(dir *repb.Directory) error {
		rsp.Directories = append(rsp.Directories, dir)
		for _, dirNode := range dir.Directories {
			subDir, err := fetchDir(dirNode.Digest)
			if err != nil {
				return err
			}
			if err := fetchDirFn(subDir); err != nil {
				return err
			}
		}
		return nil
	}

	rootDir, err := fetchDir(req.RootDigest)
	if err != nil {
		return err
	}
	if err := fetchDirFn(rootDir); err != nil {
		return err
	}
	stream.Send(rsp)
	return nil

}
