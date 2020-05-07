package action_cache_server

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type ActionCacheServer struct {
	env   environment.Env
	cache interfaces.Cache
}

func NewActionCacheServer(env environment.Env) (*ActionCacheServer, error) {
	cache := env.GetCache()
	if cache == nil {
		return nil, fmt.Errorf("A cache is required to enable the ActionCacheServer")
	}
	return &ActionCacheServer{
		env:   env,
		cache: cache,
	}, nil
}

func (s *ActionCacheServer) checkFileExists(ctx context.Context, hash string) error {
	ck, err := perms.UserPrefixCacheKey(ctx, s.env, hash)
	if err != nil {
		return err
	}
	ok, err := s.cache.Contains(ctx, ck)
	if err != nil {
		return err
	}
	if !ok {
		return status.NotFoundError(fmt.Sprintf("ActionResult output file: '%s' not found in cache", hash))
	}
	return nil
}

func (s *ActionCacheServer) checkDirExists(ctx context.Context, dir *repb.Directory) error {
	for _, f := range dir.GetFiles() {
		if f.Digest == nil {
			continue
		}
		if err := s.checkFileExists(ctx, f.Digest.Hash); err != nil {
			return err
		}
	}
	return nil
}

func (s *ActionCacheServer) validateActionResult(ctx context.Context, r *repb.ActionResult) error {
	for _, f := range r.OutputFiles {
		if len(f.Contents) > 0 && f.GetDigest().GetSizeBytes() > 0 {
			if err := s.checkFileExists(ctx, f.Digest.Hash); err != nil {
				return err
			}
		}
	}

	// Compute the user prefix just once, rather than in a loop, below.
	userPrefix, err := perms.UserPrefixCacheKey(ctx, s.env, "")
	if err != nil {
		return err
	}

	for _, d := range r.OutputDirectories {
		blob, err := s.cache.Get(ctx, userPrefix+d.GetTreeDigest().Hash)
		if err != nil {
			return err
		}
		tree := &repb.Tree{}
		if err := proto.Unmarshal(blob, tree); err != nil {
			return err
		}
		if err := s.checkDirExists(ctx, tree.Root); err != nil {
			return err
		}

		for _, childDir := range tree.GetChildren() {
			if err := s.checkDirExists(ctx, childDir); err != nil {
				return err
			}
		}
	}
	return nil
}

func setWorkerMetadata(ar *repb.ActionResult) {
	ar.ExecutionMetadata = &repb.ExecutedActionMetadata{
		Worker: "worker",
	}
}

// Retrieve a cached execution result.
//
// Implementations SHOULD ensure that any blobs referenced from the
// [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage]
// are available at the time of returning the
// [ActionResult][build.bazel.remote.execution.v2.ActionResult] and will be
// for some period of time afterwards. The TTLs of the referenced blobs SHOULD be increased
// if necessary and applicable.
//
// Errors:
//
// * `NOT_FOUND`: The requested `ActionResult` is not in the cache.
func (s *ActionCacheServer) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.ActionResult, error) {
	if req.ActionDigest == nil {
		return nil, status.InvalidArgumentError("ActionDigest is a required field")
	}
	hash, err := digest.Validate(req.ActionDigest)
	if err != nil {
		return nil, err
	}

	ck, err := perms.UserPrefixCacheKey(ctx, s.env, hash)
	if err != nil {
		return nil, err
	}
	// Fetch the "ActionResult" object which enumerates all the files in the action.
	blob, err := s.cache.Get(ctx, ck)
	if err != nil {
		return nil, status.NotFoundError(fmt.Sprintf("ActionResult (%s) not found: %s", hash, err))
	}

	rsp := &repb.ActionResult{}
	if err := proto.Unmarshal(blob, rsp); err != nil {
		return nil, err
	}
	if err := s.validateActionResult(ctx, rsp); err != nil {
		return nil, status.NotFoundError(fmt.Sprintf("ActionResult (%s) not found: %s", hash, err))
	}
	return rsp, nil
}

// Upload a new execution result.
//
// In order to allow the server to perform access control based on the type of
// action, and to assist with client debugging, the client MUST first upload
// the [Action][build.bazel.remote.execution.v2.Execution] that produced the
// result, along with its
// [Command][build.bazel.remote.execution.v2.Command], into the
// `ContentAddressableStorage`.
//
// Errors:
//
// * `INVALID_ARGUMENT`: One or more arguments are invalid.
// * `FAILED_PRECONDITION`: One or more errors occurred in updating the
//   action result, such as a missing command or action.
// * `RESOURCE_EXHAUSTED`: There is insufficient storage space to add the
//   entry to the cache.
func (s *ActionCacheServer) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	if req.ActionDigest == nil {
		return nil, status.InvalidArgumentError("ActionDigest is a required field")
	}

	if req.ActionResult == nil {
		return nil, status.InvalidArgumentError("ActionResult is a required field")
	}

	hash, err := digest.Validate(req.ActionDigest)
	if err != nil {
		return nil, err
	}

	// Context: https://github.com/bazelbuild/remote-apis/pull/131
	// More: https://github.com/buchgr/bazel-remote/commit/7de536f47bf163fb96bc1e38ffd5e444e2bcaa00
	setWorkerMetadata(req.ActionResult)

	blob, err := proto.Marshal(req.ActionResult)
	if err != nil {
		return nil, err
	}

	ck, err := perms.UserPrefixCacheKey(ctx, s.env, hash)
	if err != nil {
		return nil, err
	}

	if err := s.cache.Set(ctx, ck, blob); err != nil {
		return nil, err
	}
	return req.ActionResult, nil
}
