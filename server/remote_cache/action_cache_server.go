package action_cache_server

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"

	repb "proto/remote_execution"
)

type ActionCacheServer struct {
	cache interfaces.Cache
}

func NewActionCacheServer(env environment.Env) (*ActionCacheServer, error) {
	cache := env.GetCache()
	if cache == nil {
		return nil, fmt.Errorf("A cache is required to enable the ActionCacheServer")
	}
	return &ActionCacheServer{
		cache: cache,
	}, nil
}

func (s *ActionCacheServer) validateActionResult(r *repb.ActionResult) error {
	// TODO
	return nil
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

	// Fetch the "ActionResult" object which enumerates all the files in the action.
	blob, err := s.cache.Get(ctx, hash)
	if err != nil {
		return nil, status.NotFoundError(fmt.Sprintf("ActionResult (%s) not found: %s", hash, err))
		return nil, err
	}

	rsp := &repb.ActionResult{}
	if err := proto.Unmarshal(blob, rsp); err != nil {
		return nil, err
	}
	if err := s.validateActionResult(rsp); err != nil {
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

	blob, err := proto.Marshal(req.ActionResult)
	if err != nil {
		return nil, err
	}

	if err := s.cache.Set(ctx, hash, blob); err != nil {
		return nil, err
	}
	return req.ActionResult, nil
}
