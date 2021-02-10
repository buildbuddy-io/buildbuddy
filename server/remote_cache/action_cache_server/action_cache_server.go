package action_cache_server

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/namespace"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

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

func (s *ActionCacheServer) getCache(instanceName string) interfaces.Cache {
	return namespace.ActionCache(s.cache, instanceName)
}

func (s *ActionCacheServer) getCASCache(instanceName string) interfaces.Cache {
	return namespace.CASCache(s.cache, instanceName)
}

func (s *ActionCacheServer) checkFilesExist(ctx context.Context, cache interfaces.Cache, digests []*repb.Digest) error {
	foundMap, err := cache.ContainsMulti(ctx, digests)
	if err != nil {
		return err
	}
	for _, d := range digests {
		found, ok := foundMap[d]
		if !ok {
			return status.InternalErrorf("AC Inconsistent result from cache.ContainsMulti (missing %q)", d)
		}
		if !found {
			return status.NotFoundErrorf("ActionResult output file: '%s' not found in cache", d)
		}
	}
	return nil
}

func (s *ActionCacheServer) validateActionResult(ctx context.Context, cache interfaces.Cache, r *repb.ActionResult) error {
	outputFileDigests := make([]*repb.Digest, 0, len(r.OutputFiles))
	mu := &sync.Mutex{}
	appendDigest := func(d *repb.Digest) {
		if d != nil && d.GetSizeBytes() > 0 {
			mu.Lock()
			outputFileDigests = append(outputFileDigests, d)
			mu.Unlock()
		}
	}
	for _, f := range r.OutputFiles {
		appendDigest(f.GetDigest())
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, d := range r.OutputDirectories {
		dc := d
		g.Go(func() error {
			blob, err := cache.Get(gCtx, dc.GetTreeDigest())
			if err != nil {
				return err
			}
			tree := &repb.Tree{}
			if err := proto.Unmarshal(blob, tree); err != nil {
				return err
			}
			for _, f := range tree.GetRoot().GetFiles() {
				appendDigest(f.GetDigest())
			}
			for _, dir := range tree.GetChildren() {
				for _, f := range dir.GetFiles() {
					appendDigest(f.GetDigest())
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	return s.checkFilesExist(ctx, cache, outputFileDigests)
}

func setWorkerMetadata(ar *repb.ActionResult) {
	if ar.ExecutionMetadata == nil {
		ar.ExecutionMetadata = &repb.ExecutedActionMetadata{
			Worker: base64.StdEncoding.EncodeToString(uuid.NodeID()),
		}
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
	_, err := digest.Validate(req.ActionDigest)
	if err != nil {
		return nil, err
	}
	ctx, err = prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}

	cache := s.getCache(req.GetInstanceName())
	casCache := s.getCASCache(req.GetInstanceName())

	ht := hit_tracker.NewHitTracker(ctx, s.env, true)
	// Fetch the "ActionResult" object which enumerates all the files in the action.
	d := req.GetActionDigest()
	downloadTracker := ht.TrackDownload(d)
	blob, err := cache.Get(ctx, d)
	if err != nil {
		ht.TrackMiss(d)
		return nil, status.NotFoundErrorf("ActionResult (%s) not found: %s", d, err)
	}
	defer downloadTracker.Close()

	rsp := &repb.ActionResult{}
	if err := proto.Unmarshal(blob, rsp); err != nil {
		return nil, err
	}
	if err := s.validateActionResult(ctx, casCache, rsp); err != nil {
		return nil, status.NotFoundErrorf("ActionResult (%s) not found: %s", d, err)
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
	_, err := digest.Validate(req.GetActionDigest())
	if err != nil {
		return nil, err
	}
	ctx, err = prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}
	ht := hit_tracker.NewHitTracker(ctx, s.env, true)
	d := req.GetActionDigest()
	uploadTracker := ht.TrackUpload(d)
	cache := s.getCache(req.GetInstanceName())

	// Context: https://github.com/bazelbuild/remote-apis/pull/131
	// More: https://github.com/buchgr/bazel-remote/commit/7de536f47bf163fb96bc1e38ffd5e444e2bcaa00
	setWorkerMetadata(req.ActionResult)

	blob, err := proto.Marshal(req.ActionResult)
	if err != nil {
		return nil, err
	}

	if err := cache.Set(ctx, d, blob); err != nil {
		return nil, err
	}
	uploadTracker.Close()
	return req.ActionResult, nil
}
