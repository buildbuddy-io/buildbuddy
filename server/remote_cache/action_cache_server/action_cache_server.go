package action_cache_server

import (
	"context"
	"fmt"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

type ActionCacheServer struct {
	env   environment.Env
	cache interfaces.Cache
}

func Register(env environment.Env) error {
	// OPTIONAL CACHE API -- only enable if configured.
	if env.GetCache() == nil {
		return nil
	}
	actionCacheServer, err := NewActionCacheServer(env)
	if err != nil {
		return status.InternalErrorf("Error initializing ActionCacheServer: %s", err)
	}
	env.SetActionCacheServer(actionCacheServer)
	return nil
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

func checkFilesExist(ctx context.Context, cache interfaces.Cache, digests []*rspb.ResourceName) error {
	missing, err := cache.FindMissing(ctx, digests)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return status.NotFoundErrorf("ActionResult output file: '%s' not found in cache", missing[0])
	}
	return nil
}

func ValidateActionResult(ctx context.Context, cache interfaces.Cache, remoteInstanceName string, r *repb.ActionResult) error {
	outputFileDigests := make([]*rspb.ResourceName, 0, len(r.OutputFiles))
	mu := &sync.Mutex{}
	appendDigest := func(d *repb.Digest) {
		if d != nil && d.GetSizeBytes() > 0 {
			mu.Lock()
			rn := digest.NewResourceName(d, remoteInstanceName, rspb.CacheType_CAS).ToProto()
			outputFileDigests = append(outputFileDigests, rn)
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
			rn := digest.NewResourceName(dc.GetTreeDigest(), remoteInstanceName, rspb.CacheType_CAS).ToProto()
			blob, err := cache.Get(gCtx, rn)
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

	return checkFilesExist(ctx, cache, outputFileDigests)
}

func setWorkerMetadata(ar *repb.ActionResult) error {
	if ar.ExecutionMetadata == nil {
		ar.ExecutionMetadata = &repb.ExecutedActionMetadata{
			// This will return the Host ID in the normal case and
			// fall back to a safe ID if permissions don't allow a
			// Host ID. Don't warn in that case because this func is
			// called on every UpdateActionResult, and the
			// consequences of this ID changing are none.
			Worker: uuid.GetFailsafeHostID(),
		}
	}
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
	_, err := digest.Validate(req.ActionDigest)
	if err != nil {
		return nil, err
	}
	ctx, err = prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}

	ht := hit_tracker.NewHitTracker(ctx, s.env, true)
	// Fetch the "ActionResult" object which enumerates all the files in the action.
	d := req.GetActionDigest()
	downloadTracker := ht.TrackDownload(d)
	blob, err := s.cache.Get(ctx, &rspb.ResourceName{
		Digest:       d,
		CacheType:    rspb.CacheType_AC,
		InstanceName: req.GetInstanceName(),
	})
	if err != nil {
		ht.TrackMiss(d)
		return nil, status.NotFoundErrorf("ActionResult (%s) not found: %s", d, err)
	}
	defer downloadTracker.CloseWithBytesTransferred(int64(len(blob)), int64(len(blob)), repb.Compressor_IDENTITY, "ac_server")

	rsp := &repb.ActionResult{}
	if err := proto.Unmarshal(blob, rsp); err != nil {
		return nil, err
	}
	if err := ValidateActionResult(ctx, s.cache, req.GetInstanceName(), rsp); err != nil {
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
//   - `INVALID_ARGUMENT`: One or more arguments are invalid.
//   - `FAILED_PRECONDITION`: One or more errors occurred in updating the
//     action result, such as a missing command or action.
//   - `RESOURCE_EXHAUSTED`: There is insufficient storage space to add the
//     entry to the cache.
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

	canWrite, err := capabilities.IsGranted(ctx, s.env, akpb.ApiKey_CACHE_WRITE_CAPABILITY)
	if err != nil {
		return nil, err
	}
	// For read-only API keys, pretend the request succeeded so bazel doesn't error out.
	if !canWrite {
		return req.ActionResult, nil
	}

	ht := hit_tracker.NewHitTracker(ctx, s.env, true)
	d := req.GetActionDigest()
	acResource := digest.NewResourceName(d, req.GetInstanceName(), rspb.CacheType_AC)
	uploadTracker := ht.TrackUpload(d)

	// Context: https://github.com/bazelbuild/remote-apis/pull/131
	// More: https://github.com/buchgr/bazel-remote/commit/7de536f47bf163fb96bc1e38ffd5e444e2bcaa00
	if err := setWorkerMetadata(req.ActionResult); err != nil {
		return nil, err
	}

	blob, err := proto.Marshal(req.ActionResult)
	if err != nil {
		return nil, err
	}

	if err := s.cache.Set(ctx, acResource.ToProto(), blob); err != nil {
		return nil, err
	}
	uploadTracker.CloseWithBytesTransferred(int64(len(blob)), int64(len(blob)), repb.Compressor_IDENTITY, "ac_server")
	return req.ActionResult, nil
}
