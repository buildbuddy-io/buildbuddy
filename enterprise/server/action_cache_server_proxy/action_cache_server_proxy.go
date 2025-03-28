package action_cache_server_proxy

import (
	"bytes"
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	gstatus "google.golang.org/grpc/status"
)

var (
	cacheActionResults = flag.Bool("cache_proxy.cache_action_results", false, "If true, the proxy will cache ActionCache.GetActionResult responses.")
	actionCacheSalt    = flag.String("cache_proxy.action_cache_salt", "actioncache-170325", "A salt to reset action cache contents when needed.")
)

type ActionCacheServerProxy struct {
	env         environment.Env
	localCache  interfaces.Cache
	remoteCache repb.ActionCacheClient
}

func Register(env *real_environment.RealEnv) error {
	actionCacheServer, err := NewActionCacheServerProxy(env)
	if err != nil {
		return status.InternalErrorf("Error initializing ActionCacheServerProxy: %s", err)
	}
	env.SetActionCacheServer(actionCacheServer)
	return nil
}

func NewActionCacheServerProxy(env environment.Env) (*ActionCacheServerProxy, error) {
	remoteCache := env.GetActionCacheClient()
	if remoteCache == nil {
		return nil, fmt.Errorf("An ActionCacheClient is required to enable the ActionCacheServerProxy")
	}
	return &ActionCacheServerProxy{
		env:         env,
		localCache:  env.GetCache(),
		remoteCache: remoteCache,
	}, nil
}

func (s *ActionCacheServerProxy) getACKeyForGetActionResultRequest(req *repb.GetActionResultRequest) (*digest.ACResourceName, error) {
	hashBytes, err := req.MarshalVT()
	if err != nil {
		return nil, err
	}
	hashBytes = append(hashBytes, []byte(*actionCacheSalt)...)
	d, err := digest.Compute(bytes.NewReader(hashBytes), req.GetDigestFunction())
	if err != nil {
		return nil, err
	}
	return digest.NewACResourceName(d, req.GetInstanceName(), req.GetDigestFunction()), nil
}

func (s *ActionCacheServerProxy) getLocallyCachedActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.Digest, *repb.ActionResult, error) {
	key, err := s.getACKeyForGetActionResultRequest(req)
	if err != nil {
		return nil, nil, err
	}
	ptr := &rspb.ResourceName{}
	err = cachetools.ReadProtoFromAC(ctx, s.localCache, key, ptr)
	if err != nil {
		return nil, nil, err
	}
	casRN, err := digest.CASResourceNameFromProto(ptr)
	if err != nil {
		return nil, nil, err
	}
	out := &repb.ActionResult{}
	err = cachetools.ReadProtoFromCAS(ctx, s.localCache, casRN, out)
	if err != nil {
		return nil, nil, err
	}

	return ptr.GetDigest(), out, nil
}

func (s *ActionCacheServerProxy) cacheActionResultLocally(ctx context.Context, req *repb.GetActionResultRequest, resp *repb.ActionResult) error {
	key, err := s.getACKeyForGetActionResultRequest(req)
	if err != nil {
		return err
	}
	d, err := cachetools.UploadProtoToCAS(ctx, s.localCache, req.GetInstanceName(), req.GetDigestFunction(), resp)
	if err != nil {
		return err
	}
	casRN := digest.NewCASResourceName(d, req.GetInstanceName(), req.GetDigestFunction())
	buf, err := casRN.ToProto().MarshalVT()
	if err != nil {
		return err
	}
	return s.localCache.Set(ctx, key.ToProto(), buf)
}

// Action Cache entries are not content-addressable, so the value pointed to
// by a given key may change in the backing cache. Thus, we always send a
// request to the authoritative cache, but send a hash of the last value we
// received to avoid transferring data on unmodified actions.
func (s *ActionCacheServerProxy) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.ActionResult, error) {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}

	// First, see if we have a local copy of this ActionResult.
	var local *repb.ActionResult
	if *cacheActionResults {
		var err error
		localDigest, localResult, err := s.getLocallyCachedActionResult(ctx, req)
		if err != nil && !status.IsNotFoundError(err) {
			return nil, err
		}
		if localDigest != nil {
			// See if remote matches our locally-cached result.
			req.CachedActionResultDigest = localDigest
			local = localResult
		}
	}

	resp, err := s.remoteCache.GetActionResult(ctx, req)
	labels := prometheus.Labels{
		metrics.StatusLabel: fmt.Sprintf("%d", gstatus.Code(err)),
	}

	// The response indicates that our cached value is valid; use it.
	if *cacheActionResults && req.GetCachedActionResultDigest().GetHash() != "" &&
		proto.Equal(req.GetCachedActionResultDigest(), resp.GetActionResultDigest()) {
		resp = local
		labels[metrics.CacheHitMissStatus] = "hit"
	} else {
		if *cacheActionResults && err == nil && resp != nil {
			s.cacheActionResultLocally(ctx, req, resp)
		}
		labels[metrics.CacheHitMissStatus] = "miss"
	}

	metrics.ActionCacheProxiedReadRequests.With(labels).Inc()
	metrics.ActionCacheProxiedReadByes.With(labels).Add(float64(proto.Size(resp)))
	return resp, err
}

// Action Cache entries are not content-addressable, so the value pointed to
// by a given key may change in the backing cache. Thus, don't cache them
// locally when writing to the authoritative cache.
func (s *ActionCacheServerProxy) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	resp, err := s.remoteCache.UpdateActionResult(ctx, req)
	labels := prometheus.Labels{
		metrics.StatusLabel:        fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.CacheHitMissStatus: "miss",
	}
	metrics.ActionCacheProxiedWriteRequests.With(labels).Inc()
	metrics.ActionCacheProxiedWriteByes.With(labels).Add(float64(proto.Size(req)))
	return resp, err
}
