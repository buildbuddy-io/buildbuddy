package action_cache_server_proxy

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	gstatus "google.golang.org/grpc/status"
)

var (
	cacheActionResults = flag.Bool("cache_proxy.cache_action_results", false, "If true, the proxy will cache ActionCache.GetActionResult responses.")
	actionCacheSalt    = flag.String("cache_proxy.action_cache_salt", "actioncache-170401", "A salt to reset action cache contents when needed.")
)

type ActionCacheServerProxy struct {
	env           environment.Env
	authenticator interfaces.Authenticator
	localCache    interfaces.Cache
	remoteCache   repb.ActionCacheClient
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
		env:           env,
		authenticator: env.GetAuthenticator(),
		localCache:    env.GetCache(),
		remoteCache:   remoteCache,
	}, nil
}

// TODO: Reinstate
func getACKeyForGetActionResultRequest(req *repb.GetActionResultRequest) (*digest.ACResourceName, error) {
	//hashBytes, err := proto.Marshal(req)
	//if err != nil {
	//	return nil, err
	//}
	//hashBytes = append(hashBytes, []byte(*actionCacheSalt)...)
	//d, err := digest.Compute(bytes.NewReader(hashBytes), req.GetDigestFunction())
	//if err != nil {
	//	return nil, err
	//}
	//return digest.NewACResourceName(d, req.GetInstanceName(), req.GetDigestFunction()), nil
	return digest.NewACResourceName(req.GetActionDigest(), req.GetInstanceName(), req.GetDigestFunction()), nil
}

func (s *ActionCacheServerProxy) getLocallyCachedActionResult(ctx context.Context, key *digest.ACResourceName) (*repb.Digest, *repb.ActionResult, error) {
	//ptr := &rspb.ResourceName{}
	//if err := cachetools.ReadProtoFromAC(ctx, s.localCache, key, ptr); err != nil {
	//	return nil, nil, err
	//}
	//casRN, err := digest.CASResourceNameFromProto(ptr)
	//if err != nil {
	//	return nil, nil, err
	//}
	//out := &repb.ActionResult{}
	//err = cachetools.ReadProtoFromCAS(ctx, s.localCache, casRN, out)
	//if err != nil {
	//	return nil, nil, err
	//}
	//
	//return ptr.GetDigest(), out, nil
	blob, err := s.localCache.Get(ctx, key.ToProto())
	if err != nil {
		return nil, nil, status.NotFoundErrorf("ActionResult (%s) not found: %s", key.GetDigest(), err)
	}

	rsp := &repb.ActionResult{}
	if err := proto.Unmarshal(blob, rsp); err != nil {
		return nil, nil, err
	}
	return key.GetDigest(), rsp, nil
}

func (s *ActionCacheServerProxy) cacheActionResultLocally(ctx context.Context, key *digest.ACResourceName, req *repb.GetActionResultRequest, resp *repb.ActionResult) error {
	d, err := cachetools.UploadProtoToCAS(ctx, s.localCache, req.GetInstanceName(), req.GetDigestFunction(), resp)
	if err != nil {
		return err
	}
	casRN := digest.NewCASResourceName(d, req.GetInstanceName(), req.GetDigestFunction())
	buf, err := proto.Marshal(casRN.ToProto())
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
	if authutil.EncryptionEnabled(ctx, s.authenticator) {
		resp, err := s.remoteCache.GetActionResult(ctx, req)
		labels := prometheus.Labels{
			metrics.StatusLabel:        fmt.Sprintf("%d", gstatus.Code(err)),
			metrics.CacheHitMissStatus: metrics.UncacheableStatusLabel,
		}
		metrics.ActionCacheProxiedReadRequests.With(labels).Inc()
		metrics.ActionCacheProxiedReadBytes.With(labels).Add(float64(proto.Size(resp)))
		return resp, err
	}

	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}
	// TODO: Put the constant somewhere accessible
	md := metadata.ValueFromIncomingContext(ctx, "proxy_skip_remote")
	skipRemote := len(md) > 0 && md[0] == "true"

	// First, see if we have a local copy of this ActionResult.
	var local *repb.ActionResult
	var localKey *digest.ACResourceName
	if *cacheActionResults {
		localKey, err = getACKeyForGetActionResultRequest(req)
		if err != nil {
			return nil, err
		}
		var err error
		localDigest, localResult, err := s.getLocallyCachedActionResult(ctx, localKey)
		if skipRemote {
			return localResult, err
		}
		if err != nil && !status.IsNotFoundError(err) {
			return nil, err
		}
		if localDigest != nil {
			// See if remote matches our locally-cached result.
			req.CachedActionResultDigest = localDigest
			local = localResult
		}
	}

	var resp *repb.ActionResult
	if skipRemote {
		resp = local
	} else {
		resp, err := s.remoteCache.GetActionResult(ctx, req)
		labels := prometheus.Labels{
			metrics.StatusLabel: fmt.Sprintf("%d", gstatus.Code(err)),
		}

		// The response indicates that our cached value is valid; use it.
		if *cacheActionResults && req.GetCachedActionResultDigest().GetHash() != "" &&
			proto.Equal(req.GetCachedActionResultDigest(), resp.GetActionResultDigest()) {
			resp = local
			// TODO: Fix the labels if local only
			labels[metrics.CacheHitMissStatus] = metrics.HitStatusLabel
		} else {
			if *cacheActionResults && err == nil && resp != nil {
				s.cacheActionResultLocally(ctx, localKey, req, resp)
			}
			labels[metrics.CacheHitMissStatus] = metrics.MissStatusLabel
		}
		metrics.ActionCacheProxiedReadRequests.With(labels).Inc()
		metrics.ActionCacheProxiedReadBytes.With(labels).Add(float64(proto.Size(resp)))
	}

	return resp, err
}

// TODO: Better understand why we aren't writing locally now
// Action Cache entries are not content-addressable, so the value pointed to
// by a given key may change in the backing cache. Thus, don't cache them
// locally when writing to the authoritative cache.
func (s *ActionCacheServerProxy) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	md := metadata.ValueFromIncomingContext(ctx, "proxy_skip_remote")
	skipRemote := len(md) > 0 && md[0] == "true"
	if skipRemote {
		// TODO: Do we need to add some of the validation from remote
		blob, err := proto.Marshal(req.ActionResult)
		if err != nil {
			return nil, err
		}

		d := req.GetActionDigest()
		acResource := digest.NewResourceName(d, req.GetInstanceName(), rspb.CacheType_AC, req.GetDigestFunction())
		if err := s.localCache.Set(ctx, acResource.ToProto(), blob); err != nil {
			return nil, err
		}
		return req.ActionResult, nil
	}

	resp, err := s.remoteCache.UpdateActionResult(ctx, req)
	labels := prometheus.Labels{
		metrics.StatusLabel:        fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.CacheHitMissStatus: metrics.MissStatusLabel,
	}
	metrics.ActionCacheProxiedWriteRequests.With(labels).Inc()
	metrics.ActionCacheProxiedWriteBytes.With(labels).Add(float64(proto.Size(req)))
	return resp, err
}
