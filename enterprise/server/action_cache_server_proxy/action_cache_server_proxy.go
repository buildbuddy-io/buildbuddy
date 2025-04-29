package action_cache_server_proxy

import (
	"bytes"
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
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

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	gstatus "google.golang.org/grpc/status"
)

var (
	cacheActionResults = flag.Bool("cache_proxy.cache_action_results", false, "If true, the proxy will cache ActionCache.GetActionResult responses.")
	actionCacheSalt    = flag.String("cache_proxy.action_cache_salt", "actioncache-170401", "A salt to reset action cache contents when needed.")
)

type ActionCacheServerProxy struct {
	env            environment.Env
	authenticator  interfaces.Authenticator
	localCache     interfaces.Cache
	remoteACClient repb.ActionCacheClient
	localACServer  repb.ActionCacheServer
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
		env:            env,
		authenticator:  env.GetAuthenticator(),
		localCache:     env.GetCache(),
		remoteACClient: remoteCache,
		localACServer:  env.GetLocalActionCacheServer(),
	}, nil
}

func getACKeyForGetActionResultRequest(req *repb.GetActionResultRequest) (*digest.ACResourceName, error) {
	hashBytes, err := proto.Marshal(req)
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

// getActionResultFromLocalCAS returns a locally cached action result saved via
// `cacheActionResultToLocalCAS`.
//
// This function should only be used when also fetching a remote action result.
// Unlike typical action cache implementations, it doesn't validate that all blobs
// referenced from the CAS are still present. It relies on the remote call to do any
// validation. It also fetches the action result from the CAS instead of the AC,
// so there is digest validation on its value.
// The remote result is always the source of truth. If the values do not
// match, the local result should be discarded.
func (s *ActionCacheServerProxy) getActionResultFromLocalCAS(ctx context.Context, key *digest.ACResourceName) (*repb.Digest, *repb.ActionResult, error) {
	// NOTE: To avoid double-counting AC hits, we deliberately don't track
	// download for these reads.  The remote server will count the full response
	// size when checking the cached value.
	ptr := &rspb.ResourceName{}
	if err := cachetools.ReadProtoFromAC(ctx, s.localCache, key, ptr); err != nil {
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

// cacheActionResultToLocalCAS stores the ActionResult `resp` in the CAS and then
// creates an AC entry that points from `acKey` to the generated CAS key.
// In the cache: { `acKey` => CAS key, CAS key => `resp` }
//
// Typically, the contents fetched from an action cache key do not have to match the
// digest, while for the CAS we validate that the contents do match the digest.
// Storing the ActionResult in the CAS lets us generate a digest we can validate.
//
// Action results saved via this function should be fetched with
// `getActionResultFromLocalCAS`.
func (s *ActionCacheServerProxy) cacheActionResultToLocalCAS(ctx context.Context, acKey *digest.ACResourceName, req *repb.GetActionResultRequest, resp *repb.ActionResult) error {
	d, err := cachetools.UploadProtoToCAS(ctx, s.localCache, req.GetInstanceName(), req.GetDigestFunction(), resp)
	if err != nil {
		return err
	}
	casRN := digest.NewCASResourceName(d, req.GetInstanceName(), req.GetDigestFunction())
	buf, err := proto.Marshal(casRN.ToProto())
	if err != nil {
		return err
	}
	return s.localCache.Set(ctx, acKey.ToProto(), buf)
}

// Action Cache entries are not content-addressable, so the value pointed to
// by a given key may change in the backing cache. Thus, we always send a
// request to the authoritative cache, but send a hash of the last value we
// received to avoid transferring data on unmodified actions.
func (s *ActionCacheServerProxy) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.ActionResult, error) {
	if authutil.EncryptionEnabled(ctx, s.authenticator) {
		resp, err := s.remoteACClient.GetActionResult(ctx, req)
		labels := prometheus.Labels{
			metrics.StatusLabel:           fmt.Sprintf("%d", gstatus.Code(err)),
			metrics.CacheHitMissStatus:    metrics.UncacheableStatusLabel,
			metrics.CacheProxyRequestType: proxy_util.RequestTypeLabelFromContext(ctx),
		}
		metrics.ActionCacheProxiedReadRequests.With(labels).Inc()
		metrics.ActionCacheProxiedReadBytes.With(labels).Add(float64(proto.Size(resp)))
		return resp, err
	}

	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env.GetAuthenticator())
	if err != nil {
		return nil, err
	}

	if proxy_util.SkipRemote(ctx) {
		rsp, err := s.localACServer.GetActionResult(ctx, req)

		cacheStatus := metrics.MissStatusLabel
		if err == nil {
			cacheStatus = metrics.HitStatusLabel
		}
		labels := prometheus.Labels{
			metrics.StatusLabel:           fmt.Sprintf("%d", gstatus.Code(err)),
			metrics.CacheHitMissStatus:    cacheStatus,
			metrics.CacheProxyRequestType: metrics.LocalOnlyCacheProxyRequestLabel,
		}
		metrics.ActionCacheProxiedReadRequests.With(labels).Inc()
		metrics.ActionCacheProxiedReadBytes.With(labels).Add(float64(proto.Size(rsp)))

		return rsp, err
	}

	// If using the remote cache as the source of truth, we must validate that
	// any locally stored action result matches that stored remotely.
	var local *repb.ActionResult
	var localKey *digest.ACResourceName
	if *cacheActionResults {
		localKey, err = getACKeyForGetActionResultRequest(req)
		if err != nil {
			return nil, err
		}
		var err error
		localDigest, localResult, err := s.getActionResultFromLocalCAS(ctx, localKey)
		if err != nil && !status.IsNotFoundError(err) {
			return nil, err
		}
		if localDigest != nil {
			// When `CachedActionResultDigest` is set, the remote AC server
			// will validate whether its cached result has the same digest as
			// `CachedActionResultDigest`. If they match, it will not send
			// the full contents of the action result back, in order to reduce
			// network transfer.
			req.CachedActionResultDigest = localDigest
			local = localResult
		}
	}

	resp, err := s.remoteACClient.GetActionResult(ctx, req)
	labels := prometheus.Labels{
		metrics.StatusLabel:           fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.CacheProxyRequestType: metrics.DefaultCacheProxyRequestLabel,
	}

	// If `ActionResultDigest` is set on the response, the contents stored
	// in the remote AC key match those stored locally, and the remote server
	// will not send back the full contents of the action result. Return
	// the locally cached value.
	if *cacheActionResults && req.GetCachedActionResultDigest().GetHash() != "" &&
		proto.Equal(req.GetCachedActionResultDigest(), resp.GetActionResultDigest()) {
		resp = local
		labels[metrics.CacheHitMissStatus] = metrics.HitStatusLabel
	} else {
		if *cacheActionResults && err == nil && resp != nil {
			s.cacheActionResultToLocalCAS(ctx, localKey, req, resp)
		}
		labels[metrics.CacheHitMissStatus] = metrics.MissStatusLabel
	}

	metrics.ActionCacheProxiedReadRequests.With(labels).Inc()
	metrics.ActionCacheProxiedReadBytes.With(labels).Add(float64(proto.Size(resp)))
	return resp, err
}

func (s *ActionCacheServerProxy) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	// Only if it's explicitly requested do we cache AC results locally.
	if proxy_util.SkipRemote(ctx) && !authutil.EncryptionEnabled(ctx, s.authenticator) {
		resp, err := s.localACServer.UpdateActionResult(ctx, req)

		labels := prometheus.Labels{
			metrics.StatusLabel:           fmt.Sprintf("%d", gstatus.Code(err)),
			metrics.CacheHitMissStatus:    metrics.MissStatusLabel,
			metrics.CacheProxyRequestType: metrics.LocalOnlyCacheProxyRequestLabel,
		}
		metrics.ActionCacheProxiedWriteRequests.With(labels).Inc()
		metrics.ActionCacheProxiedWriteBytes.With(labels).Add(float64(proto.Size(req)))

		return resp, err
	}

	// By default, we use the remote cache as the source of truth for AC results.
	// Action Cache entries are not content-addressable, so the value pointed to
	// by a given key may change. Cache proxies in different clusters could have
	// different values stored locally, so simplify by always using the remote value.
	// Thus, don't cache action results locally by default.
	resp, err := s.remoteACClient.UpdateActionResult(ctx, req)
	labels := prometheus.Labels{
		metrics.StatusLabel:           fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.CacheHitMissStatus:    metrics.MissStatusLabel,
		metrics.CacheProxyRequestType: proxy_util.RequestTypeLabelFromContext(ctx),
	}
	metrics.ActionCacheProxiedWriteRequests.With(labels).Inc()
	metrics.ActionCacheProxiedWriteBytes.With(labels).Add(float64(proto.Size(req)))
	return resp, err
}
