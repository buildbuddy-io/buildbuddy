package action_cache_server_proxy

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_crypter"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/metadata"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

var (
	cacheActionResults = flag.Bool("cache_proxy.cache_action_results", true, "If true, the proxy will cache ActionCache.GetActionResult responses.")
	actionCacheSalt    = flag.String("cache_proxy.action_cache_salt", "actioncache-170401", "A salt to reset action cache contents when needed.")
)

type ActionCacheServerProxy struct {
	supportsEncryption func(context.Context) bool
	env                environment.Env
	authenticator      interfaces.Authenticator
	localCache         interfaces.Cache
	remoteACClient     repb.ActionCacheClient
	localACServer      repb.ActionCacheServer
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
		supportsEncryption: remote_crypter.SupportsEncryption(env),
		env:                env,
		authenticator:      env.GetAuthenticator(),
		localCache:         env.GetCache(),
		remoteACClient:     remoteCache,
		localACServer:      env.GetLocalActionCacheServer(),
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

func isDefaultGetActionResultRequest(req *repb.GetActionResultRequest) bool {
	return proto.Equal(req, &repb.GetActionResultRequest{
		InstanceName:             req.GetInstanceName(),
		ActionDigest:             req.GetActionDigest(),
		DigestFunction:           req.GetDigestFunction(),
		CachedActionResultDigest: req.GetCachedActionResultDigest(),
	})
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
func (s *ActionCacheServerProxy) getActionResultFromLocalCAS(ctx context.Context, key *digest.ACResourceName, fetchMetadata bool) (*repb.Digest, *repb.ActionResult, *interfaces.CacheMetadata, error) {
	// NOTE: To avoid double-counting AC hits, we deliberately don't track
	// download for these reads.  The remote server will count the full response
	// size when checking the cached value.
	ptr := &rspb.ResourceName{}
	var acMD *interfaces.CacheMetadata
	if fetchMetadata {
		data, md, err := s.localCache.GetWithMetadata(ctx, key.ToProto())
		if err != nil {
			return nil, nil, nil, err
		}
		if err := proto.Unmarshal(data, ptr); err != nil {
			return nil, nil, nil, err
		}
		acMD = md
	} else {
		if err := cachetools.ReadProtoFromAC(ctx, s.localCache, key, ptr); err != nil {
			return nil, nil, nil, err
		}
	}
	casRN, err := digest.CASResourceNameFromProto(ptr)
	if err != nil {
		return nil, nil, nil, err
	}
	out := &repb.ActionResult{}
	if err := cachetools.ReadProtoFromCAS(ctx, s.localCache, casRN, out); err != nil {
		return nil, nil, nil, err
	}

	return ptr.GetDigest(), out, acMD, nil
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

func (s *ActionCacheServerProxy) actionCacheTTL(ctx context.Context) time.Duration {
	efp := s.env.GetExperimentFlagProvider()
	if efp == nil {
		return 0
	}
	ttlSeconds := efp.Int64(ctx, "cache_proxy.action_cache_ttl_seconds", 0)
	if ttlSeconds <= 0 {
		return 0
	}
	return time.Duration(ttlSeconds) * time.Second
}

func (s *ActionCacheServerProxy) isLocalActionResultFresh(md *interfaces.CacheMetadata, ttl time.Duration) bool {
	if ttl <= 0 || md == nil || md.LastModifyTimeUsec <= 0 {
		return false
	}

	// Use mtime, not atime: local reads update atime, but TTL freshness
	// depends on when this proxy last validated or updated the AC pointer
	// from the remote app.
	return s.env.GetClock().Now().Sub(time.UnixMicro(md.LastModifyTimeUsec)) < ttl
}

func (s *ActionCacheServerProxy) shouldCacheUpdatedActionResult(ctx context.Context) bool {
	canWrite, err := capabilities.IsGranted(ctx, s.authenticator, cappb.Capability_CACHE_WRITE)
	if err != nil || !canWrite {
		return false
	}
	return s.supportsEncryption(ctx) || !authutil.EncryptionEnabled(ctx, s.authenticator)
}

func (s *ActionCacheServerProxy) recordLocalACHit(ctx context.Context, req *repb.GetActionResultRequest, resp *repb.ActionResult, sizeBytes int64) {
	ctx = authutil.ContextWithCachedAuthHeaders(ctx, s.authenticator)
	md, _ := metadata.FromIncomingContext(ctx)
	md = md.Copy()
	if md == nil {
		md = metadata.MD{}
	}
	// SkipRemote tells the AC hit tracker that the app is not tracking this, so
	// we need to track it ourselves with HitTrackerService. If we don't skip
	// remote, the AC hit won't be tracked in the cache_proxy.
	md.Set(proxy_util.SkipRemoteKey, "true")
	trackCtx := metadata.NewIncomingContext(ctx, md)
	ht := s.env.GetHitTrackerFactory().NewACHitTracker(trackCtx, bazel_request.GetRequestMetadata(trackCtx))
	ht.SetExecutedActionMetadata(resp.GetExecutionMetadata())
	if err := ht.TrackDownload(req.GetActionDigest()).CloseWithBytesTransferred(sizeBytes, sizeBytes, repb.Compressor_IDENTITY, "ac_server"); err != nil {
		log.Debugf("GetActionResult: download tracker error: %s", err)
	}
}

// Action Cache entries are not content-addressable, so the value pointed to
// by a given key may change in the backing cache. If a local result is older
// than the configured TTL, we send a request to the authoritative cache, but
// send a hash of the last value we received to avoid transferring data on
// unmodified actions.
func (s *ActionCacheServerProxy) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.ActionResult, error) {
	if err := authutil.ValidateRestrictedACAccess(ctx, s.env, req.GetInstanceName()); err != nil {
		return nil, err
	}
	if authutil.EncryptionEnabled(ctx, s.authenticator) && !s.supportsEncryption(ctx) {
		resp, err := s.remoteACClient.GetActionResult(ctx, req)
		labels := prometheus.Labels{
			metrics.StatusLabel:           status.MetricsLabel(err),
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
			metrics.StatusLabel:           status.MetricsLabel(err),
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
	ttl := s.actionCacheTTL(ctx)
	if *cacheActionResults {
		localKey, err = getACKeyForGetActionResultRequest(req)
		if err != nil {
			return nil, err
		}
		// Only fetch AC entry metadata when we may serve the TTL fast path.
		// Otherwise the extra Metadata call is wasted work.
		ttlFastPathEligible := isDefaultGetActionResultRequest(req) && ttl > 0
		var err error
		localDigest, localResult, localACMD, err := s.getActionResultFromLocalCAS(ctx, localKey, ttlFastPathEligible)
		if err != nil && !status.IsNotFoundError(err) {
			return nil, err
		}
		if localDigest != nil {
			// TODO: Consider async app revalidation of TTL hits, updating or deleting
			// the local AC entry if the authoritative result changed.
			if ttlFastPathEligible && s.isLocalActionResultFresh(localACMD, ttl) {
				// Skip checking for existence of output files. The app recently
				// validated or updated this result, which refreshed the referenced
				// outputs' atime. With remote_download_minimal, this proxy
				// often won't have every output locally.
				sizeBytes := int64(proto.Size(localResult))
				s.recordLocalACHit(ctx, req, localResult, sizeBytes)
				labels := prometheus.Labels{
					metrics.StatusLabel:           status.MetricsLabel(nil),
					metrics.CacheHitMissStatus:    metrics.HitStatusLabel,
					metrics.CacheProxyRequestType: metrics.DefaultCacheProxyRequestLabel,
				}
				metrics.ActionCacheProxiedReadRequests.With(labels).Inc()
				metrics.ActionCacheProxiedReadBytes.With(labels).Add(float64(sizeBytes))
				return localResult, nil
			}
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
		metrics.StatusLabel:           status.MetricsLabel(err),
		metrics.CacheProxyRequestType: metrics.DefaultCacheProxyRequestLabel,
	}

	// If `ActionResultDigest` is set on the response, the contents stored
	// in the remote AC key match those stored locally, and the remote server
	// will not send back the full contents of the action result. Return
	// the locally cached value.
	// TODO: For proxy-to-proxy requests with CachedActionResultDigest set,
	// preserve hash-only responses, including on the TTL fast path.
	if *cacheActionResults && local != nil && err == nil && resp != nil && req.GetCachedActionResultDigest().GetHash() != "" &&
		proto.Equal(req.GetCachedActionResultDigest(), resp.GetActionResultDigest()) {
		resp = local
		labels[metrics.CacheHitMissStatus] = metrics.HitStatusLabel
		if ttl > 0 {
			casRN := digest.NewCASResourceName(req.GetCachedActionResultDigest(), req.GetInstanceName(), req.GetDigestFunction()).ToProto()
			casRNProto, marshalErr := proto.Marshal(casRN)
			if marshalErr != nil {
				log.CtxInfof(ctx, "Error marshaling CAS resource name for AC entry refresh: %s", marshalErr)
			} else {
				refreshCtx, refreshCancel := background.ExtendContextForFinalization(ctx, 5*time.Second)
				localKeyProto := localKey.ToProto()
				go func() {
					defer refreshCancel()
					// Rewrite the AC pointer to refresh its mtime after the remote app
					// confirmed the result is unchanged.
					err := s.localCache.Set(refreshCtx, localKeyProto, casRNProto)
					if err != nil {
						log.CtxInfof(ctx, "Error setting AC entry during refresh: %s", err)
					}
				}()
			}
		}
	} else {
		if *cacheActionResults && err == nil && resp != nil && resp.GetActionResultDigest().GetHash() == "" {
			// Remote returned the full result, either because the local result
			// was missing or changed; store it locally for subsequent reads.
			s.cacheActionResultToLocalCAS(ctx, localKey, req, resp)
		}
		labels[metrics.CacheHitMissStatus] = metrics.MissStatusLabel
	}

	metrics.ActionCacheProxiedReadRequests.With(labels).Inc()
	metrics.ActionCacheProxiedReadBytes.With(labels).Add(float64(proto.Size(resp)))
	return resp, err
}

func (s *ActionCacheServerProxy) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	if err := authutil.ValidateRestrictedACAccess(ctx, s.env, req.GetInstanceName()); err != nil {
		return nil, err
	}
	// Only if it's explicitly requested do we cache AC results locally.
	if proxy_util.SkipRemote(ctx) && (!authutil.EncryptionEnabled(ctx, s.authenticator) || s.supportsEncryption(ctx)) {
		resp, err := s.localACServer.UpdateActionResult(ctx, req)

		labels := prometheus.Labels{
			metrics.StatusLabel:           status.MetricsLabel(err),
			metrics.CacheHitMissStatus:    metrics.MissStatusLabel,
			metrics.CacheProxyRequestType: metrics.LocalOnlyCacheProxyRequestLabel,
		}
		metrics.ActionCacheProxiedWriteRequests.With(labels).Inc()
		metrics.ActionCacheProxiedWriteBytes.With(labels).Add(float64(proto.Size(req)))

		return resp, err
	}

	resp, err := s.remoteACClient.UpdateActionResult(ctx, req)
	labels := prometheus.Labels{
		metrics.StatusLabel:           status.MetricsLabel(err),
		metrics.CacheHitMissStatus:    metrics.MissStatusLabel,
		metrics.CacheProxyRequestType: proxy_util.RequestTypeLabelFromContext(ctx),
	}
	metrics.ActionCacheProxiedWriteRequests.With(labels).Inc()
	metrics.ActionCacheProxiedWriteBytes.With(labels).Add(float64(proto.Size(req)))
	if err != nil || resp == nil {
		return resp, err
	}

	// If we have a proxy AC TTL, then we can store this AC entry as valid for that TTL.
	// This ensures the REv2 guarantee that GetActionResult serves the most recent UpdateActionResult
	// for all requests to the same endpoint (this proxy).
	ttl := s.actionCacheTTL(ctx)
	if *cacheActionResults && ttl > 0 && s.shouldCacheUpdatedActionResult(ctx) {
		cacheCtx, prefixErr := prefix.AttachUserPrefixToContext(ctx, s.env.GetAuthenticator())
		if prefixErr == nil {
			getReq := &repb.GetActionResultRequest{
				InstanceName:   req.GetInstanceName(),
				ActionDigest:   req.GetActionDigest(),
				DigestFunction: req.GetDigestFunction(),
			}
			if localKey, keyErr := getACKeyForGetActionResultRequest(getReq); keyErr == nil {
				actionResult := resp
				if resp.GetExecutionMetadata().GetUsageStats().GetTimeline() != nil {
					// Default GetActionResult omits timeline data.
					actionResult = resp.CloneVT()
					actionResult.GetExecutionMetadata().GetUsageStats().Timeline = nil
				}
				s.cacheActionResultToLocalCAS(cacheCtx, localKey, getReq, actionResult)
			}
		}
	}
	return resp, err
}
