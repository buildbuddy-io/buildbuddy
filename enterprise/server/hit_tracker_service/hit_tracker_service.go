package hit_tracker_service

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	hitpb "github.com/buildbuddy-io/buildbuddy/proto/hit_tracker"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	"google.golang.org/grpc/peer"
)

type HitTrackerService struct {
	env               environment.Env
	hitTrackerFactory interfaces.HitTrackerFactory
}

func Register(env *real_environment.RealEnv) error {
	if env.GetHitTrackerFactory() == nil {
		return status.InvalidArgumentError("HitTrackerService requires a HitTrackerFactory")
	}
	env.SetHitTrackerServiceServer(&HitTrackerService{
		env:               env,
		hitTrackerFactory: env.GetHitTrackerFactory(),
	})
	return nil
}

func (h HitTrackerService) Track(ctx context.Context, req *hitpb.TrackRequest) (*hitpb.TrackResponse, error) {
	// When usage tracking would reject the caller, Record fails before
	// recording anything, so skip it and log once instead of once per hit.
	skipRecord := false
	if h.env.GetUsageTracker() != nil {
		authenticator := h.env.GetAuthenticator()
		if _, err := authenticator.AuthenticatedUser(ctx); err != nil && !(authutil.IsAnonymousUserError(err) && authenticator.AnonymousUsageEnabled(ctx)) {
			skipRecord = true
			peerAddr := "unknown"
			if p, ok := peer.FromContext(ctx); ok && p != nil && p.Addr != nil {
				peerAddr = p.Addr.String()
			}
			log.CtxWarningf(ctx, "Not recording %d cache hits from server %q at %s: %s", len(req.GetHits()), req.GetServer(), peerAddr, err)
		}
	}
	for _, hit := range req.GetHits() {
		var hitTracker interfaces.HitTracker
		if hit.GetResource().GetCacheType() == rspb.CacheType_AC {
			hitTracker = h.hitTrackerFactory.NewRemoteACHitTracker(ctx, hit.GetRequestMetadata(), req.GetServer())
		} else if hit.GetResource().GetCacheType() == rspb.CacheType_CAS {
			hitTracker = h.hitTrackerFactory.NewRemoteCASHitTracker(ctx, hit.GetRequestMetadata(), req.GetServer())
		} else {
			alert.UnexpectedEvent("invalid_hit_tracker_event", "invalid cache type %s", hit.GetResource().GetCacheType())
			return nil, status.InvalidArgumentErrorf("invalid cache type %s", hit.GetResource().GetCacheType())
		}
		var transferTimer interfaces.TransferTimer
		if hit.GetCacheRequestType() == capb.RequestType_WRITE {
			transferTimer = hitTracker.TrackUpload(hit.GetResource().GetDigest())
		} else if hit.GetCacheRequestType() == capb.RequestType_READ {
			transferTimer = hitTracker.TrackDownload(hit.GetResource().GetDigest())
		} else {
			alert.UnexpectedEvent("invalid_hit_tracker_event", "invalid cache request type %s", hit.GetCacheRequestType())
			return nil, status.InvalidArgumentErrorf("invalid cache request type %s", hit.GetCacheRequestType())
		}
		if skipRecord {
			continue
		}
		duration := hit.GetDuration().AsDuration()
		err := transferTimer.Record(hit.GetSizeBytes(), duration, hit.GetResource().GetCompressor())
		if err != nil {
			log.CtxWarningf(ctx, "Error recording hit-tracking metrics: %s", err)
		}
	}
	return &hitpb.TrackResponse{}, nil
}
