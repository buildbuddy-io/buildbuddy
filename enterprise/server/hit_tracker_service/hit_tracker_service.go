package hit_tracker_service

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	hitpb "github.com/buildbuddy-io/buildbuddy/proto/hit_tracker"
)

type HitTrackerService struct {
	hitTrackerFactory interfaces.HitTrackerFactory
}

func Register(env *real_environment.RealEnv) error {
	if env.GetHitTrackerFactory() == nil {
		return status.InvalidArgumentError("HitTrackerService requires a HitTrackerFactory")
	}
	env.SetHitTrackerServiceServer(&HitTrackerService{
		hitTrackerFactory: env.GetHitTrackerFactory(),
	})
	return nil
}

// TODO(iain): record a source (e.g. Cache Proxy).
func (h HitTrackerService) Track(ctx context.Context, req *hitpb.TrackRequest) (*hitpb.TrackResponse, error) {
	for _, cacheHit := range req.GetHits() {
		if cacheHit.GetInvocationId() == "" {
			log.Warning("Skipping TrackRequest.Hits with empty invocation ID")
			continue
		}

		hitTracker := h.hitTrackerFactory.NewCASHitTracker(ctx, cacheHit.GetInvocationId())
		for i := int64(0); i < cacheHit.GetEmptyHits(); i++ {
			hitTracker.TrackEmptyHit()
		}

		for _, download := range cacheHit.GetDownloads() {
			transferTimer := hitTracker.TrackDownload(download.GetResource().GetDigest())
			duration := download.GetDuration().AsDuration()
			err := transferTimer.Record(download.GetSizeBytes(), duration, download.GetResource().GetCompressor())
			if err != nil {
				log.CtxWarningf(ctx, "Error recording hit-tracking metrics: %s", err)
			}
		}
	}
	return &hitpb.TrackResponse{}, nil
}
