package hit_tracker_service

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"

	hitpb "github.com/buildbuddy-io/buildbuddy/proto/hit_tracker"
)

type HitTrackerService struct {
}

func Register(env *real_environment.RealEnv) {
	env.SetHitTrackerServiceServer(&HitTrackerService{})
}

func (h HitTrackerService) Track(ctx context.Context, req *hitpb.TrackRequest) (*hitpb.TrackResponse, error) {
	return &hitpb.TrackResponse{}, nil
}
