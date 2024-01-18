package badge

import (
	"context"

	bpb "github.com/buildbuddy-io/buildbuddy/proto/badge"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
)

func GetUserBadges(ctx context.Context, env environment.Env, req *bpb.GetUserBadgesRequest) (*bpb.GetUserBadgesResponse, error) {
	res := &bpb.GetUserBadgesResponse{
		Badges: []*bpb.Badge{
			{
				Description: "Earth Saver",
				ImageUrl:    "https://storage.googleapis.com/lulu-hackweek-badges/earth-saver.png",
			},
			{
				Description: "First Build",
				ImageUrl:    "https://storage.googleapis.com/lulu-hackweek-badges/first-build-badge-silver.png",
			},
		},
	}
	return res, nil
}
