package badge

import (
	"context"

	bpb "github.com/buildbuddy-io/buildbuddy/proto/badge"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func GetUserBadges(ctx context.Context, env environment.Env, req *bpb.GetUserBadgesRequest) (*bpb.GetUserBadgesResponse, error) {
	auth := env.GetAuthenticator()
	if auth == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	_, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}

	reqUserId := req.GetUserId()
	if len(reqUserId) == 0 {
		reqUserId = req.GetRequestContext().GetUserId().GetId()
	}

	users, err := env.GetUserDB().GetDisplayUsers(ctx, []string{reqUserId})
	if err != nil {
		return nil, err
	}
	du := users[reqUserId]
	res := &bpb.GetUserBadgesResponse{
		DisplayUser: du,
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
