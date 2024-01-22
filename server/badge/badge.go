package badge

import (
	"context"

	bpb "github.com/buildbuddy-io/buildbuddy/proto/badge"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
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

func CreateBadges(ctx context.Context, env environment.Env, req *bpb.CreateBadgesRequest) (*bpb.CreateBadgesResponse, error) {
	auth := env.GetAuthenticator()
	if auth == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	_, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}

	err = env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		for _, badgeProto := range req.GetBadges() {
			badgeRow := badgeProtoToTable(badgeProto)
			if err := tx.NewQuery(ctx, "create_badge").Create(badgeRow); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &bpb.CreateBadgesResponse{}, nil
}

func GrantUserBadges(ctx context.Context, env environment.Env, req *bpb.GrantUserBadgesRequest) (*bpb.GrantUserBadgesResponse, error) {
	return nil, nil
}

func badgeProtoToTable(in *bpb.Badge) *tables.Badge {
	return &tables.Badge{
		BadgeID:     in.GetBadgeId(),
		ImageURL:    in.GetImageUrl(),
		Description: in.GetDescription(),
	}
}
