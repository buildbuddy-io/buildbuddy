package badge

import (
	"context"

	bpb "github.com/buildbuddy-io/buildbuddy/proto/badge"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"gorm.io/gorm/clause"
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

	groupID := req.GetRequestContext().GetGroupId()
	if groupID == "" {
		return nil, status.InvalidArgumentError("group_id is required")
	}

	reqUserID := req.GetUserId()
	if len(reqUserID) == 0 {
		reqUserID = req.GetRequestContext().GetUserId().GetId()
	}

	users, err := env.GetUserDB().GetDisplayUsers(ctx, []string{reqUserID})
	if err != nil {
		return nil, err
	}
	du := users[reqUserID]

	res := &bpb.GetUserBadgesResponse{
		DisplayUser: du,
	}

	rq := env.GetDBHandle().NewQuery(ctx, "badge_get_user_badges").Raw(`
       SELECT b.badge_id, b.image_url, b.description FROM "Badges" as b
	   JOIN "UserBadges" as ub ON b.badge_id = ub.badge_id
	   WHERE ub.group_id = ?
	   AND ub.user_id = ?
	   ORDER by b.badge_id
    `, groupID, reqUserID)
	badges, err := db.ScanAll(rq, &tables.Badge{})
	if err != nil {
		return res, err
	}

	for _, badge := range badges {
		res.Badges = append(res.Badges, badgeTableToProto(badge))
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
	auth := env.GetAuthenticator()
	if auth == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	_, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetBadgeId() == "" {
		return nil, status.InvalidArgumentError("badge_id is required")
	}

	groupID := req.GetRequestContext().GetGroupId()
	if groupID == "" {
		return nil, status.InvalidArgumentError("group_id is required")
	}

	userBadges := make([]*tables.UserBadge, 0, len(req.GetAddUserIds()))
	for _, userID := range req.GetAddUserIds() {
		userBadges = append(userBadges, &tables.UserBadge{
			GroupID: groupID,
			UserID:  userID,
			BadgeID: req.GetBadgeId(),
		})
	}

	err = env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		var existing tables.Badge
		if err := tx.GORM(ctx, "badge_check_user_id").Where("badge_id = ?", req.GetBadgeId()).First(&existing).Error; err != nil {
			if db.IsRecordNotFound(err) {
				return status.InvalidArgumentErrorf("badge_id %q doesn't exist", req.GetBadgeId())
			}
			return err
		}

		if len(userBadges) > 0 {
			if err := tx.GORM(ctx, "badge_insert_user_badge").Clauses(clause.OnConflict{DoNothing: true}).Create(userBadges).Error; err != nil {
				return err
			}
		}

		if len(req.GetRemoveUserIds()) > 0 {
			if txError := tx.NewQuery(ctx, "badge_delete_user_badges").Raw(
				`DELETE FROM "UserBadges" WHERE badge_id=? AND group_id=? AND user_id IN ?`, req.GetRemoveUserIds()).Exec().Error; txError != nil {
				return txError
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func badgeProtoToTable(in *bpb.Badge) *tables.Badge {
	return &tables.Badge{
		BadgeID:     in.GetBadgeId(),
		ImageURL:    in.GetImageUrl(),
		Description: in.GetDescription(),
	}
}

func badgeTableToProto(in *tables.Badge) *bpb.Badge {
	return &bpb.Badge{
		BadgeId:     in.BadgeID,
		ImageUrl:    in.ImageURL,
		Description: in.Description,
	}
}
