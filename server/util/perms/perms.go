package perms

import (
	"context"
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	aclpb "github.com/buildbuddy-io/buildbuddy/proto/acl"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

const (
	NONE         = 0o0
	OWNER_READ   = 0o0400
	OWNER_WRITE  = 0o0200
	OWNER_EXEC   = 0o0100
	GROUP_READ   = 0o040
	GROUP_WRITE  = 0o020
	GROUP_EXEC   = 0o010
	OTHERS_READ  = 0o04
	OTHERS_WRITE = 0o02
	OTHERS_EXEC  = 0o01
	ALL          = 0o0777
)

type UserGroupPerm struct {
	UserID  string
	GroupID string
	Perms   int
}

func AnonymousUserPermissions() *UserGroupPerm {
	return &UserGroupPerm{
		UserID:  "",
		GroupID: "",
		Perms:   OTHERS_READ,
	}
}

func GroupAuthPermissions(groupID string) *UserGroupPerm {
	return &UserGroupPerm{
		UserID:  groupID,
		GroupID: groupID,
		Perms:   GROUP_READ | GROUP_WRITE,
	}
}

func ToACLProto(userID *uidpb.UserId, groupID string, perms int) *aclpb.ACL {
	return &aclpb.ACL{
		UserId:  userID,
		GroupId: groupID,
		OwnerPermissions: &aclpb.ACL_Permissions{
			Read:  perms&OWNER_READ != 0,
			Write: perms&OWNER_WRITE != 0,
		},
		GroupPermissions: &aclpb.ACL_Permissions{
			Read:  perms&GROUP_READ != 0,
			Write: perms&GROUP_WRITE != 0,
		},
		OthersPermissions: &aclpb.ACL_Permissions{
			Read:  perms&OTHERS_READ != 0,
			Write: perms&OTHERS_WRITE != 0,
		},
	}
}

func AddPermissionsCheckToQuery(ctx context.Context, env environment.Env, q *query_builder.Query) error {
	return AddPermissionsCheckToQueryWithTableAlias(ctx, env, q, "")
}

func AddPermissionsCheckToQueryWithTableAlias(ctx context.Context, env environment.Env, q *query_builder.Query, tableAlias string) error {
	tablePrefix := ""
	if tableAlias != "" {
		tablePrefix = tableAlias + "."
	}
	o := query_builder.OrClauses{}
	o.AddOr(fmt.Sprintf("(%sperms & ? != 0)", tablePrefix), OTHERS_READ)

	hasUser := false
	if auth := env.GetAuthenticator(); auth != nil {
		if u, err := auth.AuthenticatedUser(ctx); err == nil {
			hasUser = true
			if u.GetGroupID() != "" {
				groupArgs := []interface{}{
					GROUP_READ,
					u.GetGroupID(),
				}
				o.AddOr(fmt.Sprintf("(%sperms & ? != 0 AND %sgroup_id = ?)", tablePrefix, tablePrefix), groupArgs...)
			} else if u.GetUserID() != "" {
				groupArgs := []interface{}{
					GROUP_READ,
				}
				groupParams := make([]string, 0)
				for _, groupID := range u.GetAllowedGroups() {
					groupArgs = append(groupArgs, groupID)
					groupParams = append(groupParams, "?")
				}
				groupParamString := "(" + strings.Join(groupParams, ", ") + ")"
				groupQueryStr := fmt.Sprintf("(%sperms & ? != 0 AND %sgroup_id IN %s)", tablePrefix, tablePrefix, groupParamString)
				o.AddOr(groupQueryStr, groupArgs...)
				o.AddOr(fmt.Sprintf("(%sperms & ? != 0 AND %suser_id = ?)", tablePrefix, tablePrefix), OWNER_READ, u.GetUserID())
			}
			if u.IsAdmin() {
				o.AddOr(fmt.Sprintf("(%sperms & ? != 0)", tablePrefix), ALL)
			}
		}
	}

	if !hasUser && !env.GetConfigurator().GetAnonymousUsageEnabled() {
		return status.PermissionDeniedErrorf("Anonymous access disabled, permission denied.")
	}

	orQuery, orArgs := o.Build()
	q = q.AddWhereClause("("+orQuery+")", orArgs...)
	return nil
}
