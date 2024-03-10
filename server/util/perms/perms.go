package perms

import (
	"context"
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/blocklist"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
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
	Perms   int32
}

func AnonymousUserPermissions() *UserGroupPerm {
	return &UserGroupPerm{
		UserID:  "",
		GroupID: "",
		Perms:   OTHERS_READ,
	}
}

func DefaultPermissions(u interfaces.UserInfo) *UserGroupPerm {
	return &UserGroupPerm{
		UserID:  u.GetUserID(),
		GroupID: u.GetGroupID(),
		Perms:   GROUP_READ | GROUP_WRITE,
	}
}

func ToACLProto(userID *uidpb.UserId, groupID string, perms int32) *aclpb.ACL {
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

func FromACL(acl *aclpb.ACL) (int32, error) {
	if acl == nil {
		return 0, status.InvalidArgumentError("ACL is nil.")
	}
	if acl.GetOwnerPermissions() == nil || acl.GetGroupPermissions() == nil || acl.GetOthersPermissions() == nil {
		return 0, status.InvalidArgumentError("ACL is missing one or more required permissions fields.")
	}
	p := int32(0)
	if acl.GetOwnerPermissions().GetRead() {
		p |= OWNER_READ
	}
	if acl.GetOwnerPermissions().GetWrite() {
		p |= OWNER_WRITE
	}
	if acl.GetGroupPermissions().GetRead() {
		p |= GROUP_READ
	}
	if acl.GetGroupPermissions().GetWrite() {
		p |= GROUP_WRITE
	}
	if acl.GetOthersPermissions().GetRead() {
		p |= OTHERS_READ
	}
	if acl.GetOthersPermissions().GetWrite() {
		p |= OTHERS_WRITE
	}
	return p, nil
}

func AuthorizeRead(u interfaces.UserInfo, acl *aclpb.ACL) error {
	if u == nil {
		return status.InvalidArgumentError("user cannot be nil.")
	}
	if acl == nil {
		return status.InvalidArgumentError("acl cannot be nil.")
	}

	perms, err := FromACL(acl)
	if err != nil {
		return err
	}

	if perms&OTHERS_READ != 0 {
		return nil
	}
	isOwner := u.GetUserID() == acl.GetUserId().GetId()
	if isOwner && perms&OWNER_READ != 0 {
		return nil
	}
	if perms&GROUP_READ != 0 {
		for _, groupID := range u.GetAllowedGroups() {
			if groupID == acl.GetGroupId() {
				return nil
			}
		}
	}

	return status.PermissionDeniedError("You do not have permission to perform this action.")
}

func AuthorizeWrite(authenticatedUser *interfaces.UserInfo, acl *aclpb.ACL) error {
	if authenticatedUser == nil {
		return status.InvalidArgumentError("authenticatedUser cannot be nil.")
	}
	u := *authenticatedUser
	if acl == nil {
		return status.InvalidArgumentError("acl cannot be nil.")
	}

	perms, err := FromACL(acl)
	if err != nil {
		return err
	}

	if perms&OTHERS_WRITE != 0 {
		log.Warning("Ignoring request to allow OTHERS_WRITE. This should not happen!")
	}
	isOwner := u.GetUserID() == acl.GetUserId().GetId()
	if isOwner && perms&OWNER_WRITE != 0 {
		return nil
	}
	if perms&GROUP_WRITE != 0 {
		for _, groupID := range u.GetAllowedGroups() {
			if groupID == acl.GetGroupId() {
				return nil
			}
		}
	}

	return status.PermissionDeniedError("You do not have permission to perform this action.")
}

func AddPermissionsCheckToQuery(ctx context.Context, env environment.Env, q *query_builder.Query) error {
	return AddPermissionsCheckToQueryWithTableAlias(ctx, env, q, "")
}

func AddPermissionsCheckToQueryWithTableAlias(ctx context.Context, env environment.Env, q *query_builder.Query, tableAlias string) error {
	o, err := GetPermissionsCheckClauses(ctx, env, q, tableAlias)
	if err != nil {
		return err
	}
	orQuery, orArgs := o.Build()
	q.AddWhereClause("("+orQuery+")", orArgs...)
	return nil
}

func GetPermissionsCheckClauses(ctx context.Context, env environment.Env, q *query_builder.Query, tableAlias string) (*query_builder.OrClauses, error) {
	tablePrefix := ""
	if tableAlias != "" {
		tablePrefix = tableAlias + "."
	}
	o := &query_builder.OrClauses{}
	o.AddOr(fmt.Sprintf("(%sperms & ? != 0)", tablePrefix), OTHERS_READ)

	hasUser := false
	auth := env.GetAuthenticator()
	if u, err := auth.AuthenticatedUser(ctx); err == nil {
		hasUser = true
		if u.GetUserID() != "" {
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
		} else if u.GetGroupID() != "" {
			groupArgs := []interface{}{
				GROUP_READ,
				u.GetGroupID(),
			}
			o.AddOr(fmt.Sprintf("(%sperms & ? != 0 AND %sgroup_id = ?)", tablePrefix, tablePrefix), groupArgs...)
		}
	}

	if !hasUser && !env.GetAuthenticator().AnonymousUsageEnabled(ctx) {
		return nil, status.PermissionDeniedErrorf("Anonymous access disabled, permission denied.")
	}

	return o, nil
}

func AuthorizeGroupAccess(ctx context.Context, env environment.Env, groupID string) error {
	if groupID == "" {
		return status.InvalidArgumentError("group ID is required")
	}
	user, err := env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return err
	}
	for _, allowedGroupID := range user.GetAllowedGroups() {
		if allowedGroupID == groupID {
			return nil
		}
	}
	return status.PermissionDeniedError("You do not have access to the requested group")
}

func AuthorizeGroupAccessForStats(ctx context.Context, env environment.Env, groupID string) error {
	if err := AuthorizeGroupAccess(ctx, env, groupID); err != nil {
		return err
	}
	if blocklist.IsBlockedForStatsQuery(groupID) {
		return status.ResourceExhaustedError("Too many rows.")
	}
	return nil
}

// ForAuthenticatedGroup returns GROUP_READ|GROUP_WRITE permissions for authenticated groups,
// or OTHERS_READ for anonymous users.
func ForAuthenticatedGroup(ctx context.Context, env environment.Env) (*UserGroupPerm, error) {
	auth := env.GetAuthenticator()
	u, err := auth.AuthenticatedUser(ctx)
	if err != nil || u.GetGroupID() == "" {
		if authutil.IsAnonymousUserError(err) && auth.AnonymousUsageEnabled(ctx) {
			return AnonymousUserPermissions(), nil
		}
		return nil, status.PermissionDeniedErrorf("Anonymous access disabled, permission denied.")
	}

	return DefaultPermissions(u), nil
}
