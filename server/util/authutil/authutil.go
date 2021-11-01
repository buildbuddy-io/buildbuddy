package authutil

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
)

// AuthorizeGroupRole checks whether the given user has any of the allowed roles
// within the given group.
func AuthorizeGroupRole(u interfaces.UserInfo, groupID string, allowedRoles role.Role) error {
	r := role.None
	for _, m := range u.GetGroupMemberships() {
		if m.GroupID == groupID {
			r = m.Role
			break
		}
	}
	if r == role.None {
		// User is not a member of the group at all; they were probably removed from
		// their org during their current UI session.
		return status.PermissionDeniedError("You do not have access to the requested organization")
	}
	if r&allowedRoles == 0 {
		return status.PermissionDeniedError("You do not have the appropriate role within this organization")
	}
	return nil
}

// EffectiveGroup returns the group membership effective for the request
// context, given an authenticated user.
func EffectiveGroup(ctx context.Context, u interfaces.UserInfo) (*interfaces.GroupMembership, error) {
	// u.GetGroupID() will be non-empty for API key auth (gRPC-based) if
	// authentication was successful (which it should be, since we have a valid
	// UserInfo here).
	//
	// For Web-based auth, the request context determines the preferred group ID.
	groupID := u.GetGroupID()

	if groupID == "" {
		reqCtx := requestcontext.ProtoRequestContextFromContext(ctx)
		if reqCtx == nil || reqCtx.GetGroupId() == "" {
			return nil, status.InvalidArgumentError("'request_context.group_id' field is missing.")
		}
		groupID = reqCtx.GetGroupId()
	}

	for _, membership := range u.GetGroupMemberships() {
		if membership.GroupID == groupID {
			return membership, nil
		}
	}

	return nil, status.PermissionDeniedErrorf("You are not a member of group %q", groupID)
}
