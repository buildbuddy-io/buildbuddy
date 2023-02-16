package authutil

import (
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

// AuthorizeGroupRole checks whether the given user has any of the allowed roles
// within the given group.
func AuthorizeGroupRole(u interfaces.UserInfo, groupID string, allowedRoles role.Role) error {
	if groupID == "" {
		return status.PermissionDeniedError("A group ID is required")
	}

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
