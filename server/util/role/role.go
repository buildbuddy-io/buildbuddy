package role

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
)

// Constants for UserGroup.Role. These are powers of 2 so that we can allow
// assigning multiple roles to users and use these as bitmasks to check
// role membership.
const (
	// None means that the user is not granted any role-based permissions for
	// a particular group.
	None Role = 0
	// Developer means a user cannot perform certain privileged actions such
	// as creating API keys and viewing usage data, but can perform most other
	// common actions such as viewing invocation history.
	Developer Role = 1 << 0
	// Admin means a user has unrestricted access within a group.
	Admin Role = 1 << 1

	// DefaultRole is the role assigned to users when joining a group they did
	// not create.
	Default = Developer
)

// Role represents a user's role within a group.
type Role uint32

func ToProto(role Role) grpb.Group_Role {
	if role&Admin == Admin {
		return grpb.Group_ADMIN_ROLE
	}
	return grpb.Group_DEVELOPER_ROLE
}

func FromProto(role grpb.Group_Role) Role {
	if role == grpb.Group_ADMIN_ROLE {
		return Admin
	}
	return Developer
}

// ToCapabilities returns the capabilities granted to the given role.
func ToCapabilities(role Role) ([]akpb.ApiKey_Capability, error) {
	switch role {
	case Developer:
		return nil, nil
	case Admin:
		return []akpb.ApiKey_Capability{akpb.ApiKey_ORG_ADMIN_CAPABILITY}, nil
	default:
		return nil, status.InternalErrorf("unexpected role %d", role)
	}
}
