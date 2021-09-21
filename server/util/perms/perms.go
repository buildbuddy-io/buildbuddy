package perms

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	aclpb "github.com/buildbuddy-io/buildbuddy/proto/acl"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
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

// Constants for UserGroup.Role. These are powers of 2 so that we can allow
// assigning multiple roles to users and use these as bitmasks to check
// role membership.
const (
	// DeveloperRole means a user cannot perform certain privileged actions such
	// as creating API keys and viewing usage data, but can perform most other
	// common actions such as viewing invocation history.
	DeveloperRole Role = 1 << 0
	// AdminRole means a user has unrestricted access within a group.
	AdminRole Role = 1 << 1

	// DefaultRole is the role assigned to users when joining a group they did
	// not create.
	// TODO(bduffany): Change this to DeveloperRole once we have a way to manage
	// roles via the UI (otherwise, there would be no easy way to promote new
	// users to admins in the meantime).
	DefaultRole = AdminRole
)

// Role represents a value stored in the UserGroup.Role field.
type Role uint32

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

func FromACL(acl *aclpb.ACL) (int, error) {
	if acl == nil {
		return 0, status.InvalidArgumentError("ACL is nil.")
	}
	if acl.GetOwnerPermissions() == nil || acl.GetGroupPermissions() == nil || acl.GetOthersPermissions() == nil {
		return 0, status.InvalidArgumentError("ACL is missing one or more required permissions fields.")
	}
	p := 0
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

func RoleToProto(role Role) grpb.Group_Role {
	if role&AdminRole == AdminRole {
		return grpb.Group_ADMIN_ROLE
	}
	return grpb.Group_DEVELOPER_ROLE
}

func RoleFromProto(role grpb.Group_Role) Role {
	if role == grpb.Group_ADMIN_ROLE {
		return AdminRole
	}
	return DeveloperRole
}
