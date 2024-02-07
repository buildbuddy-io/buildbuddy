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
	//
	// Developers have CAS write permissions and readonly AC permissions.
	Developer Role = 1 << 0

	// Admin means a user has unrestricted access within a group.
	Admin Role = 1 << 1

	// Writer role grants the same capabilities as Developer role, except it
	// allows CAS read+write and AC read+write.
	Writer Role = 1 << 2

	// Reader role grants the same capabilities as Developer role, except it
	// allows only CAS reads and AC reads (no cache writes are allowed).
	Reader Role = 1 << 3

	// DefaultRole is the role assigned to users when joining a group they did
	// not create.
	Default = Developer
)

// Role represents a user's role within a group.
type Role uint32

func ToProto(role Role) (grpb.Group_Role, error) {
	if role&Admin == Admin {
		return grpb.Group_ADMIN_ROLE, nil
	}
	if role&Developer == Developer {
		return grpb.Group_DEVELOPER_ROLE, nil
	}
	if role&Writer == Writer {
		return grpb.Group_WRITER_ROLE, nil
	}
	if role&Reader == Reader {
		return grpb.Group_READER_ROLE, nil
	}
	return 0, status.InvalidArgumentError("invalid Role value")
}

func FromProto(role grpb.Group_Role) (Role, error) {
	switch role {
	case grpb.Group_ADMIN_ROLE:
		return Admin, nil
	case grpb.Group_DEVELOPER_ROLE:
		return Developer, nil
	case grpb.Group_WRITER_ROLE:
		return Writer, nil
	case grpb.Group_READER_ROLE:
		return Reader, nil
	default:
		return 0, status.InvalidArgumentError("invalid Role value")
	}
}

// ToCapabilities returns the maximum set of allowed capabilities that can be
// granted to a user with the given role.
func ToCapabilities(role Role) ([]akpb.ApiKey_Capability, error) {
	switch role {
	case Developer:
		return []akpb.ApiKey_Capability{
			akpb.ApiKey_CAS_WRITE_CAPABILITY,
		}, nil
	case Admin:
		return []akpb.ApiKey_Capability{
			akpb.ApiKey_CAS_WRITE_CAPABILITY,
			akpb.ApiKey_CACHE_WRITE_CAPABILITY,
			akpb.ApiKey_ORG_ADMIN_CAPABILITY,
		}, nil
	case Writer:
		return []akpb.ApiKey_Capability{
			akpb.ApiKey_CAS_WRITE_CAPABILITY,
			akpb.ApiKey_CACHE_WRITE_CAPABILITY,
		}, nil
	case Reader:
		return nil, nil
	default:
		return nil, status.InternalErrorf("unexpected role %d", role)
	}
}
