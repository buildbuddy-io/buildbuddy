package perms

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
