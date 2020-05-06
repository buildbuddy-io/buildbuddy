package perms

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
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

func GroupAuthPermissions(g *tables.Group) *UserGroupPerm {
	return &UserGroupPerm{
		UserID:  g.GroupID,
		GroupID: g.GroupID,
		Perms:   GROUP_READ | GROUP_WRITE,
	}
}

func addPrefix(prefix, key string) string {
	r := prefix + "/" + key
	return r
}

func UserPrefixCacheKey(ctx context.Context, env environment.Env, key string) (string, error) {
	if userDB := env.GetUserDB(); userDB != nil {
		// Try group-based auth? (most common for grpc)
		g, err := userDB.GetBasicAuthGroup(ctx)
		if err != nil {
			return "", err
		}
		if g != nil {
			return addPrefix(g.GroupID, key), nil
		}

		// Attempt to lookup this user by auth token?
		tu, err := userDB.GetUser(ctx)
		if err != nil {
			return "", err
		}
		if tu != nil {
			return addPrefix(tu.UserID, key), nil
		}
	}
	return addPrefix("ANON", key), nil
}
