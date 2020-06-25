package perms

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
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

	userPrefix = "user:"
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

func addPrefix(prefix, key string) string {
	r := prefix + "/" + key
	return r
}

func UserPrefixCacheKey(ctx context.Context, env environment.Env, key string) (string, error) {
	if auth := env.GetAuthenticator(); auth != nil {
		if u, err := auth.AuthenticatedUser(ctx); err == nil {
			if u.GetGroupID() != "" {
				return addPrefix(u.GetGroupID(), key), nil
			} else if u.GetUserID() != "" {
				return addPrefix(u.GetUserID(), key), nil
			}
		}
	}
	return addPrefix("ANON", key), nil
}

func AttachUserPrefixToContext(ctx context.Context, env environment.Env) context.Context {
	prefix, err := UserPrefixCacheKey(ctx, env, "")
	prefixVal := ""
	if err == nil {
		prefixVal = prefix
	}
	return context.WithValue(ctx, userPrefix, prefixVal)
}

func UserPrefixFromContext(ctx context.Context) string {
	prefixVal := ""
	if v := ctx.Value(userPrefix); v != nil {
		prefixVal = v.(string)
	}
	return prefixVal
}
