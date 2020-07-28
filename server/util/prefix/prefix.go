package prefix

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
)

const (
	userPrefix = "user:"
)

func addPrefix(prefix, key string) string {
	r := prefix + "/" + key
	return r
}

func userPrefixCacheKey(ctx context.Context, env environment.Env, key string) (string, error) {
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
	prefix, err := userPrefixCacheKey(ctx, env, "")
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
