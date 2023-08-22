package prefix

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	userPrefix = "user:"
)

func addPrefix(prefix, key string) string {
	r := prefix + "/" + key
	return r
}

func userPrefixCacheKey(ctx context.Context, env environment.Env, key string) (string, error) {
	auth := env.GetAuthenticator()
	// Note: authenticator can't be nil, even in the OSS version (we use
	// NullAuthenticator insteadof nil).
	u, err := auth.AuthenticatedUser(ctx)
	if authutil.IsAnonymousUserError(err) && auth.AnonymousUsageEnabled(ctx) {
		return addPrefix(interfaces.AuthAnonymousUser, key), nil
	}
	if err != nil {
		return "", err
	}
	if u.GetGroupID() == "" {
		return "", status.PermissionDeniedErrorf("Attempting to write to cache as a user with no group.")
	}
	return addPrefix(u.GetGroupID(), key), nil
}

func AttachUserPrefixToContext(ctx context.Context, env environment.Env) (context.Context, error) {
	prefix, err := userPrefixCacheKey(ctx, env, "")
	if err != nil {
		return nil, err
	}
	return context.WithValue(ctx, userPrefix, prefix), nil
}

func UserPrefixFromContext(ctx context.Context) (string, error) {
	if v := ctx.Value(userPrefix); v != nil {
		return v.(string), nil
	}
	log.Warning("No user prefix on context -- did you forget to call AttachUserPrefixToContext")
	return "", status.PermissionDeniedErrorf("Anonymous access disabled, permission denied.")
}
