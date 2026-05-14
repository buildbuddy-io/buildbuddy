package prefix

import (
	"context"

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

func userPrefixCacheKey(ctx context.Context, authenticator interfaces.Authenticator, key string) (string, error) {
	// Note: authenticator can't be nil, even in the OSS version (we use
	// NullAuthenticator insteadof nil).
	u, err := authenticator.AuthenticatedUser(ctx)
	if authutil.IsAnonymousUserError(err) && authenticator.AnonymousUsageEnabled(ctx) {
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

func UserPrefix(ctx context.Context, authenticator interfaces.Authenticator) (string, error) {
	prefix, err := userPrefixCacheKey(ctx, authenticator, "")
	if err != nil {
		return "", err
	}
	return prefix, nil
}

func AttachUserPrefixToContext(ctx context.Context, authenticator interfaces.Authenticator) (context.Context, error) {
	prefix, err := userPrefixCacheKey(ctx, authenticator, "")
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
