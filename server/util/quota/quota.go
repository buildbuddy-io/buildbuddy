package quota

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func getGroupID(ctx context.Context, env environment.Env) string {
	if a := env.GetAuthenticator(); a != nil {
		user, err := a.AuthenticatedUser(ctx)
		if err != nil {
			return ""
		}
		return user.GetGroupID()
	}
	return ""
}

// GetKey gets the key for quota accounting from the context.
// If available, group_id is used, otherwise the key falls back to the ip address.
func GetKey(ctx context.Context, env environment.Env) (string, error) {
	if groupID := getGroupID(ctx, env); groupID != "" {
		return groupID, nil
	}
	if ip := clientip.Get(ctx); ip != "" {
		return ip, nil
	}
	return "", status.InternalErrorf("quota key is empty")

}
