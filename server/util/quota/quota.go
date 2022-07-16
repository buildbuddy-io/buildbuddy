package quota

import (
	"context"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"
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

func getIP(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	vals := md.Get("X-Forwarded-For")

	if len(vals) == 0 {
		return ""
	}
	ips := strings.Split(vals[0], ",")
	return ips[0]
}

// GetKey gets the key for quota accounting from the context.
// If available, group_id is used, otherwise the key falls back to the ip address.
func GetKey(ctx context.Context, env environment.Env) (string, error) {
	if groupID := getGroupID(ctx, env); groupID != "" {
		return groupID, nil
	}
	if ip := getIP(ctx); ip != "" {
		return ip, nil
	}
	return "", status.InternalErrorf("quota key is empty")

}
