package quota

import (
	"context"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"google.golang.org/grpc/metadata"
)

func getGroupID(ctx context.Context, env environment.Env) string {
	if a := env.GetAuthenticator(); a != nil {
		user, err := a.AuthenticatedUser(ctx)
		if err != nil {
			return interfaces.AuthAnonymousUser
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

// GetKey get the key for quota accounting from the context.
// Use group_id if is available from the context, otherwise, use the ip address.
func GetKey(ctx context.Context, env environment.Env) string {
	if groupID := getGroupID(ctx, env); groupID != "" {
		return groupID
	}
	return getIP(ctx)
}
