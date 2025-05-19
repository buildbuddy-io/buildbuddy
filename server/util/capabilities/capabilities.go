package capabilities

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
)

var (
	// DefaultAuthenticatedUserCapabilities are granted to users that are authenticated and
	// whose capabilities aren't explicitly provided (e.g. when creating a new API key
	// programmatically).
	DefaultAuthenticatedUserCapabilities = []cappb.Capability{cappb.Capability_CACHE_WRITE}
	// DefaultAuthenticatedUserCapabilitiesMask is the mask form of DefaultAuthenticatedUserCapabilities.
	DefaultAuthenticatedUserCapabilitiesMask = ToInt(DefaultAuthenticatedUserCapabilities)

	// AnonymousUserCapabilities are granted to users that aren't authenticated, as long as
	// anonymous usage is enabled in the server configuration.
	AnonymousUserCapabilities = DefaultAuthenticatedUserCapabilities
	// AnonymousUserCapabilitiesMask is the mask form of AnonymousUserCapabilities.
	AnonymousUserCapabilitiesMask = ToInt(AnonymousUserCapabilities)

	// UserAPIKeyCapabilitiesMask defines the capabilities that are allowed to
	// be assigned to user-owned API keys.
	UserAPIKeyCapabilitiesMask = ToInt([]cappb.Capability{
		cappb.Capability_CACHE_WRITE,
		cappb.Capability_CAS_WRITE,
	})
)

func FromInt(m int32) []cappb.Capability {
	caps := []cappb.Capability{}
	for _, c := range cappb.Capability_value {
		if m&c > 0 {
			caps = append(caps, cappb.Capability(c))
		}
	}
	return caps
}

func ToInt(caps []cappb.Capability) int32 {
	m := int32(0)
	for _, c := range caps {
		m |= int32(c)
	}
	return m
}

func ApplyMask(caps []cappb.Capability, mask int32) []cappb.Capability {
	return FromInt(ToInt(caps) & mask)
}

func IsGranted(ctx context.Context, env environment.Env, cap cappb.Capability) (bool, error) {
	authIsRequired := !env.GetAuthenticator().AnonymousUsageEnabled(ctx)
	user, err := authutil.AuthorizeGroupAccess(ctx, env)
	if err != nil {
		if authutil.IsAnonymousUserError(err) {
			if authIsRequired {
				return false, nil
			}
			return int32(cap)&AnonymousUserCapabilitiesMask > 0, nil
		}
		return false, err
	}
	return user.HasCapability(cap), nil
}

func ForAuthenticatedUser(ctx context.Context, authenticator interfaces.Authenticator) ([]cappb.Capability, error) {
	u, err := authenticator.AuthenticatedUser(ctx)
	if err != nil {
		if authutil.IsAnonymousUserError(err) && authenticator.AnonymousUsageEnabled(ctx) {
			return DefaultAuthenticatedUserCapabilities, nil
		}
		return nil, err
	}
	return u.GetCapabilities(), nil
}

// ForAuthenticatedUserGroup returns the authenticated user's capabilities
// within the given group ID.
func ForAuthenticatedUserGroup(ctx context.Context, authenticator interfaces.Authenticator, groupID string) ([]cappb.Capability, error) {
	u, err := authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	for _, gm := range u.GetGroupMemberships() {
		if gm.GroupID == groupID {
			return gm.Capabilities, nil
		}
	}
	return nil, status.PermissionDeniedError("you are not a member of the requested organization")
}
