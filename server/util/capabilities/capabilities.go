package capabilities

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
)

var (
	// DefaultAuthenticatedUserCapabilities are granted to users that are authenticated and
	// whose capabilities aren't explicitly provided (e.g. when creating a new API key
	// programmatically).
	DefaultAuthenticatedUserCapabilities = []akpb.ApiKey_Capability{akpb.ApiKey_CACHE_WRITE_CAPABILITY}
	// DefaultAuthenticatedUserCapabilitiesMask is the mask form of DefaultAuthenticatedUserCapabilities.
	DefaultAuthenticatedUserCapabilitiesMask = ToInt(DefaultAuthenticatedUserCapabilities)

	// AnonymousUserCapabilities are granted to users that aren't authenticated, as long as
	// anonymous usage is enabled in the server configuration.
	AnonymousUserCapabilities = DefaultAuthenticatedUserCapabilities
	// AnonymousUserCapabilitiesMask is the mask form of AnonymousUserCapabilities.
	AnonymousUserCapabilitiesMask = ToInt(AnonymousUserCapabilities)
)

func FromInt(m int32) []akpb.ApiKey_Capability {
	caps := []akpb.ApiKey_Capability{}
	for _, c := range akpb.ApiKey_Capability_value {
		if m&c > 0 {
			caps = append(caps, akpb.ApiKey_Capability(c))
		}
	}
	return caps
}

func ToInt(caps []akpb.ApiKey_Capability) int32 {
	m := int32(0)
	for _, c := range caps {
		m |= int32(c)
	}
	return m
}

func IsGranted(ctx context.Context, env environment.Env, cap akpb.ApiKey_Capability) (bool, error) {
	a := env.GetAuthenticator()
	authIsRequired := !a.AnonymousUsageEnabled(ctx)
	user, err := a.AuthenticatedUser(ctx)
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

func ForAuthenticatedUser(ctx context.Context, env environment.Env) ([]akpb.ApiKey_Capability, error) {
	auth := env.GetAuthenticator()
	if auth == nil {
		return nil, status.UnimplementedError("Auth is not configured")
	}
	u, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		if authutil.IsAnonymousUserError(err) && auth.AnonymousUsageEnabled(ctx) {
			return DefaultAuthenticatedUserCapabilities, nil
		}
		return nil, err
	}
	return u.GetCapabilities(), nil
}
