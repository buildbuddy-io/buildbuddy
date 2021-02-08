package capabilities

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
)

var (
	// AnonymousUserCapabilities are granted to users that aren't authenticated, as long as
	// anonymous usage is enabled in the server configuration.
	AnonymousUserCapabilities = []akpb.ApiKey_Capability{akpb.ApiKey_CACHE_WRITE_CAPABILITY}
	// AnonymousUserCapabilitiesMask is the mask form of AnonymousUserCapabilities.
	AnonymousUserCapabilitiesMask = ToInt(DefaultCapabilities)
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
	authIsRequired := !env.GetConfigurator().GetAnonymousUsageEnabled()
	a := env.GetAuthenticator()
	if a == nil && authIsRequired {
		return false, status.UnimplementedError("Not Implemented")
	}
	user, err := a.AuthenticatedUser(ctx)
	if isAnonymousUser := err != nil; isAnonymousUser {
		if authIsRequired {
			return false, err
		}
		return int32(cap)&AnonymousUserCapabilitiesMask > 0, nil
	}
	return user.HasCapability(cap), nil
}
