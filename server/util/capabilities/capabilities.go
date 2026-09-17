package capabilities

import (
	"context"
	"slices"
	"strings"

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
		cappb.Capability_IMAGE_CACHE_WRITE,
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

func IsGranted(ctx context.Context, authenticator interfaces.Authenticator, cap cappb.Capability) (bool, error) {
	authIsRequired := !authenticator.AnonymousUsageEnabled(ctx)
	user, err := authenticator.AuthenticatedUser(ctx)
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

// ActionCacheWriteCapabilities returns the capabilities that independently grant
// writes to the given action cache instance. Restricted AC client identity checks
// must still be performed separately.
func ActionCacheWriteCapabilities(instanceName string) cappb.Capability {
	caps := cappb.Capability_CACHE_WRITE
	if !strings.HasPrefix(instanceName, interfaces.OCIImageInstanceNamePrefix) {
		return caps
	}
	// Path-based caches clean instance names when constructing storage keys.
	// Do not let an image-only writer escape the reserved namespace using "..".
	if slices.Contains(strings.Split(instanceName, "/"), "..") {
		return caps
	}
	return caps | cappb.Capability_IMAGE_CACHE_WRITE
}

// CanWriteActionCache checks whether the caller has write permission for the
// given action cache instance. It does not replace restricted client identity checks.
func CanWriteActionCache(ctx context.Context, authenticator interfaces.Authenticator, instanceName string) (bool, error) {
	return IsGranted(ctx, authenticator, ActionCacheWriteCapabilities(instanceName))
}

// CanWriteCAS checks whether the caller has write permission for the given CAS instance.
func CanWriteCAS(ctx context.Context, authenticator interfaces.Authenticator, instanceName string) (bool, error) {
	return IsGranted(ctx, authenticator, ActionCacheWriteCapabilities(instanceName)|cappb.Capability_CAS_WRITE)
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
