package api_key

import (
	"github.com/buildbuddy-io/buildbuddy/server/tables"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
)

const (
	// Default visibility for group-level keys: visible to group admins only.
	DefaultAPIKeyVisibility = int32(akpb.Visibility_VISIBLE_TO_GROUP_ADMINS)

	// User-owned keys are visible to their owner via the key's ACL, so the
	// visible-to-developers bit isn't set on them.
	UserAPIKeyVisibility = int32(akpb.Visibility_VISIBLE_TO_GROUP_ADMINS)

	// Visibility for group-level keys that are also shared with non-admin
	// members of the group.
	DeveloperVisibleAPIKeyVisibility = int32(akpb.Visibility_VISIBLE_TO_DEVELOPERS | akpb.Visibility_VISIBLE_TO_GROUP_ADMINS)

	// Impersonation API keys aren't visible to group admins or developers.
	ImpersonationAPIKeyVisibility = int32(akpb.Visibility_UNKNOWN_VISIBILITY)
)

func VisibilitiesToInt(visibilities []akpb.Visibility) int32 {
	mask := int32(0)
	for _, visibility := range visibilities {
		mask |= int32(visibility)
	}
	return mask
}

func VisibilitiesFromInt(mask int32) []akpb.Visibility {
	var visibilities []akpb.Visibility
	for bit := int32(1); bit > 0 && bit <= mask; bit <<= 1 {
		if mask&bit != 0 {
			visibilities = append(visibilities, akpb.Visibility(bit))
		}
	}
	return visibilities
}

func IsVisibleToDevelopers(mask int32) bool {
	return mask&int32(akpb.Visibility_VISIBLE_TO_DEVELOPERS) != 0
}

func VisibilityFromLegacyFlag(visibleToDevelopers bool) int32 {
	mask := int32(akpb.Visibility_VISIBLE_TO_GROUP_ADMINS)
	if visibleToDevelopers {
		mask |= int32(akpb.Visibility_VISIBLE_TO_DEVELOPERS)
	}
	return mask
}

type visibilityRequest interface {
	*akpb.CreateApiKeyRequest | *akpb.UpdateApiKeyRequest
	GetVisibility() []akpb.Visibility
	GetVisibleToDevelopers() bool
}

// FixRequestVisibility rewrites a create or update request so that both the
// visibility list and the deprecated visible_to_developers flag are populated
// and agree. The list wins if the client set it; otherwise it is derived from
// the flag. Call this before the request reaches the DB so that every key
// written by this server version has a correct visibility mask.
// TODO(iain): remove once all clients send visibility.
func FixRequestVisibility[T visibilityRequest](req T) {
	mask := VisibilitiesToInt(req.GetVisibility())
	if mask == 0 {
		mask = VisibilityFromLegacyFlag(req.GetVisibleToDevelopers())
	}
	visibilities := VisibilitiesFromInt(mask)
	visibleToDevelopers := IsVisibleToDevelopers(mask)
	switch r := any(req).(type) {
	case *akpb.CreateApiKeyRequest:
		r.Visibility, r.VisibleToDevelopers = visibilities, visibleToDevelopers
	case *akpb.UpdateApiKeyRequest:
		r.Visibility, r.VisibleToDevelopers = visibilities, visibleToDevelopers
	}
}

// VisibilityOfKey returns the effective bitmask for a stored key. Keys that
// predate the bitmask have a zero Visibility and fall back to the deprecated
// flag.
// TODO(iain): remove the fallback once existing keys have been backfilled.
func VisibilityOfKey(k *tables.APIKey) int32 {
	if k.Visibility != 0 {
		return k.Visibility
	}
	return VisibilityFromLegacyFlag(k.VisibleToDevelopers)
}
