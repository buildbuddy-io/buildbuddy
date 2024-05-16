package metricsutil

import (
	"slices"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
)

var (
	observedGroupIDs = flag.Slice("metrics.observed_group_ids", []string{}, "Group IDs allowed in group_id metrics labels. Groups not in this list will be labeled as 'ANON' if unauthenticated or 'other' if authenticated.")
)

// FilteredGroupIDLabel returns a value suitable for use with the GroupID label.
// It restricts the observed group IDs to the allowed values configured by flag.
func FilteredGroupIDLabel(groupID string) string {
	if groupID == "" || groupID == "ANON" {
		return "ANON"
	}
	if slices.Contains(*observedGroupIDs, groupID) {
		return groupID
	}
	return "other"
}
