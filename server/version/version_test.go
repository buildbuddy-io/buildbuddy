package version

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelectLatestPublicRelease(t *testing.T) {
	for _, tc := range []struct {
		name, stamped, tag, want string
	}{
		{"stamped value wins", "v1.2.3", "ci-abc", "v1.2.3"},
		{"empty falls back to tag", "", "v1.2.4", "v1.2.4"},
		{"placeholder falls back to tag", "{STABLE_LATEST_PUBLIC_RELEASE}", "v1.2.4", "v1.2.4"},
		{"unknown tag passes through", "", "unknown", "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, selectLatestPublicRelease(tc.stamped, tc.tag))
		})
	}
}
