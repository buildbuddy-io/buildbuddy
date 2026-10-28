package version

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelectPublicCommit(t *testing.T) {
	for _, tc := range []struct {
		name, stamped, commit, want string
	}{
		{"stamped value wins", "abc1234", "def5678", "abc1234"},
		{"empty falls back to commit", "", "def5678", "def5678"},
		{"placeholder falls back to commit", "{STABLE_BUILDBUDDY_COMMIT_SHA}", "def5678", "def5678"},
		{"unknown commit passes through", "", "unknown", "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, selectPublicCommit(tc.stamped, tc.commit))
		})
	}
}

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
