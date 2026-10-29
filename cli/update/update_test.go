package update

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckNotDowngrade(t *testing.T) {
	require.NoError(t, checkNotDowngrade("5.0.500", "5.0.499", "this bb"))
	require.NoError(t, checkNotDowngrade("5.0.499", "5.0.499", "this bb"))
	require.NoError(t, checkNotDowngrade("5.0.499", "unknown", "this bb"))
	require.NoError(t, checkNotDowngrade("5.0.499", "", "this bb"))
	require.ErrorContains(t, checkNotDowngrade("5.0.500", "5.0.501", "this bb"), "refusing to update to 5.0.500: older than this bb 5.0.501")
	// Never move to an unsigned release, even from dev builds or when
	// nothing is installed.
	require.ErrorContains(t, checkNotDowngrade("5.0.498", "unknown", "this bb"), "are unsigned")
	require.ErrorContains(t, checkNotDowngrade("5.0.498", "", "this bb"), "are unsigned")
	require.ErrorContains(t, checkNotDowngrade("../5.0.498", "5.0.499", "this bb"), "invalid release version")
}
