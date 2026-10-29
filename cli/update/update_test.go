package update

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckNotDowngrade(t *testing.T) {
	require.NoError(t, checkNotDowngrade("5.0.500", "5.0.499"))
	require.NoError(t, checkNotDowngrade("5.0.499", "5.0.499"))
	require.NoError(t, checkNotDowngrade("5.0.499", "unknown"))
	require.NoError(t, checkNotDowngrade("5.0.499", ""))
	require.ErrorContains(t, checkNotDowngrade("5.0.500", "5.0.501"), "older than 5.0.501")
	// Never move to an unsigned release, even from dev builds or when
	// nothing is installed.
	require.ErrorContains(t, checkNotDowngrade("5.0.498", "unknown"), "are unsigned")
	require.ErrorContains(t, checkNotDowngrade("5.0.498", ""), "are unsigned")
	require.ErrorContains(t, checkNotDowngrade("../5.0.498", "5.0.499"), "invalid release version")
}
