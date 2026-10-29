package update

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckNotDowngrade(t *testing.T) {
	require.NoError(t, checkNotDowngrade("5.0.500", "5.0.499"))
	require.NoError(t, checkNotDowngrade("5.0.499", "5.0.499"))
	require.NoError(t, checkNotDowngrade("5.0.499", "unknown"))
	require.ErrorContains(t, checkNotDowngrade("5.0.498", "5.0.499"), "older than this bb")
	require.ErrorContains(t, checkNotDowngrade("../5.0.498", "5.0.499"), "invalid release version")
	require.ErrorContains(t, checkNotDowngrade("../5.0.498", "unknown"), "invalid release version")
}
