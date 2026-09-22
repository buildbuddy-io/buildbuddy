package install

import (
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunhelperutil"
	"github.com/stretchr/testify/require"
)

// TestWriteHelper runs the embedded helper, as install does.
func TestWriteHelper(t *testing.T) {
	path := helperPath
	t.Cleanup(func() { helperPath = path })
	helperPath = filepath.Join(t.TempDir(), "helper")

	require.Equal(t, "the device helper is not installed", tunHelperOutdated())
	require.NoError(t, writeTunHelper())
	v, err := installedTunHelperVersion()
	require.NoError(t, err)
	require.Equal(t, tunhelperutil.Version, v)
	require.Empty(t, tunHelperOutdated())
}
