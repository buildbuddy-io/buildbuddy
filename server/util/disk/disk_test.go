package disk_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
)

func TestGetDirUsage(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	testfs.MakeTempFile(t, dir, t.Name()+"-*")

	usage, err := disk.GetDirUsage(dir)
	require.NoError(t, err)
	require.Greater(t, usage.TotalBytes, uint64(0))
	require.Equal(t, usage.TotalBytes, usage.UsedBytes+usage.FreeBytes)
	require.GreaterOrEqual(t, usage.FreeBytes, usage.AvailBytes)
}
