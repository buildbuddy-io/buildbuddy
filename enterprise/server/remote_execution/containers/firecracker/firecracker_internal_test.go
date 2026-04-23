package firecracker

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestSnapshotDetails_DiffSnapshotDoesNotExportToCOW(t *testing.T) {
	flags.Set(t, "executor.enable_remote_snapshot_sharing", true)
	flags.Set(t, "executor.firecracker_export_snapshot_to_cow", true)

	c := &FirecrackerContainer{
		recycled:                true,
		recyclingEnabled:        true,
		supportsRemoteSnapshots: true,
		task: &repb.ExecutionTask{
			Command: &repb.Command{
				Platform: &repb.Platform{
					Properties: []*repb.Platform_Property{
						{
							Name:  platform.SnapshotSavePolicyPropertyName,
							Value: platform.AlwaysSaveSnapshot,
						},
					},
				},
			},
		},
	}

	details, err := c.snapshotDetails(context.Background())
	require.NoError(t, err)
	require.Equal(t, diffSnapshotType, details.snapshotType)
	require.Equal(t, diffMemSnapshotName, details.memSnapshotName)
	require.Equal(t, vmStateSnapshotName, details.vmStateSnapshotName)
	require.Nil(t, c.memorySnapshotExportVBD)
}
