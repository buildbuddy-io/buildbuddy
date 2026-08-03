package execution_graph_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/disk"
	"github.com/buildbuddy-io/buildbuddy/server/execution_graph"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	egapb "github.com/buildbuddy-io/buildbuddy/proto/execution_graph_analysis"
)

func TestStorageRoundTrip(t *testing.T) {
	flags.Set(t, "storage.disk.root_directory", t.TempDir())
	bs, err := disk.NewDiskBlobStore()
	require.NoError(t, err)
	ctx := context.Background()

	const iid = "8c3c4a4e-89cf-4f9e-9a29-a3b5997acaae"
	const attempt = uint64(1)

	ok, err := execution_graph.HasAnalysis(ctx, bs, iid, attempt)
	require.NoError(t, err)
	assert.False(t, ok)
	_, err = execution_graph.ReadAnalysis(ctx, bs, iid, attempt)
	require.Error(t, err)

	analysis := &egapb.ExecutionGraphAnalysis{
		Version:      execution_graph.AnalyzerVersion,
		InvocationId: iid,
		CriticalPath: &egapb.CriticalPath{NodeIndex: []int32{1, 5, 6}, DurationMillis: 18965},
	}
	require.NoError(t, execution_graph.WriteAnalysis(ctx, bs, attempt, analysis))

	ok, err = execution_graph.HasAnalysis(ctx, bs, iid, attempt)
	require.NoError(t, err)
	assert.True(t, ok)

	got, err := execution_graph.ReadAnalysis(ctx, bs, iid, attempt)
	require.NoError(t, err)
	assert.Equal(t, int64(18965), got.GetCriticalPath().GetDurationMillis())
	assert.Equal(t, iid, got.GetInvocationId())

	// A different attempt has no analysis.
	ok, err = execution_graph.HasAnalysis(ctx, bs, iid, attempt+1)
	require.NoError(t, err)
	assert.False(t, ok)
}
