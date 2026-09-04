package firecracker

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestMemorySnapshotExportMonitor(t *testing.T) {
	cached := make(chan int64, 2)
	m := newMonitor(t)
	m.tracker.chunkSizeBytes = 1
	m.cacheChunkFn = func(ctx context.Context, chunkOffset int64) error {
		cached <- chunkOffset
		return nil
	}

	m.OnWrite(copy_on_write.WriteEvent{Offset: 0, Length: 1, ChunkIndex: 0})
	m.OnWrite(copy_on_write.WriteEvent{Offset: 1, Length: 1, ChunkIndex: 1})

	require.NoError(t, m.Finish())

	require.Eventually(t, func() bool {
		cachedElements := []int64{<-cached, <-cached}
		slices.Sort(cachedElements)
		return slices.Equal([]int64{0, 1}, cachedElements)
	}, 1*time.Second, 100*time.Millisecond)
}

func newMonitor(t *testing.T) *memorySnapshotExportMonitor {
	ctx := t.Context()

	allZerosDigest := &repb.Digest{
		Hash:      "0000000000000000000000000000000000000000000000000000000000000000",
		SizeBytes: 1024,
	}
	m := newMemorySnapshotExportMonitor(ctx, nil, &copy_on_write.COWStore{}, "", false, false, 2, allZerosDigest)
	return m
}
