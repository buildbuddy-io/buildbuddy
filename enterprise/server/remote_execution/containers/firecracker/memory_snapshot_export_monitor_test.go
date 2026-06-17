package firecracker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestMemorySnapshotExportMonitor(t *testing.T) {
	started := make(chan int64, 2)
	m := newMonitor(t)
	m.cacheChunkFn = func(ctx context.Context, chunkOffset int64) error {
		started <- chunkOffset
		return nil
	}
	m.startUploadWorkers(2)

	// Queue chunks to be cached.
	m.chunkUploads <- 0
	m.chunkUploads <- 1

	require.Eventually(t, func() bool {
		return len(started) == 2
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, m.Finish())
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
