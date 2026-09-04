package firecracker

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// memorySnapshotExportMonitor observes writes while firecracker exports a memory snapshot.
// Since Firecracker writes the snapshot sequentially, the monitor can cache each completed
// chunk as soon as writes move on to a later chunk, while the snapshot data is still in the page cache.
// This removes the need for another pass through the COWStore to cache chunks after the snapshot is generated,
// reducing memory and IO from processing the large snapshots twice.
type memorySnapshotExportMonitor struct {
	cancel context.CancelFunc

	env                environment.Env
	cow                *copy_on_write.COWStore
	remoteInstanceName string
	cacheRemotely      bool
	cacheLocally       bool
	allZerosDigest     *repb.Digest

	// The write callback from COWStore adds write events to this channel.
	// The monitor can then queue finalized chunks to be cached.
	events chan copy_on_write.WriteEvent

	// chunkUploads is a channel of chunk offsets that are ready to be cached.
	chunkUploads chan int64
	eg           *errgroup.Group
	egCtx        context.Context

	tracker      sequentialChunkWriteTracker
	cacheChunkFn func(context.Context, int64) error

	compressedBytesWrittenRemotely int64
}

func newMemorySnapshotExportMonitor(ctx context.Context, env environment.Env, cow *copy_on_write.COWStore, remoteInstanceName string, cacheRemotely, cacheLocally bool, uploadConcurrency int, allZerosDigest *repb.Digest) *memorySnapshotExportMonitor {
	if uploadConcurrency <= 0 {
		uploadConcurrency = 1
	}

	ctx, cancel := context.WithCancel(ctx)
	eg, egCtx := errgroup.WithContext(ctx)
	m := &memorySnapshotExportMonitor{
		cancel:             cancel,
		env:                env,
		cow:                cow,
		remoteInstanceName: remoteInstanceName,
		cacheRemotely:      cacheRemotely,
		cacheLocally:       cacheLocally,
		allZerosDigest:     allZerosDigest,
		events:             make(chan copy_on_write.WriteEvent, 1024),
		chunkUploads:       make(chan int64, uploadConcurrency),
		eg:                 eg,
		egCtx:              egCtx,
	}
	// TODO(Maggie): Extract logic to cache a chunk from snaploader and set
	// m.cacheChunkFn.
	m.tracker = sequentialChunkWriteTracker{
		chunkSizeBytes: cow.ChunkSizeBytes(),
		finalizeChunk:  m.queueChunkUpload,
	}
	m.startUploadWorkers(uploadConcurrency)
	m.eg.Go(func() error {
		defer close(m.chunkUploads)
		return m.run()
	})
	return m
}

func (m *memorySnapshotExportMonitor) startUploadWorkers(uploadConcurrency int) {
	for i := 0; i < uploadConcurrency; i++ {
		m.eg.Go(func() error {
			for {
				select {
				case <-m.egCtx.Done():
					return nil
				case chunkOffset, ok := <-m.chunkUploads:
					if !ok {
						return nil
					}
					if err := m.cacheChunkFn(m.egCtx, chunkOffset); err != nil {
						return err
					}
				}
			}
		})
	}
}

// A write callback to be registered with the COWStore. The caller must not call
// OnWrite after Finish.
func (m *memorySnapshotExportMonitor) OnWrite(event copy_on_write.WriteEvent) {
	select {
	case <-m.egCtx.Done():
	case m.events <- event:
	}
}

func (m *memorySnapshotExportMonitor) run() error {
	for {
		select {
		case <-m.egCtx.Done():
			return m.egCtx.Err()
		case event, ok := <-m.events:
			if !ok {
				// If the chan is closed, all writes have completed and we can safely
				// finalize the tracker, which should only be called after all writes have
				// been sequentially observed.
				if err := m.tracker.Finish(); err != nil {
					log.CtxWarningf(m.egCtx, "Failed to finalize sequential chunk write tracker: %s", err)
					return err
				}
				return nil
			}
			if err := m.tracker.Observe(event); err != nil {
				return err
			}
		}
	}
}

// Finish signals that no more write events will arrive and waits for queued
// uploads to finish.
func (m *memorySnapshotExportMonitor) Finish() error {
	// Don't finalize the tracker here, because events are added to the tracker in the background,
	// and finalizing the tracker should only happen after all events have been observed
	close(m.events)
	err := m.eg.Wait()

	if m.compressedBytesWrittenRemotely > 0 {
		log.CtxDebugf(m.egCtx, "Cached %d compressed MB remotely during memory snapshot export", m.compressedBytesWrittenRemotely/(1024*1024))
	}

	return err
}

// queueChunkUpload schedules caching for a chunk that the sequential tracker has
// finalized.
func (m *memorySnapshotExportMonitor) queueChunkUpload(chunkOffset int64) error {
	select {
	case <-m.egCtx.Done():
	case m.chunkUploads <- chunkOffset:
	}
	return nil
}
