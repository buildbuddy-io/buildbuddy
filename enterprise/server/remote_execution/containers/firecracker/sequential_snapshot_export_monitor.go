package firecracker

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaputil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type sequentialChunkWriteTracker struct {
	chunkSizeBytes int64
	finalizeChunk  func(chunkOffset int64) error

	currentChunkOffset int64
	haveCurrentChunk   bool
	lastWriteEnd       int64
}

func (t *sequentialChunkWriteTracker) Observe(event copy_on_write.WriteEvent) error {
	endOffset, err := validateSequentialWriteEvent(event, t.chunkSizeBytes, t.lastWriteEnd)
	if err != nil {
		return err
	}
	if event.Length == 0 {
		return nil
	}
	t.lastWriteEnd = endOffset

	if !t.haveCurrentChunk {
		t.currentChunkOffset = event.ChunkOffset
		t.haveCurrentChunk = true
		return nil
	}
	if event.ChunkOffset < t.currentChunkOffset {
		return status.FailedPreconditionErrorf("memory snapshot writes moved backwards from chunk %d to chunk %d", t.currentChunkOffset, event.ChunkOffset)
	}
	if event.ChunkOffset == t.currentChunkOffset {
		return nil
	}
	if err := t.finalizeChunk(t.currentChunkOffset); err != nil {
		return err
	}
	t.currentChunkOffset = event.ChunkOffset
	return nil
}

func validateSequentialWriteEvent(event copy_on_write.WriteEvent, chunkSizeBytes, lastWriteEnd int64) (int64, error) {
	if event.Offset < 0 {
		return 0, status.InvalidArgumentErrorf("write offset must be non-negative: %d", event.Offset)
	}
	if event.Length < 0 {
		return 0, status.InvalidArgumentErrorf("write length must be non-negative: %d", event.Length)
	}
	if event.ChunkOffset < 0 {
		return 0, status.InvalidArgumentErrorf("chunk offset must be non-negative: %d", event.ChunkOffset)
	}
	if event.Length == 0 {
		return lastWriteEnd, nil
	}
	if chunkSizeBytes <= 0 {
		return 0, status.InvalidArgumentErrorf("chunk size must be positive: %d", chunkSizeBytes)
	}
	if event.ChunkOffset%chunkSizeBytes != 0 {
		return 0, status.FailedPreconditionErrorf("memory snapshot write event has invalid chunk offset: chunk_offset=%d chunk_size=%d", event.ChunkOffset, chunkSizeBytes)
	}

	endOffset := event.Offset + event.Length
	if endOffset < event.Offset {
		return 0, status.InvalidArgumentErrorf("write offset overflow: offset=%d length=%d", event.Offset, event.Length)
	}
	if event.Offset < lastWriteEnd {
		return 0, status.FailedPreconditionErrorf("memory snapshot writes are not monotonic: write offset %d is before previous write end %d", event.Offset, lastWriteEnd)
	}

	if event.Offset < event.ChunkOffset || endOffset > event.ChunkOffset+chunkSizeBytes {
		return 0, status.FailedPreconditionErrorf("memory snapshot write event is outside chunk bounds: offset=%d length=%d chunk_offset=%d chunk_size=%d", event.Offset, event.Length, event.ChunkOffset, chunkSizeBytes)
	}
	return endOffset, nil
}

func (t *sequentialChunkWriteTracker) Finish() error {
	if !t.haveCurrentChunk {
		return nil
	}
	chunkOffset := t.currentChunkOffset
	t.haveCurrentChunk = false
	return t.finalizeChunk(chunkOffset)
}

type memorySnapshotWriteMonitor struct {
	ctx    context.Context
	cancel context.CancelFunc

	env                environment.Env
	cow                *copy_on_write.COWStore
	remoteInstanceName string
	cacheRemotely      bool
	cacheLocally       bool
	allZerosDigest     *repb.Digest

	events     chan copy_on_write.WriteEvent
	done       chan struct{}
	monitorErr chan error

	sendMu  sync.Mutex
	stopped bool

	uploadEg  *errgroup.Group
	uploadCtx context.Context

	tracker sequentialChunkWriteTracker
}

func newMemorySnapshotWriteMonitor(ctx context.Context, env environment.Env, cow *copy_on_write.COWStore, remoteInstanceName string, cacheRemotely, cacheLocally bool, uploadConcurrency int) (*memorySnapshotWriteMonitor, error) {
	if uploadConcurrency <= 0 {
		uploadConcurrency = 1
	}
	emptyData := make([]byte, cow.ChunkSizeBytes())
	allZerosDigest, err := digest.Compute(bytes.NewReader(emptyData), repb.DigestFunction_BLAKE3)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	uploadEg, uploadCtx := errgroup.WithContext(ctx)
	uploadEg.SetLimit(uploadConcurrency)
	m := &memorySnapshotWriteMonitor{
		ctx:                ctx,
		cancel:             cancel,
		env:                env,
		cow:                cow,
		remoteInstanceName: remoteInstanceName,
		cacheRemotely:      cacheRemotely,
		cacheLocally:       cacheLocally,
		allZerosDigest:     allZerosDigest,
		events:             make(chan copy_on_write.WriteEvent, 1024),
		done:               make(chan struct{}),
		monitorErr:         make(chan error, 1),
		uploadEg:           uploadEg,
		uploadCtx:          uploadCtx,
	}
	m.tracker = sequentialChunkWriteTracker{
		chunkSizeBytes: cow.ChunkSizeBytes(),
		finalizeChunk:  m.queueChunkUpload,
	}
	go func() {
		m.monitorErr <- m.run()
	}()
	return m, nil
}

func (m *memorySnapshotWriteMonitor) OnWrite(event copy_on_write.WriteEvent) {
	m.sendMu.Lock()
	defer m.sendMu.Unlock()
	if m.stopped {
		return
	}
	select {
	case m.events <- event:
	case <-m.ctx.Done():
	}
}

func (m *memorySnapshotWriteMonitor) Finish() error {
	m.sendMu.Lock()
	if !m.stopped {
		m.stopped = true
		close(m.done)
	}
	m.sendMu.Unlock()

	monitorErr := <-m.monitorErr
	uploadErr := m.uploadEg.Wait()
	m.cancel()

	if monitorErr != nil {
		return monitorErr
	}
	return uploadErr
}

func (m *memorySnapshotWriteMonitor) run() error {
	err := m.runUntilDone()
	if err != nil {
		m.cancel()
	}
	return err
}

func (m *memorySnapshotWriteMonitor) runUntilDone() error {
	for {
		select {
		case event := <-m.events:
			if err := m.tracker.Observe(event); err != nil {
				return err
			}
		case <-m.done:
			return m.drainAndFinish()
		case <-m.ctx.Done():
			return m.ctx.Err()
		}
	}
}

func (m *memorySnapshotWriteMonitor) drainAndFinish() error {
	for {
		select {
		case event := <-m.events:
			if err := m.tracker.Observe(event); err != nil {
				return err
			}
		default:
			return m.tracker.Finish()
		}
	}
}

func (m *memorySnapshotWriteMonitor) queueChunkUpload(chunkOffset int64) error {
	if err := m.uploadCtx.Err(); err != nil {
		return nil
	}
	m.uploadEg.Go(func() error {
		if err := m.uploadCtx.Err(); err != nil {
			return nil
		}
		return m.cacheChunk(m.uploadCtx, chunkOffset)
	})
	return nil
}

func (m *memorySnapshotWriteMonitor) cacheChunk(ctx context.Context, chunkOffset int64) (returnErr error) {
	chunk := m.cow.Chunk(chunkOffset)
	if chunk == nil {
		return nil
	}
	defer func() {
		if err := chunk.Unmap(); err != nil && returnErr == nil {
			returnErr = status.WrapErrorf(err, "unmap memory snapshot chunk at offset %d", chunkOffset)
		}
	}()

	chunkDigest, err := chunk.Digest()
	if err != nil {
		return status.WrapErrorf(err, "compute memory snapshot chunk digest at offset %d", chunkOffset)
	}
	if chunkDigest.GetHash() == m.allZerosDigest.GetHash() {
		return nil
	}

	dirty := m.cow.Dirty(chunkOffset)
	if dirty {
		if err := chunk.Sync(); err != nil {
			return status.WrapErrorf(err, "sync memory snapshot chunk at offset %d", chunkOffset)
		}
	}

	path := filepath.Join(m.cow.DataDir(), copy_on_write.ChunkName(chunkOffset, dirty))
	_, err = snaputil.Cache(ctx, m.env.GetFileCache(), m.env.GetByteStreamClient(), m.cacheRemotely, m.cacheLocally && dirty, chunkDigest, m.remoteInstanceName, path, memoryChunkDirName)
	if err != nil {
		return status.WrapErrorf(err, "cache memory snapshot chunk at offset %d", chunkOffset)
	}
	return nil
}

func wrapMonitorErr(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("memory snapshot write monitor: %w", err)
}
