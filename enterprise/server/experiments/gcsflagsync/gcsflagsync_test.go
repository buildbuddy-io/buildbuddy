package gcsflagsync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	flagdsync "github.com/open-feature/flagd/core/pkg/sync"
)

// fakeBlobstore is a minimal interfaces.Blobstore whose ReadBlob returns
// caller-controlled data/error. Only ReadBlob is exercised by gcsflagsync.
type fakeBlobstore struct {
	mu   sync.Mutex
	data []byte
	err  error
}

func (f *fakeBlobstore) setData(b []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = b
}

func (f *fakeBlobstore) ReadBlob(ctx context.Context, blobName string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.data, f.err
}

func (f *fakeBlobstore) BlobExists(ctx context.Context, blobName string) (bool, error) {
	panic("not implemented")
}
func (f *fakeBlobstore) WriteBlob(ctx context.Context, blobName string, data []byte) (int, error) {
	panic("not implemented")
}
func (f *fakeBlobstore) DeleteBlob(ctx context.Context, blobName string) error {
	panic("not implemented")
}
func (f *fakeBlobstore) Writer(ctx context.Context, blobName string) (interfaces.CommittedWriteCloser, error) {
	panic("not implemented")
}

func newSyncer(bs interfaces.Blobstore) *syncer {
	return &syncer{blobstore: bs, object: "flags.json", interval: time.Minute}
}

// fetch pushes on the first read and skips unchanged objects, but pushes again
// once the object changes.
func TestFetch_PushesOnChangeAndDedups(t *testing.T) {
	ctx := context.Background()
	bs := &fakeBlobstore{data: []byte(`{"flags":{}}`)}
	s := newSyncer(bs)
	ch := make(chan flagdsync.DataSync, 8)

	require.NoError(t, s.fetch(ctx, ch, false /*=force*/))
	require.Len(t, ch, 1)
	got := <-ch
	require.Equal(t, `{"flags":{}}`, got.FlagData)
	require.Equal(t, "flags.json", got.Source)

	// Same bytes: no push.
	require.NoError(t, s.fetch(ctx, ch, false /*=force*/))
	require.Empty(t, ch)

	// Changed bytes: push.
	bs.setData([]byte(`{"flags":{"x":true}}`))
	require.NoError(t, s.fetch(ctx, ch, false /*=force*/))
	require.Len(t, ch, 1)
	require.Equal(t, `{"flags":{"x":true}}`, (<-ch).FlagData)
}

// ReSync re-pushes the current config even when the object is unchanged since
// the last push (regression test: ReSync must re-seed flagd's store).
func TestReSync_PushesEvenWhenUnchanged(t *testing.T) {
	ctx := context.Background()
	bs := &fakeBlobstore{data: []byte(`{"flags":{}}`)}
	s := newSyncer(bs)
	ch := make(chan flagdsync.DataSync, 8)

	require.NoError(t, s.fetch(ctx, ch, false /*=force*/))
	require.Len(t, ch, 1)
	<-ch

	// A normal poll of the unchanged object pushes nothing...
	require.NoError(t, s.fetch(ctx, ch, false /*=force*/))
	require.Empty(t, ch)

	// ...but ReSync pushes regardless.
	require.NoError(t, s.ReSync(ctx, ch))
	require.Len(t, ch, 1)
	require.Equal(t, `{"flags":{}}`, (<-ch).FlagData)
}

// An empty object is an error and is never pushed, so it can't replace the last
// known good config.
func TestFetch_EmptyObjectIsError(t *testing.T) {
	ctx := context.Background()
	bs := &fakeBlobstore{data: []byte{}}
	s := newSyncer(bs)
	ch := make(chan flagdsync.DataSync, 1)

	err := s.fetch(ctx, ch, true /*=force*/)
	require.True(t, status.IsFailedPreconditionError(err), "got: %v", err)
	require.Empty(t, ch)
}

// Sync fails (and does not report ready) when the initial object is empty.
func TestSync_EmptyInitialObjectIsFatalAndNotReady(t *testing.T) {
	ctx := context.Background()
	bs := &fakeBlobstore{data: []byte{}}
	s := newSyncer(bs)
	ch := make(chan flagdsync.DataSync, 1)

	require.Error(t, s.Sync(ctx, ch))
	require.False(t, s.IsReady())
	require.Empty(t, ch)
}

// Sync seeds the resolver on startup, reports ready, and returns cleanly when
// the context is cancelled.
func TestSync_SeedsAndBecomesReady(t *testing.T) {
	bs := &fakeBlobstore{data: []byte(`{"flags":{}}`)}
	s := newSyncer(bs)
	ch := make(chan flagdsync.DataSync, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- s.Sync(ctx, ch) }()

	// Initial config is pushed before ready is reported.
	require.Equal(t, `{"flags":{}}`, (<-ch).FlagData)
	require.Eventually(t, s.IsReady, time.Second, time.Millisecond)

	cancel()
	require.NoError(t, <-done)
}

func TestNew_Validation(t *testing.T) {
	ctx := context.Background()

	t.Run("requires bucket", func(t *testing.T) {
		// bucket defaults to "".
		_, err := New(ctx)
		require.True(t, status.IsFailedPreconditionError(err), "got: %v", err)
	})

	t.Run("requires object", func(t *testing.T) {
		flags.Set(t, "experiments.gcs.bucket", "my-bucket")
		_, err := New(ctx)
		require.True(t, status.IsFailedPreconditionError(err), "got: %v", err)
	})

	t.Run("rejects non-positive interval", func(t *testing.T) {
		flags.Set(t, "experiments.gcs.bucket", "my-bucket")
		flags.Set(t, "experiments.gcs.object", "flags.json")
		flags.Set(t, "experiments.gcs.poll_interval", time.Duration(0))
		_, err := New(ctx)
		require.True(t, status.IsInvalidArgumentError(err), "got: %v", err)
	})
}
