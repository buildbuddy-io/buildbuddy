package distributed

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/mockgcs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

type commitFailingGCS struct {
	filestore.PebbleGCSStorage
	commitErr error
}

type commitFailingWriter struct {
	err error
}

func (w *commitFailingWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

func (w *commitFailingWriter) Close() error {
	return nil
}

func (w *commitFailingWriter) Commit() error {
	return w.err
}

func (g *commitFailingGCS) ConditionalWriter(ctx context.Context, blobName string, overwriteExisting bool, customTime time.Time, estimatedSize int64) (interfaces.CommittedWriteCloser, error) {
	return &commitFailingWriter{err: g.commitErr}, nil
}

type deletingGCS struct {
	filestore.PebbleGCSStorage
}

type deletingWriter struct {
	interfaces.CommittedWriteCloser
	gcs      filestore.PebbleGCSStorage
	blobName string
}

func (w *deletingWriter) Commit() error {
	if err := w.CommittedWriteCloser.Commit(); err != nil {
		return err
	}
	return w.gcs.DeleteBlob(context.Background(), w.blobName)
}

func (g *deletingGCS) ConditionalWriter(ctx context.Context, blobName string, overwriteExisting bool, customTime time.Time, estimatedSize int64) (interfaces.CommittedWriteCloser, error) {
	w, err := g.PebbleGCSStorage.ConditionalWriter(ctx, blobName, overwriteExisting, customTime, estimatedSize)
	if err != nil {
		return nil, err
	}
	return &deletingWriter{
		CommittedWriteCloser: w,
		gcs:                  g.PebbleGCSStorage,
		blobName:             blobName,
	}, nil
}

func newProdPebbleGCSCache(t testing.TB, env environment.Env, gcs filestore.PebbleGCSStorage) *pebble_cache.PebbleCache {
	var minGCSFileSize int64 = 1
	var maxInlineFileSize int64 = 1
	var gcsTTLDays int64 = 30

	pc, err := pebble_cache.NewPebbleCache(env, &pebble_cache.Options{
		Name:                   "pebble_gcs_cache",
		RootDirectory:          testfs.MakeTempDir(t),
		MaxSizeBytes:           1_000_000,
		FileStorer:             filestore.New(filestore.WithGCSBlobstore(gcs, "app-name")),
		MinGCSFileSizeBytes:    &minGCSFileSize,
		MaxInlineFileSizeBytes: maxInlineFileSize,
		GCSTTLDays:             &gcsTTLDays,
	})
	require.NoError(t, err)
	require.NoError(t, pc.Start())
	t.Cleanup(func() {
		require.NoError(t, pc.Stop())
	})
	return pc
}

func newProdDistributedCache(t *testing.T, env environment.Env, base interfaces.Cache) *Cache {
	listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := startNewDCache(t, env, Options{
		ListenAddr:              listenAddr,
		GroupName:               "default",
		ReplicationFactor:       1,
		Nodes:                   []string{listenAddr},
		LookasideCacheSizeBytes: 1_000_000,
	}, base)
	waitForReady(t, listenAddr)
	return c
}

func requireFindMissing(t testing.TB, ctx context.Context, c interfaces.Cache, rn *rspb.ResourceName) {
	missing, err := c.FindMissing(ctx, []*rspb.ResourceName{rn})
	require.NoError(t, err)
	require.Len(t, missing, 1)
	require.Equal(t, rn.GetDigest().GetHash(), missing[0].GetHash())
}

func TestProdCacheStackResourceExhaustedCommitDoesNotWriteMetadata(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	clock := clockwork.NewFakeClock()
	mockGCS := mockgcs.New(clock)
	require.NoError(t, mockGCS.SetBucketCustomTimeTTL(ctx, 30))
	base := newProdPebbleGCSCache(t, env, &commitFailingGCS{
		PebbleGCSStorage: mockGCS,
		commitErr:        status.ResourceExhaustedError("too many concurrent writes"),
	})
	dc := newProdDistributedCache(t, env, base)

	rn, buf := testdigest.RandomCASResourceBuf(t, 100)
	err := dc.Set(ctx, rn, buf)
	require.Error(t, err)
	require.True(t, status.IsUnavailableError(err), "expected Unavailable, got %v", err)
	requireFindMissing(t, ctx, base, rn)
	requireFindMissing(t, ctx, dc, rn)
}

func TestProdCacheStackFindMissingDoesNotTrustDanglingGCSMetadata(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	clock := clockwork.NewFakeClock()
	mockGCS := mockgcs.New(clock)
	require.NoError(t, mockGCS.SetBucketCustomTimeTTL(ctx, 30))
	base := newProdPebbleGCSCache(t, env, &deletingGCS{PebbleGCSStorage: mockGCS})
	dc := newProdDistributedCache(t, env, base)

	rn, buf := testdigest.RandomCASResourceBuf(t, 100)
	require.NoError(t, dc.Set(ctx, rn, buf))
	requireFindMissing(t, ctx, base, rn)
	requireFindMissing(t, ctx, dc, rn)
}
