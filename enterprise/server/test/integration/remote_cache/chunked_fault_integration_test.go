package remote_cache_test

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/chunking"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/mockgcs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/cdc"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

type resourceExhaustedGCS struct {
	filestore.PebbleGCSStorage
}

type failingCommitWriter struct{}

func (w *failingCommitWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

func (w *failingCommitWriter) Close() error {
	return nil
}

func (w *failingCommitWriter) Commit() error {
	return status.ResourceExhaustedError("too many concurrent writes")
}

func (g *resourceExhaustedGCS) ConditionalWriter(ctx context.Context, blobName string, overwriteExisting bool, customTime time.Time, estimatedSize int64) (interfaces.CommittedWriteCloser, error) {
	return &failingCommitWriter{}, nil
}

type deleteOnCommitGCS struct {
	filestore.PebbleGCSStorage
	deleteAfterCommit atomic.Bool
}

type deleteOnCommitWriter struct {
	interfaces.CommittedWriteCloser
	gcs      filestore.PebbleGCSStorage
	blobName string
	enabled  *atomic.Bool
}

func (w *deleteOnCommitWriter) Commit() error {
	if err := w.CommittedWriteCloser.Commit(); err != nil {
		return err
	}
	if !w.enabled.Load() {
		return nil
	}
	return w.gcs.DeleteBlob(context.Background(), w.blobName)
}

func (g *deleteOnCommitGCS) ConditionalWriter(ctx context.Context, blobName string, overwriteExisting bool, customTime time.Time, estimatedSize int64) (interfaces.CommittedWriteCloser, error) {
	w, err := g.PebbleGCSStorage.ConditionalWriter(ctx, blobName, overwriteExisting, customTime, estimatedSize)
	if err != nil {
		return nil, err
	}
	return &deleteOnCommitWriter{
		CommittedWriteCloser: w,
		gcs:                  g.PebbleGCSStorage,
		blobName:             blobName,
		enabled:              &g.deleteAfterCommit,
	}, nil
}

func setupChunkedPebbleGCSEnv(t *testing.T, gcs filestore.PebbleGCSStorage) (*testenv.TestEnv, context.Context, *grpc.ClientConn) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.chunking_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))
	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)

	var minGCSFileSize int64 = 1
	var maxInlineFileSize int64 = 1
	var gcsTTLDays int64 = 30
	te := testenv.GetTestEnv(t)
	te.SetExperimentFlagProvider(fp)
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{
		Name:                   "pebble_gcs_cache",
		RootDirectory:          testfs.MakeTempDir(t),
		MaxSizeBytes:           1_000_000_000,
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
	te.SetCache(pc)

	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(te)
	require.NoError(t, err)
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(te)
	require.NoError(t, err)
	grpcServer, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runServer()

	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)
	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	te.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	te.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	return te, ctx, conn
}

func computeFastCDCChunkDigests(t *testing.T, ctx context.Context, buf []byte, digestFunction repb.DigestFunction_Value) []*repb.Digest {
	var chunkDigests []*repb.Digest
	c, err := chunking.NewChunker(ctx, int(chunking.AvgChunkSizeBytes(ctx, nil)), func(data []byte) error {
		d, err := digest.Compute(bytes.NewReader(data), digestFunction)
		if err != nil {
			return err
		}
		chunkDigests = append(chunkDigests, d)
		return nil
	})
	require.NoError(t, err)
	_, err = io.Copy(c, bytes.NewReader(buf))
	require.NoError(t, err)
	require.NoError(t, c.Close())
	require.Greater(t, len(chunkDigests), 1)
	return chunkDigests
}

func uploadChunkedBlob(t *testing.T, ctx context.Context, env *testenv.TestEnv, rn *rspb.ResourceName, buf []byte) error {
	ul := cachetools.NewBatchCASUploader(ctx, env, rn.GetInstanceName(), rn.GetDigestFunction(), chunking.FastCDCParams(ctx, nil))
	require.NoError(t, ul.Upload(rn.GetDigest(), cachetools.NewBytesReadSeekCloser(buf)))
	return ul.Wait()
}

func findMissingChunks(t *testing.T, ctx context.Context, env *testenv.TestEnv, chunks []*repb.Digest) []*repb.Digest {
	resp, err := env.GetContentAddressableStorageClient().FindMissingBlobs(cdc.ContextWithChunked(ctx), &repb.FindMissingBlobsRequest{
		DigestFunction: repb.DigestFunction_SHA256,
		BlobDigests:    chunks,
	})
	require.NoError(t, err)
	return resp.GetMissingBlobDigests()
}

func TestChunkedUploadGCSResourceExhaustedFailsBeforeSpliceBlob(t *testing.T) {
	mockGCS := mockgcs.New(clockwork.NewFakeClock())
	te, ctx, _ := setupChunkedPebbleGCSEnv(t, &resourceExhaustedGCS{PebbleGCSStorage: mockGCS})

	rn, buf := testdigest.RandomCASResourceBuf(t, 5*1024*1024)
	chunkDigests := computeFastCDCChunkDigests(t, ctx, buf, rn.GetDigestFunction())
	err := uploadChunkedBlob(t, ctx, te, rn, buf)
	require.Error(t, err)
	require.True(t, status.IsUnavailableError(err), "expected GCS write failure to surface as Unavailable, got %v", err)

	missing := findMissingChunks(t, ctx, te, chunkDigests)
	require.Len(t, missing, len(chunkDigests))
}

func TestChunkedUploadRetriesAfterDanglingGCSChunkMetadata(t *testing.T) {
	mockGCS := mockgcs.New(clockwork.NewFakeClock())
	gcs := &deleteOnCommitGCS{PebbleGCSStorage: mockGCS}
	gcs.deleteAfterCommit.Store(true)
	te, ctx, _ := setupChunkedPebbleGCSEnv(t, gcs)

	rn, buf := testdigest.RandomCASResourceBuf(t, 5*1024*1024)
	chunkDigests := computeFastCDCChunkDigests(t, ctx, buf, rn.GetDigestFunction())
	err := uploadChunkedBlob(t, ctx, te, rn, buf)
	require.Error(t, err)
	require.Len(t, findMissingChunks(t, ctx, te, chunkDigests), len(chunkDigests))

	gcs.deleteAfterCommit.Store(false)
	require.NoError(t, uploadChunkedBlob(t, ctx, te, rn, buf))

	out := &bytes.Buffer{}
	casRN := digest.NewCASResourceName(rn.GetDigest(), rn.GetInstanceName(), rn.GetDigestFunction())
	require.NoError(t, cachetools.GetBlob(ctx, te.GetByteStreamClient(), casRN, out))
	require.Equal(t, buf, out.Bytes())
}
