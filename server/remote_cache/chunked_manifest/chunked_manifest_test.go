package chunked_manifest_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/chunked_manifest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestStoreAndLoad(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)
	cache := te.GetCache()

	chunk1RN, chunk1Data := testdigest.RandomCASResourceBuf(t, 100)
	chunk2RN, chunk2Data := testdigest.RandomCASResourceBuf(t, 150)
	chunk3RN, chunk3Data := testdigest.RandomCASResourceBuf(t, 200)

	require.NoError(t, cache.Set(ctx, chunk1RN, chunk1Data))
	require.NoError(t, cache.Set(ctx, chunk2RN, chunk2Data))
	require.NoError(t, cache.Set(ctx, chunk3RN, chunk3Data))

	allData := append(append(chunk1Data, chunk2Data...), chunk3Data...)
	blobDigest, err := digest.Compute(bytes.NewReader(allData), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	cm := &chunked_manifest.ChunkedManifest{
		BlobDigest:     blobDigest,
		ChunkDigests:   []*repb.Digest{chunk1RN.GetDigest(), chunk2RN.GetDigest(), chunk3RN.GetDigest()},
		InstanceName:   "test-instance",
		DigestFunction: repb.DigestFunction_SHA256,
	}

	badDigest, err := digest.Compute(bytes.NewReader([]byte("random data")), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	cm.BlobDigest = badDigest

	require.Error(t, cm.Store(ctx, cache))
	cm.BlobDigest = blobDigest
	require.NoError(t, cm.Store(ctx, cache))

	loadedCM, err := chunked_manifest.Load(ctx, cache, blobDigest, "test-instance", repb.DigestFunction_SHA256)
	require.NoError(t, err)

	assert.Equal(t, cm.BlobDigest.GetHash(), loadedCM.BlobDigest.GetHash())
	assert.Equal(t, cm.BlobDigest.GetSizeBytes(), loadedCM.BlobDigest.GetSizeBytes())
	assert.Equal(t, len(cm.ChunkDigests), len(loadedCM.ChunkDigests))
	for i := range cm.ChunkDigests {
		assert.Equal(t, cm.ChunkDigests[i].GetHash(), loadedCM.ChunkDigests[i].GetHash())
		assert.Equal(t, cm.ChunkDigests[i].GetSizeBytes(), loadedCM.ChunkDigests[i].GetSizeBytes())
	}
	assert.Equal(t, cm.InstanceName, loadedCM.InstanceName)
	assert.Equal(t, cm.DigestFunction, loadedCM.DigestFunction)
}

func TestLoadNotFoundDueToMissingChunk(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)
	cache := te.GetCache()

	chunk1RN, chunk1Data := testdigest.RandomCASResourceBuf(t, 100)
	chunk2RN, chunk2Data := testdigest.RandomCASResourceBuf(t, 150)
	chunk3RN, chunk3Data := testdigest.RandomCASResourceBuf(t, 200)

	require.NoError(t, cache.Set(ctx, chunk1RN, chunk1Data))
	require.NoError(t, cache.Set(ctx, chunk2RN, chunk2Data))
	require.NoError(t, cache.Set(ctx, chunk3RN, chunk3Data))

	allData := append(append(chunk1Data, chunk2Data...), chunk3Data...)
	blobDigest, err := digest.Compute(bytes.NewReader(allData), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	cm := &chunked_manifest.ChunkedManifest{
		BlobDigest:     blobDigest,
		ChunkDigests:   []*repb.Digest{chunk1RN.GetDigest(), chunk2RN.GetDigest(), chunk3RN.GetDigest()},
		InstanceName:   "test-instance",
		DigestFunction: repb.DigestFunction_SHA256,
	}

	cm.BlobDigest = blobDigest
	require.NoError(t, cm.Store(ctx, cache))

	cache.Delete(ctx, chunk3RN)

	_, err = chunked_manifest.Load(ctx, cache, blobDigest, "test-instance", repb.DigestFunction_SHA256)
	require.Error(t, err)
	require.ErrorContains(t, err, "manifest found with missing chunks")
	require.True(t, status.IsNotFoundError(err))
}
