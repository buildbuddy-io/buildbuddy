package chunked_manifest_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/chunked_manifest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

func TestStoreAndLoad(t *testing.T) {
	for _, salt := range []string{"", "test-salt"} {
		t.Run(fmt.Sprintf("salt=%s", salt), func(t *testing.T) {
			if salt != "" {
				flags.Set(t, "cache.chunking.ac_key_salt", salt)
			}

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
		})
	}
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

	require.NoError(t, cm.Store(ctx, cache))

	cache.Delete(ctx, chunk3RN)

	_, err = chunked_manifest.Load(ctx, cache, blobDigest, "test-instance", repb.DigestFunction_SHA256)
	require.Error(t, err)
	require.EqualError(t, err, "rpc error: code = NotFound desc = required chunks not found in CAS: be1a87b87a0e01c5952aff653918c80039fecad566da0df4475c35ba1b04d00e/200")
	require.True(t, status.IsNotFoundError(err))
}

func TestLoadWithoutManifest_BlobExists(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)
	cache := te.GetCache()

	blobRN, blobData := testdigest.RandomCASResourceBuf(t, 500)
	require.NoError(t, cache.Set(ctx, blobRN, blobData))

	// Load should return Unimplemented since the blob exists but wasn't stored with chunking.
	_, err = chunked_manifest.Load(ctx, cache, blobRN.GetDigest(), "", repb.DigestFunction_SHA256)
	require.Error(t, err)
	require.True(t, status.IsUnimplementedError(err))
	assert.Contains(t, err.Error(), "exists but was not stored with chunking")
}

func TestLoadWithoutManifest_BlobMissing(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)
	cache := te.GetCache()

	blobRN, _ := testdigest.RandomCASResourceBuf(t, 500)

	_, err = chunked_manifest.Load(ctx, cache, blobRN.GetDigest(), "", repb.DigestFunction_SHA256)
	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err))
}

func TestStore_ErrGroupContextCancellation(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	wrapper := &contextCheckingCache{
		Cache: te.GetCache(),
		t:     t,
	}

	chunk1RN, chunk1Data := testdigest.RandomCASResourceBuf(t, 100)
	chunk2RN, chunk2Data := testdigest.RandomCASResourceBuf(t, 150)

	require.NoError(t, wrapper.Set(ctx, chunk1RN, chunk1Data))
	require.NoError(t, wrapper.Set(ctx, chunk2RN, chunk2Data))

	allData := append(chunk1Data, chunk2Data...)
	blobDigest, err := digest.Compute(bytes.NewReader(allData), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	cm := &chunked_manifest.ChunkedManifest{
		BlobDigest:     blobDigest,
		ChunkDigests:   []*repb.Digest{chunk1RN.GetDigest(), chunk2RN.GetDigest()},
		InstanceName:   "test-instance",
		DigestFunction: repb.DigestFunction_SHA256,
	}

	wrapper.checkContextOnSet = true
	err = cm.Store(ctx, wrapper)
	require.NoError(t, err, "Store should not fail due to errgroup context cancellation")
	require.True(t, wrapper.setWasCalled, "cache.Set should have been called")
}

type contextCheckingCache struct {
	interfaces.Cache

	t                 *testing.T
	checkContextOnSet bool
	setWasCalled      bool
}

func (c *contextCheckingCache) Set(ctx context.Context, r *rspb.ResourceName, data []byte) error {
	if c.checkContextOnSet {
		c.setWasCalled = true
		require.NoError(c.t, ctx.Err())
	}
	return c.Cache.Set(ctx, r, data)
}
