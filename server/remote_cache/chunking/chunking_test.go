package chunking_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/chunking"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

func TestChunker_ReassemblesOriginalData(t *testing.T) {
	ctx := context.Background()

	const dataSize = 1 << 20
	originalData := make([]byte, dataSize)
	_, err := rand.Read(originalData)
	require.NoError(t, err)

	var chunks [][]byte
	writeChunkFn := func(data []byte) error {
		// Copy since the underlying buffer may be reused.
		chunk := make([]byte, len(data))
		copy(chunk, data)
		chunks = append(chunks, chunk)
		return nil
	}

	const averageSize = 64 * 1024
	c, err := chunking.NewChunker(ctx, averageSize, writeChunkFn)
	require.NoError(t, err)

	_, err = c.Write(originalData)
	require.NoError(t, err)

	err = c.Close()
	require.NoError(t, err)

	require.Greater(t, len(chunks), 1, "expected multiple chunks for 1MB of data")

	minSize := averageSize / 4
	maxSize := averageSize * 4
	for i, chunk := range chunks {
		if i < len(chunks)-1 {
			require.GreaterOrEqual(t, len(chunk), minSize, "chunk %d is smaller than min size", i)
		}
		require.LessOrEqual(t, len(chunk), maxSize, "chunk %d is larger than max size", i)
	}

	var reassembled bytes.Buffer
	for _, chunk := range chunks {
		reassembled.Write(chunk)
	}
	require.Equal(t, originalData, reassembled.Bytes(), "reassembled data should match original")
}

func TestChunker_DeterministicChunking(t *testing.T) {
	ctx := context.Background()

	const dataSize = 256 * 1024
	originalData := make([]byte, dataSize)
	_, err := rand.Read(originalData)
	require.NoError(t, err)

	const averageSize = 16 * 1024

	var runs [2][][]byte
	for run := 0; run < 2; run++ {
		var chunks [][]byte
		writeChunkFn := func(data []byte) error {
			chunk := make([]byte, len(data))
			copy(chunk, data)
			chunks = append(chunks, chunk)
			return nil
		}

		c, err := chunking.NewChunker(ctx, averageSize, writeChunkFn)
		require.NoError(t, err)

		_, err = c.Write(originalData)
		require.NoError(t, err)

		err = c.Close()
		require.NoError(t, err)

		runs[run] = chunks
	}

	require.Equal(t, len(runs[0]), len(runs[1]), "should produce the same number of chunks")
	for i := range runs[0] {
		require.Equal(t, runs[0][i], runs[1][i], "chunk %d should be identical across runs", i)
	}
}

func TestChunker_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	writeChunkFn := func(data []byte) error {
		return nil
	}

	const averageSize = 16 * 1024
	c, err := chunking.NewChunker(ctx, averageSize, writeChunkFn)
	require.NoError(t, err)

	cancel()

	largeData := make([]byte, 1<<20)
	_, err = c.Write(largeData)
	require.ErrorIs(t, err, context.Canceled)

	err = c.Close()
	require.ErrorIs(t, err, context.Canceled)
}

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

			cm := &chunking.Manifest{
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

			loadedCM, err := chunking.LoadManifest(ctx, cache, blobDigest, "test-instance", repb.DigestFunction_SHA256)
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

func TestLoadWithoutManifest_BlobExists(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)
	cache := te.GetCache()

	blobRN, blobData := testdigest.RandomCASResourceBuf(t, 500)
	require.NoError(t, cache.Set(ctx, blobRN, blobData))

	_, err = chunking.LoadManifest(ctx, cache, blobRN.GetDigest(), "", repb.DigestFunction_SHA256)
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

	_, err = chunking.LoadManifest(ctx, cache, blobRN.GetDigest(), "", repb.DigestFunction_SHA256)
	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err))
}

func TestLoadWithoutManifest_SaltedHashNotLeaked(t *testing.T) {
	const salt = "test-salt"
	flags.Set(t, "cache.chunking.ac_key_salt", salt)

	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	blobRN, _ := testdigest.RandomCASResourceBuf(t, 500)
	blobDigest := blobRN.GetDigest()

	saltedDigest, err := digest.Compute(bytes.NewReader([]byte(salt+":"+blobDigest.GetHash())), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	_, err = chunking.LoadManifest(ctx, te.GetCache(), blobDigest, "", repb.DigestFunction_SHA256)

	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err))
	assert.Contains(t, err.Error(), blobDigest.GetHash())
	assert.NotContains(t, err.Error(), saltedDigest.GetHash())
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

	cm := &chunking.Manifest{
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

func TestDigestsSummary(t *testing.T) {
	d := func(hash string, size int64) *repb.Digest {
		return &repb.Digest{Hash: hash, SizeBytes: size}
	}

	tests := []struct {
		name    string
		digests []*repb.Digest
		want    string
	}{
		{"empty", nil, ""},
		{"one", []*repb.Digest{d("abc", 10)}, "abc/10"},
		{"three", []*repb.Digest{d("a", 1), d("b", 2), d("c", 3)}, "a/1, b/2, c/3"},
		{"four", []*repb.Digest{d("a", 1), d("b", 2), d("c", 3), d("d", 4)}, "a/1, b/2, c/3 (4 total)"},
		{"six", []*repb.Digest{d("a", 1), d("b", 2), d("c", 3), d("d", 4), d("e", 5), d("f", 6)}, "a/1, b/2, c/3 (6 total)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := chunking.DigestsSummary(tc.digests)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestMissingChunkChecker(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	trackingCache := &findMissingTrackingCache{Cache: te.GetCache()}

	chunk1RN, chunk1Data := testdigest.RandomCASResourceBuf(t, 100)
	chunk2RN, chunk2Data := testdigest.RandomCASResourceBuf(t, 150)
	chunk3RN, chunk3Data := testdigest.RandomCASResourceBuf(t, 200)

	require.NoError(t, trackingCache.Set(ctx, chunk1RN, chunk1Data))
	require.NoError(t, trackingCache.Set(ctx, chunk2RN, chunk2Data))

	allData := append(append(chunk1Data, chunk2Data...), chunk3Data...)
	blobDigest, err := digest.Compute(bytes.NewReader(allData), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	manifestAllPresent := &chunking.Manifest{
		BlobDigest:     blobDigest,
		ChunkDigests:   []*repb.Digest{chunk1RN.GetDigest(), chunk2RN.GetDigest()},
		InstanceName:   "",
		DigestFunction: repb.DigestFunction_SHA256,
	}
	manifestWithMissing := &chunking.Manifest{
		BlobDigest:     blobDigest,
		ChunkDigests:   []*repb.Digest{chunk1RN.GetDigest(), chunk3RN.GetDigest()},
		InstanceName:   "",
		DigestFunction: repb.DigestFunction_SHA256,
	}

	checker := chunking.NewMissingChunkChecker(trackingCache)

	missing, err := checker.AnyChunkMissing(ctx, manifestAllPresent)
	require.NoError(t, err)
	assert.False(t, missing, "expected no missing chunks")
	assert.Equal(t, 1, trackingCache.findMissingCalls, "expected one FindMissing call")

	missing, err = checker.AnyChunkMissing(ctx, manifestAllPresent)
	require.NoError(t, err)
	assert.False(t, missing, "expected no missing chunks (cached)")
	assert.Equal(t, 1, trackingCache.findMissingCalls, "expected no additional FindMissing call")

	missing, err = checker.AnyChunkMissing(ctx, manifestWithMissing)
	require.NoError(t, err)
	assert.True(t, missing, "expected missing chunk")
	assert.Equal(t, 2, trackingCache.findMissingCalls, "expected one more FindMissing call for unknown chunk")

	missing, err = checker.AnyChunkMissing(ctx, manifestWithMissing)
	require.NoError(t, err)
	assert.True(t, missing, "expected missing chunk (cached)")
	assert.Equal(t, 2, trackingCache.findMissingCalls, "expected no additional FindMissing call")
}

type findMissingTrackingCache struct {
	interfaces.Cache
	findMissingCalls int
}

func (c *findMissingTrackingCache) FindMissing(ctx context.Context, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	c.findMissingCalls++
	return c.Cache.FindMissing(ctx, resources)
}

func BenchmarkStore(b *testing.B) {
	*log.LogLevel = "error"
	log.Configure()

	sizes := []int{1 * 1024 * 1024, 10 * 1024 * 1024, 100 * 1024 * 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			ctx := context.Background()
			te := testenv.GetTestEnv(b)
			ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
			require.NoError(b, err)
			cache := te.GetCache()

			blobRN, blobData := testdigest.RandomCASResourceBuf(b, int64(size))

			const chunkSize = 512 * 1024
			var chunkDigests []*repb.Digest
			for i := 0; i < len(blobData); i += chunkSize {
				end := i + chunkSize
				if end > len(blobData) {
					end = len(blobData)
				}
				chunk := blobData[i:end]
				d, err := digest.Compute(bytes.NewReader(chunk), repb.DigestFunction_SHA256)
				require.NoError(b, err)
				chunkDigests = append(chunkDigests, d)
				rn := digest.NewCASResourceName(d, "bench", repb.DigestFunction_SHA256)
				require.NoError(b, cache.Set(ctx, rn.ToProto(), chunk))
			}

			cm := &chunking.Manifest{
				BlobDigest:     blobRN.GetDigest(),
				ChunkDigests:   chunkDigests,
				InstanceName:   "bench",
				DigestFunction: repb.DigestFunction_SHA256,
			}

			for b.Loop() {
				if err := cm.Store(ctx, cache); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
