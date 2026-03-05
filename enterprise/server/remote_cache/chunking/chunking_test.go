package chunking_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/chunking"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

type getMultiCountingCache struct {
	interfaces.Cache
	getMultiCalls int
}

func (c *getMultiCountingCache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	c.getMultiCalls += len(resources)
	return c.Cache.GetMulti(ctx, resources)
}

func TestStore_SharedValidationMarkerSkipsRehashForIdenticalManifest(t *testing.T) {
	flags.Set(t, "cache.chunking.ac_key_salt", "test-salt")

	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{
		RootDirectory: testfs.MakeTempDir(t),
		MaxSizeBytes:  200_000_000,
	})
	require.NoError(t, err)
	require.NoError(t, pc.Start())
	t.Cleanup(func() { pc.Stop() })

	baseCache := pc
	cache := &getMultiCountingCache{Cache: baseCache}

	const blobSize = 100 * 1024 * 1024
	blobData := make([]byte, blobSize)
	_, err = rand.Read(blobData)
	require.NoError(t, err)
	blobDigest, err := digest.Compute(bytes.NewReader(blobData), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	var chunkDigests []*repb.Digest
	c, err := chunking.NewChunker(ctx, int(chunking.AvgChunkSizeBytes()), func(data []byte) error {
		d, err := digest.Compute(bytes.NewReader(data), repb.DigestFunction_BLAKE3)
		if err != nil {
			return err
		}
		buf := make([]byte, len(data))
		copy(buf, data)
		rn := digest.NewCASResourceName(d, "instance-a", repb.DigestFunction_BLAKE3)
		if err := baseCache.Set(ctx, rn.ToProto(), buf); err != nil {
			return err
		}
		chunkDigests = append(chunkDigests, d)
		return nil
	})
	require.NoError(t, err)
	_, err = c.Write(blobData)
	require.NoError(t, err)
	require.NoError(t, c.Close())

	t.Logf("%d chunks, marker size 32 bytes (constant)", len(chunkDigests))

	firstManifest := &chunking.Manifest{
		BlobDigest:     blobDigest,
		ChunkDigests:   chunkDigests,
		InstanceName:   "instance-a",
		DigestFunction: repb.DigestFunction_BLAKE3,
	}
	require.NoError(t, firstManifest.Store(ctx, cache))
	require.Equal(t, len(chunkDigests), cache.getMultiCalls, "first store should hash all chunks")

	cache.getMultiCalls = 0
	secondManifest := &chunking.Manifest{
		BlobDigest:     blobDigest,
		ChunkDigests:   chunkDigests,
		InstanceName:   "instance-b",
		DigestFunction: repb.DigestFunction_BLAKE3,
	}
	require.NoError(t, secondManifest.Store(ctx, cache))
	assert.Equal(t, 0, cache.getMultiCalls, "second store should reuse shared validation marker and open no readers")
}
