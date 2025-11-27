package chunk_metadata_store

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/chunker"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

func TestChunkMetadataStore(t *testing.T) {
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("user1", "group1")))

	ctx := context.Background()
	ctx, err := prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
	require.NoError(t, err)

	_, runServer, lis := testenv.RegisterLocalGRPCServer(t, env)
	testcache.Setup(t, env, lis)
	go runServer()

	cache := env.GetCache()
	store := NewChunkMetadataStore(cache)

	instanceName := "test-instance"
	digestFunction := repb.DigestFunction_SHA256

	// Test files from the website static assets filegroup
	// Runfiles paths for workspace files include the _main/ prefix
	testFiles := []string{
		"_main/website/static/img/team/team.png",
		"_main/website/static/img/cache_misses.png",
		"_main/website/static/img/blog/cache_misses.png",
		"_main/website/static/img/blog/members.png",
	}

	for _, testFile := range testFiles {
		t.Run(testFile, func(t *testing.T) {
			filePath := testfs.RunfilePath(t, testFile)
			file, err := os.Open(filePath)
			require.NoError(t, err)
			defer file.Close()

			fileDigest, err := digest.Compute(file, digestFunction)
			require.NoError(t, err)
			_, err = file.Seek(0, io.SeekStart)
			require.NoError(t, err)

			chunkResources := make([]*rspb.ResourceName, 0)
			var mu sync.Mutex
			averageChunkSize := 64 * 1024 // 64KB average chunk size

			writeChunkFn := func(chunkData []byte) error {
				chunkDataCopy := make([]byte, len(chunkData))
				copy(chunkDataCopy, chunkData)

				chunkDigest, err := digest.Compute(bytes.NewReader(chunkDataCopy), digestFunction)
				if err != nil {
					return err
				}

				chunkRN := digest.NewCASResourceName(chunkDigest, instanceName, digestFunction)
				chunkRN.SetCompressor(repb.Compressor_IDENTITY) // Store uncompressed chunks

				if err := cache.Set(ctx, chunkRN.ToProto(), chunkDataCopy); err != nil {
					return err
				}

				mu.Lock()
				chunkResources = append(chunkResources, chunkRN.ToProto())
				mu.Unlock()

				return nil
			}

			chunker, err := chunker.New(ctx, averageChunkSize, writeChunkFn)
			require.NoError(t, err)

			_, err = io.Copy(chunker, file)
			require.NoError(t, err)
			require.NoError(t, chunker.Close())

			require.Greater(t, len(chunkResources), 0, "Expected at least one chunk")

			chunkMetadata := &sgpb.StorageMetadata_ChunkedMetadata{
				Resource: chunkResources,
			}

			require.NoError(t, store.Set(ctx, instanceName, fileDigest, digestFunction, chunkMetadata))

			retrievedMetadata, err := store.Get(ctx, instanceName, fileDigest, digestFunction)
			require.NoError(t, err)
			require.NotNil(t, retrievedMetadata)

			require.Equal(t, len(chunkResources), len(retrievedMetadata.GetResource()))
			for i, expectedChunk := range chunkResources {
				actualChunk := retrievedMetadata.GetResource()[i]
				require.Equal(t, expectedChunk.GetDigest().GetHash(), actualChunk.GetDigest().GetHash())
				require.Equal(t, expectedChunk.GetDigest().GetSizeBytes(), actualChunk.GetDigest().GetSizeBytes())
				require.Equal(t, expectedChunk.GetCompressor(), actualChunk.GetCompressor(), "Compressor field should be preserved")
			}

			for _, chunkRN := range chunkResources {
				chunkData, err := cache.Get(ctx, chunkRN)
				require.NoError(t, err)
				require.NotEmpty(t, chunkData)

				chunkDigest, err := digest.Compute(bytes.NewReader(chunkData), digestFunction)
				require.NoError(t, err)
				require.Equal(t, chunkRN.GetDigest().GetHash(), chunkDigest.GetHash())
				require.Equal(t, chunkRN.GetDigest().GetSizeBytes(), chunkDigest.GetSizeBytes())
			}

			_, err = file.Seek(0, io.SeekStart)
			require.NoError(t, err)
			originalData, err := io.ReadAll(file)
			require.NoError(t, err)

			reconstructedData := make([]byte, 0, len(originalData))
			for _, chunkRN := range retrievedMetadata.GetResource() {
				chunkData, err := cache.Get(ctx, chunkRN)
				require.NoError(t, err)
				reconstructedData = append(reconstructedData, chunkData...)
			}

			require.Equal(t, originalData, reconstructedData, "Reconstructed file should match original")
		})
	}
}

func TestChunkMetadataIndirection(t *testing.T) {
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("user1", "group1")))

	ctx := context.Background()
	ctx, err := prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
	require.NoError(t, err)

	_, runServer, lis := testenv.RegisterLocalGRPCServer(t, env)
	testcache.Setup(t, env, lis)
	go runServer()

	cache := env.GetCache()
	store := NewChunkMetadataStore(cache)

	instanceName := "test-instance"
	digestFunction := repb.DigestFunction_SHA256

	chunkDigest1 := &repb.Digest{Hash: "abc123", SizeBytes: 1024}
	chunkDigest2 := &repb.Digest{Hash: "def456", SizeBytes: 2048}
	chunkMetadata := &sgpb.StorageMetadata_ChunkedMetadata{
		Resource: []*rspb.ResourceName{
			digest.NewCASResourceName(chunkDigest1, instanceName, digestFunction).ToProto(),
			digest.NewCASResourceName(chunkDigest2, instanceName, digestFunction).ToProto(),
		},
	}

	blobDigest := &repb.Digest{Hash: "blob123abc", SizeBytes: 3072}

	require.NoError(t, store.Set(ctx, instanceName, blobDigest, digestFunction, chunkMetadata))

	pointerKey := store.MakeMetadataPointerKey(instanceName, blobDigest, digestFunction)
	pointerBlob, err := cache.Get(ctx, pointerKey)
	require.NoError(t, err)

	pointer := sgpb.StorageMetadata_ChunkedMetadataPointer{}
	require.NoError(t, proto.Unmarshal(pointerBlob, &pointer))
	require.NotNil(t, pointer.GetMetadataDigest())
	require.NotEmpty(t, pointer.GetMetadataDigest().GetHash())
	require.Greater(t, pointer.GetMetadataDigest().GetSizeBytes(), int64(0))

	metadataRN := digest.NewCASResourceName(pointer.GetMetadataDigest(), instanceName, digestFunction)
	metadataBlob, err := cache.Get(ctx, metadataRN.ToProto())
	require.NoError(t, err)

	fetchedMetadata := sgpb.StorageMetadata_ChunkedMetadata{}
	require.NoError(t, proto.Unmarshal(metadataBlob, &fetchedMetadata))
	require.Equal(t, len(chunkMetadata.GetResource()), len(fetchedMetadata.GetResource()))
	require.Equal(t, chunkMetadata.GetResource()[0].GetDigest().GetHash(),
		fetchedMetadata.GetResource()[0].GetDigest().GetHash())

	expectedMetadataBlob, err := proto.Marshal(chunkMetadata)
	require.NoError(t, err)
	expectedDigest, err := digest.Compute(bytes.NewReader(expectedMetadataBlob), digestFunction)
	require.NoError(t, err)
	require.Equal(t, expectedDigest.GetHash(), pointer.GetMetadataDigest().GetHash())
	require.Equal(t, expectedDigest.GetSizeBytes(), pointer.GetMetadataDigest().GetSizeBytes())

	retrievedMetadata, err := store.Get(ctx, instanceName, blobDigest, digestFunction)
	require.NoError(t, err)
	require.Equal(t, len(chunkMetadata.GetResource()), len(retrievedMetadata.GetResource()))
	for i := 0; i < len(chunkMetadata.GetResource()); i++ {
		require.Equal(t, chunkMetadata.GetResource()[i].GetDigest().GetHash(),
			retrievedMetadata.GetResource()[i].GetDigest().GetHash())
	}
}
