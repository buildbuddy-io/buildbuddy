package mockmetadata_test

import (
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/mockmetadata"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/stretchr/testify/require"

	mdpb "github.com/buildbuddy-io/buildbuddy/proto/metadata"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

func randomFileMetadata(t testing.TB, sizeBytes int64) *sgpb.FileMetadata {
	r, buf := testdigest.RandomCASResourceBuf(t, sizeBytes)
	now := time.Now()
	return &sgpb.FileMetadata{
		FileRecord: &sgpb.FileRecord{
			Isolation: &sgpb.Isolation{
				CacheType:   r.GetCacheType(),
				PartitionId: "groupIDgoeshere",
			},
			Digest:         r.GetDigest(),
			DigestFunction: r.GetDigestFunction(),
		},
		StorageMetadata: &sgpb.StorageMetadata{
			InlineMetadata: &sgpb.StorageMetadata_InlineMetadata{
				Data:          buf,
				CreatedAtNsec: now.UnixNano(),
			},
		},
		StoredSizeBytes: int64(len(buf)),
		LastModifyUsec:  now.UnixMicro(),
		LastAccessUsec:  now.UnixMicro(),
	}
}

func TestBasicOperations(t *testing.T) {
	mm, err := mockmetadata.NewServer(1024)
	require.NoError(t, err)

	// Generate a random FileMetadata.
	fm := randomFileMetadata(t, 100)

	// The FileMetadata should not be found yet because it hasn't been
	// written.
	rsp, err := mm.Find(t.Context(), &mdpb.FindRequest{
		FileRecords: []*sgpb.FileRecord{fm.GetFileRecord()},
	})
	require.NoError(t, err)
	require.Equal(t, len(rsp.GetFindResponses()), 1)
	require.False(t, rsp.GetFindResponses()[0].GetPresent())

	// Write the FileMetadata.
	_, err = mm.Set(t.Context(), &mdpb.SetRequest{
		SetOperations: []*mdpb.SetRequest_SetOperation{{FileMetadata: fm}},
	})
	require.NoError(t, err)

	// The FileMetadata should be found now.
	findRsp, err := mm.Find(t.Context(), &mdpb.FindRequest{
		FileRecords: []*sgpb.FileRecord{fm.GetFileRecord()},
	})
	require.NoError(t, err)
	require.Equal(t, len(findRsp.GetFindResponses()), 1)
	require.True(t, findRsp.GetFindResponses()[0].GetPresent())

	// Reading the FileMetadata should yield a response that is identical
	// to the FileMetadata that was written.
	getRsp, err := mm.Get(t.Context(), &mdpb.GetRequest{
		FileRecords: []*sgpb.FileRecord{fm.GetFileRecord()},
	})
	require.NoError(t, err)
	require.Equal(t, len(getRsp.GetFileMetadatas()), 1)
	require.True(t, proto.Equal(fm, getRsp.GetFileMetadatas()[0]))

	// Deleting the FileMetadata should succeed.
	_, err = mm.Delete(t.Context(), &mdpb.DeleteRequest{
		DeleteOperations: []*mdpb.DeleteRequest_DeleteOperation{{FileRecord: fm.GetFileRecord()}},
	})
	require.NoError(t, err)

	// The FileMetadata should not be found again, because it was deleted.
	rsp, err = mm.Find(t.Context(), &mdpb.FindRequest{
		FileRecords: []*sgpb.FileRecord{fm.GetFileRecord()},
	})
	require.NoError(t, err)
	require.Equal(t, len(rsp.GetFindResponses()), 1)
	require.False(t, rsp.GetFindResponses()[0].GetPresent())

	// Reading the FileMetadata should fail.
	getRsp, err = mm.Get(t.Context(), &mdpb.GetRequest{
		FileRecords: []*sgpb.FileRecord{fm.GetFileRecord()},
	})
	require.NoError(t, err)
	require.Equal(t, len(getRsp.GetFileMetadatas()), 1)
	require.Nil(t, getRsp.GetFileMetadatas()[0])
}
