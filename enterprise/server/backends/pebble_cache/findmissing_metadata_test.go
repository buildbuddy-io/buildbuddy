package pebble_cache

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/stretchr/testify/require"

	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

func TestParseFindMissingMetadata(t *testing.T) {
	b, err := proto.Marshal(&sgpb.FileMetadata{
		StorageMetadata: &sgpb.StorageMetadata{
			GcsMetadata: &sgpb.StorageMetadata_GCSMetadata{
				BlobName:           "blob",
				LastCustomTimeUsec: 123,
			},
		},
		StoredSizeBytes: 456,
		LastAccessUsec:  789,
	})
	require.NoError(t, err)

	md := &findMissingMetadata{}
	require.NoError(t, parseFindMissingMetadata(b, md))

	require.Equal(t, int64(456), md.storedSizeBytes)
	require.Equal(t, int64(789), md.lastAccessUsec)
	require.True(t, md.hasGCSMetadata)
	require.Equal(t, int64(123), md.gcsLastCustomTimeUsec)
}
