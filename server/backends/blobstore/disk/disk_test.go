package disk

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/util"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/stretchr/testify/require"
)

func TestDiskBlobStore(t *testing.T) {
	for _, tc := range []struct {
		name             string
		prefix, blobName string
		blob             []byte
		useWriter        bool
	}{
		{
			name:      "WithoutPrefix",
			prefix:    "",
			blobName:  "test_blob",
			blob:      []byte("test"),
			useWriter: false,
		},
		{
			name:      "WithPrefix",
			prefix:    "my_prefix",
			blobName:  "test_blob",
			blob:      []byte("test"),
			useWriter: false,
		},
		{
			name:      "WithoutPrefixUseWriter",
			prefix:    "",
			blobName:  "test_blob",
			blob:      []byte("test"),
			useWriter: true,
		},
		{
			name:      "WithPrefixUseWriter",
			prefix:    "my_prefix",
			blobName:  "test_blob",
			blob:      []byte("test"),
			useWriter: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			originalRootDir := *rootDirectory
			*rootDirectory = t.TempDir()
			t.Cleanup(func() {
				*rootDirectory = originalRootDir
			})

			var bs interfaces.Blobstore
			bs, err := NewDiskBlobStore()
			require.NoError(t, err)
			if tc.prefix != "" {
				bs = util.NewPrefixBlobstore(bs, tc.prefix)
			}

			ctx := context.Background()
			if tc.useWriter {
				w, err := bs.Writer(ctx, tc.blobName)
				require.NoError(t, err)
				n, err := w.Write(tc.blob)
				require.NoError(t, err)
				require.Greater(t, n, 0)

				// The blob should not exist until we commit.
				exist, err := bs.BlobExists(ctx, tc.blobName)
				require.NoError(t, err)
				require.True(t, !exist)

				require.NoError(t, w.Commit())
				require.NoError(t, w.Close())
			} else {
				n, err := bs.WriteBlob(ctx, tc.blobName, tc.blob)
				require.NoError(t, err)
				require.Greater(t, n, 0)
			}

			path := filepath.Join(*rootDirectory, tc.prefix, tc.blobName)
			require.FileExists(t, path)

			exist, err := bs.BlobExists(ctx, tc.blobName)
			require.NoError(t, err)
			require.True(t, exist)

			b, err := bs.ReadBlob(ctx, tc.blobName)
			require.NoError(t, err)
			require.Equal(t, b, tc.blob)

			err = bs.DeleteBlob(ctx, tc.blobName)
			require.NoError(t, err)

			exist, err = bs.BlobExists(ctx, tc.blobName)
			require.NoError(t, err)
			require.False(t, exist)
		})
	}
}
