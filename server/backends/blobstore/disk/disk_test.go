package disk

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/util"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
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

type namedBlob struct {
	name string
	buf  []byte
}

func getBlobs(t testing.TB, num int, sizeBytes int64) []*namedBlob {
	blobs := make([]*namedBlob, 0, num)
	for i := 0; i < num; i++ {
		d, b := testdigest.RandomCASResourceBuf(t, sizeBytes)
		blobs = append(blobs, &namedBlob{
			name: d.GetDigest().GetHash(),
			buf:  b,
		})
	}
	return blobs
}

func writeBlobs(t testing.TB, ctx context.Context, bs interfaces.Blobstore, blobs []*namedBlob) {
	for _, b := range blobs {
		if _, err := bs.WriteBlob(ctx, b.name, b.buf); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkReadBlob(b *testing.B) {
	originalRootDir := *rootDirectory
	*rootDirectory = b.TempDir()
	b.Cleanup(func() {
		*rootDirectory = originalRootDir
	})
	var bs interfaces.Blobstore
	bs, err := NewDiskBlobStore()
	ctx := context.Background()
	require.NoError(b, err)
	for _, size := range []int64{10, 1e3, 1e4, 1e5, 1e6} {
		blobs := getBlobs(b, 100, size)
		writeBlobs(b, ctx, bs, blobs)
		name := fmt.Sprintf("size=%d", size)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				namedBlob := blobs[rand.Intn(len(blobs))]
				b.SetBytes(int64(len(namedBlob.buf)))
				_, err = bs.ReadBlob(ctx, namedBlob.name)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkWriteBlob(b *testing.B) {
	originalRootDir := *rootDirectory
	*rootDirectory = b.TempDir()
	b.Cleanup(func() {
		*rootDirectory = originalRootDir
	})
	var bs interfaces.Blobstore
	bs, err := NewDiskBlobStore()
	ctx := context.Background()
	require.NoError(b, err)
	for _, size := range []int64{10, 1e3, 1e4, 1e5, 1e6} {
		blobs := getBlobs(b, 100, size)
		writeBlobs(b, ctx, bs, blobs)
		name := fmt.Sprintf("size=%d", size)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				namedBlob := blobs[rand.Intn(len(blobs))]
				b.SetBytes(int64(len(namedBlob.buf)))
				_, err = bs.WriteBlob(ctx, namedBlob.name, namedBlob.buf)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
