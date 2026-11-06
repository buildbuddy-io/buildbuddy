package aws

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/util"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
)

type fakeDownloadClient struct {
	getObject func(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

func (c *fakeDownloadClient) GetObject(ctx context.Context, input *s3.GetObjectInput, options ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return c.getObject(ctx, input, options...)
}

type errorReader struct {
	err error
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}

func newTestBlobStore(client *fakeDownloadClient) *AwsS3BlobStore {
	bucket := "bucket"
	return &AwsS3BlobStore{
		bucket: &bucket,
		downloader: manager.NewDownloader(client, func(d *manager.Downloader) {
			d.Concurrency = 1
			d.PartBodyMaxRetries = 0
		}),
	}
}

func TestDownloadNoSuchKey(t *testing.T) {
	store := newTestBlobStore(&fakeDownloadClient{
		getObject: func(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return nil, fmt.Errorf("request failed: %w", &types.NoSuchKey{})
		},
	})

	data, err := store.download(context.Background(), "missing")
	require.Nil(t, data)
	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err))
}

func TestDownloadReturnsErrorWithoutPartialData(t *testing.T) {
	downloadErr := errors.New("download failed")
	partialData := []byte("partial data")
	store := newTestBlobStore(&fakeDownloadClient{
		getObject: func(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			contentLength := int64(len(partialData))
			return &s3.GetObjectOutput{
				Body:          io.NopCloser(io.MultiReader(bytes.NewReader(partialData), errorReader{err: downloadErr})),
				ContentLength: &contentLength,
			}, nil
		},
	})

	data, err := store.download(context.Background(), "blob")
	require.Nil(t, data)
	require.ErrorIs(t, err, downloadErr)
}

func TestReadBlobDecompressesDownloadedData(t *testing.T) {
	want := []byte("uncompressed data")
	compressed, err := util.Compress(want)
	require.NoError(t, err)
	store := newTestBlobStore(&fakeDownloadClient{
		getObject: func(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			contentLength := int64(len(compressed))
			return &s3.GetObjectOutput{
				Body:          io.NopCloser(bytes.NewReader(compressed)),
				ContentLength: &contentLength,
			}, nil
		},
	})

	data, err := store.ReadBlob(context.Background(), "blob")
	require.NoError(t, err)
	require.Equal(t, want, data)
}
