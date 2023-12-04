package azure

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/util"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
)

var (
	// Azure flags
	azureAccountName   = flag.String("storage.azure.account_name", "", "The name of the Azure storage account")
	azureAccountKey    = flag.String("storage.azure.account_key", "", "The key for the Azure storage account", flag.Secret)
	azureContainerName = flag.String("storage.azure.container_name", "", "The name of the Azure storage container")
)

const (
	// Prometheus BlobstoreTypeLabel values
	azureLabel = "azure"
)

const azureURLTemplate = "https://%s.blob.core.windows.net/%s"

// GCSBlobStore implements the blobstore API on top of the google cloud storage API.
type AzureBlobStore struct {
	credential    *azblob.SharedKeyCredential
	containerName string
	containerURL  *azblob.ContainerURL
}

func UseAzureBlobStore() bool {
	return *azureAccountName != ""
}

func NewAzureBlobStore(ctx context.Context) (*AzureBlobStore, error) {
	credential, err := azblob.NewSharedKeyCredential(*azureAccountName, *azureAccountKey)
	if err != nil {
		return nil, err
	}
	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	portalURL, err := url.Parse(fmt.Sprintf(azureURLTemplate, *azureAccountName, *azureContainerName))
	if err != nil {
		return nil, err
	}
	containerURL := azblob.NewContainerURL(*portalURL, pipeline)
	z := &AzureBlobStore{
		credential:    credential,
		containerName: *azureContainerName,
		containerURL:  &containerURL,
	}
	if err := z.createContainerIfNotExists(ctx); err != nil {
		return nil, err
	}
	log.Debugf("Azure blobstore configured (container: %q)", *azureContainerName)
	return z, nil
}

func (z *AzureBlobStore) isAzureError(err error, code azblob.ServiceCodeType) bool {
	if serr, ok := err.(azblob.StorageError); ok {
		if serr.ServiceCode() == code {
			return true
		}
	}
	return false
}

func (z *AzureBlobStore) containerExists(ctx context.Context) (bool, error) {
	ctx, spn := tracing.StartSpan(ctx)
	_, err := z.containerURL.GetProperties(ctx, azblob.LeaseAccessConditions{})
	spn.End()
	if err == nil {
		return true, nil
	}
	if !z.isAzureError(err, azblob.ServiceCodeContainerNotFound) {
		return false, err
	}
	return false, nil
}

func (z *AzureBlobStore) createContainerIfNotExists(ctx context.Context) error {
	if exists, err := z.containerExists(ctx); err != nil {
		return err
	} else if !exists {
		ctx, spn := tracing.StartSpan(ctx)
		defer spn.End()
		_, err = z.containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
		return err
	}
	return nil
}

func (z *AzureBlobStore) ReadBlob(ctx context.Context, blobName string) ([]byte, error) {
	blobURL := z.containerURL.NewBlockBlobURL(blobName)
	response, err := blobURL.Download(ctx, 0 /*=offset*/, azblob.CountToEnd, azblob.BlobAccessConditions{}, false /*=rangeGetContentMD5*/, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		if z.isAzureError(err, azblob.ServiceCodeBlobNotFound) {
			return nil, status.NotFoundError(err.Error())
		}
		return nil, err
	}

	start := time.Now()
	readCloser := response.Body(azblob.RetryReaderOptions{})
	defer readCloser.Close()
	counter := &ioutil.Counter{}
	reader := io.TeeReader(readCloser, counter)
	duration := time.Since(start)
	bytes, err := util.Decompress(reader)
	util.RecordReadMetrics(azureLabel, duration, counter.Count(), err)
	return bytes, err
}

func (z *AzureBlobStore) WriteBlob(ctx context.Context, blobName string, data []byte) (int, error) {
	compressedData, err := util.Compress(data)
	if err != nil {
		return 0, err
	}
	n := len(compressedData)
	start := time.Now()
	blobURL := z.containerURL.NewBlockBlobURL(blobName)
	ctx, spn := tracing.StartSpan(ctx)
	_, err = azblob.UploadBufferToBlockBlob(ctx, compressedData, blobURL, azblob.UploadToBlockBlobOptions{})
	spn.End()
	util.RecordWriteMetrics(azureLabel, start, n, err)
	return n, err
}

func (z *AzureBlobStore) DeleteBlob(ctx context.Context, blobName string) error {
	start := time.Now()
	blobURL := z.containerURL.NewBlockBlobURL(blobName)
	ctx, spn := tracing.StartSpan(ctx)
	_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	spn.End()
	util.RecordDeleteMetrics(azureLabel, start, err)
	return err
}

func (z *AzureBlobStore) BlobExists(ctx context.Context, blobName string) (bool, error) {
	blobURL := z.containerURL.NewBlockBlobURL(blobName)
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	if _, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{}); err != nil {
		if z.isAzureError(err, azblob.ServiceCodeBlobNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (z *AzureBlobStore) Writer(ctx context.Context, blobName string) (interfaces.CommittedWriteCloser, error) {
	// Open a pipe.
	pr, pw := io.Pipe()

	errch := make(chan error, 1)

	ctx, cancel := context.WithCancel(ctx)

	// Upload from pr in a separate Go routine.
	go func() {
		defer pr.Close()
		_, err := azblob.UploadStreamToBlockBlob(
			ctx,
			pr,
			z.containerURL.NewBlockBlobURL(blobName),
			azblob.UploadStreamToBlockBlobOptions{},
		)
		errch <- err
		close(errch)
	}()

	zw := util.NewCompressWriter(pw)
	cwc := ioutil.NewCustomCommitWriteCloser(zw)
	cwc.CommitFn = func(int64) error {
		if compresserCloseErr := zw.Close(); compresserCloseErr != nil {
			cancel() // Don't try to finish the commit op if Close() failed.
			if pipeCloseErr := pw.Close(); pipeCloseErr != nil {
				log.Errorf("Error closing the pipe for %s: %s", blobName, pipeCloseErr)
			}
			// Canceling the context makes any error in errch meaningless; don't
			// bother to read it.
			return compresserCloseErr
		}
		if writerCloseErr := pw.Close(); writerCloseErr != nil {
			return writerCloseErr
		}
		return <-errch
	}
	cwc.CloseFn = func() error {
		cancel()
		return nil
	}
	return cwc, nil
}
