package blobstore

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/aws"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/azure"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/disk"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/gcs"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/util"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

func Register(env *real_environment.RealEnv) error {
	bs, err := NewFromConfig(env.GetServerContext())
	if err != nil {
		return err
	}
	env.SetBlobstore(bs)
	return nil
}

// NewFromConfig returns whatever blobstore is specified in the config.
func NewFromConfig(ctx context.Context) (interfaces.Blobstore, error) {
	bs, err := getBlobstore(ctx)
	if err != nil {
		return bs, err
	}
	return util.NewDefaultPrefixBlobstore(bs), nil
}

func getBlobstore(ctx context.Context) (interfaces.Blobstore, error) {
	log.Debug("Configuring blobstore")
	if gcs.UseGCSBlobStore() {
		log.Debug("Configuring GCS blobstore")
		return gcs.NewGCSBlobStore(ctx, true /*=enableCompression*/)
	}
	if aws.UseAwsS3BlobStore() {
		log.Debug("Configuring AWS blobstore")
		return aws.NewAwsS3BlobStore(ctx)
	}
	if azure.UseAzureBlobStore() {
		log.Debug("Configuring Azure blobstore")
		return azure.NewAzureBlobStore(ctx)
	}
	if disk.UseDiskBlobStore() {
		log.Debug("Disk blobstore configured")
		return disk.NewDiskBlobStore()
	}
	return nil, fmt.Errorf("No storage backend configured -- please specify at least one in the config")
}
