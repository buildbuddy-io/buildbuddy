package blobstore

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/tryflame/buildbuddy/server/config"
	"google.golang.org/api/option"
)

// A Blobstore must allow for reading, writing, and deleting blobs.
type Blobstore interface {
	ReadBlob(ctx context.Context, blobName string) ([]byte, error)
	WriteBlob(ctx context.Context, blobName string, data []byte) error
	DeleteBlob(ctx context.Context, blobName string) error
}

// Returns whatever blobstore is specified in the config.
func GetConfiguredBlobstore(c *config.Configurator) (Blobstore, error) {
	if c.GetStorageDiskRootDir() != "" {
		return NewDiskBlobStore(c.GetStorageDiskRootDir()), nil
	}
	if gcsConfig := c.GetStorageGCSConfig(); gcsConfig != nil {
		opts := make([]option.ClientOption, 0)
		if gcsConfig.CredentialsFile != "" {
			opts = append(opts, option.WithCredentialsFile(gcsConfig.CredentialsFile))
		}
		return NewGCSBlobStore(gcsConfig.Bucket, gcsConfig.ProjectID, opts...)
	}
	return nil, fmt.Errorf("No storage backend configured -- please specify at least one in the config")
}

func ensureDirectoryExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return nil
}

// A super simple disk-based blob storage implementation.
type DiskBlobStore struct {
	rootDir string
}

func NewDiskBlobStore(rootDir string) *DiskBlobStore {
	return &DiskBlobStore{
		rootDir: rootDir,
	}
}

func (d *DiskBlobStore) WriteBlob(ctx context.Context, blobName string, data []byte) error {
	// Probably could support nesting in directories here but we are lazy.
	if strings.Contains(blobName, "..") {
		return fmt.Errorf("blobName (%s) must not contain ../", blobName)
	}
	fullPath := filepath.Join(d.rootDir, blobName)
	if err := ensureDirectoryExists(filepath.Dir(fullPath)); err != nil {
		return err
	}
	tmpFileName := fullPath + ".tmp"
	if err := ioutil.WriteFile(tmpFileName, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpFileName, fullPath)
}

func (d *DiskBlobStore) ReadBlob(ctx context.Context, blobName string) ([]byte, error) {
	return ioutil.ReadFile(filepath.Join(d.rootDir, blobName))
}

func (d *DiskBlobStore) DeleteBlob(ctx context.Context, blobName string) error {
	return os.Remove(filepath.Join(d.rootDir, blobName))
}

type GCSBlobStore struct {
	gcsClient    *storage.Client
	bucketHandle *storage.BucketHandle
	projectID    string
}

func NewGCSBlobStore(bucketName, projectID string, opts ...option.ClientOption) (*GCSBlobStore, error) {
	ctx := context.Background()
	gcsClient, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	g := &GCSBlobStore{
		gcsClient: gcsClient,
		projectID: projectID,
	}
	err = g.createBucketIfNotExists(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	return g, nil
}

func (g *GCSBlobStore) createBucketIfNotExists(ctx context.Context, bucketName string) error {
	if _, err := g.gcsClient.Bucket(bucketName).Attrs(ctx); err != nil {
		log.Printf("Creating storage bucket: %s", bucketName)
		g.bucketHandle = g.gcsClient.Bucket(bucketName)
		return g.bucketHandle.Create(ctx, g.projectID, nil)
	}
	g.bucketHandle = g.gcsClient.Bucket(bucketName)
	return nil
}

func (g *GCSBlobStore) ReadBlob(ctx context.Context, blobName string) ([]byte, error) {
	reader, err := g.bucketHandle.Object(blobName).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(reader)
}

func (g *GCSBlobStore) WriteBlob(ctx context.Context, blobName string, data []byte) error {
	writer := g.bucketHandle.Object(blobName).NewWriter(ctx)
	defer writer.Close()
	_, err := writer.Write(data)
	return err
}

func (g *GCSBlobStore) DeleteBlob(ctx context.Context, blobName string) error {
	return g.bucketHandle.Object(blobName).Delete(ctx)
}
