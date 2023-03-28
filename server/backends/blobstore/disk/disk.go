package disk

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/util"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
)

var (
	// Disk flags
	rootDirectory = flag.String("storage.disk.root_directory", "/tmp/buildbuddy", "The root directory to store all blobs in, if using disk based storage.")
	useV2Layout   = flag.Bool("storage.disk.use_v2_layout", false, "If enabled, files will be stored using the v2 layout. See disk_cache.MigrateToV2Layout for a description.")
)

const (
	// Prometheus BlobstoreTypeLabel values
	diskLabel = "disk"
)

// A Disk-based blob storage implementation that reads and writes blobs to/from
// files.
type DiskBlobStore struct {
	rootDir string
}

func UseDiskBlobStore() bool {
	return *rootDirectory != ""
}

func NewDiskBlobStore() (*DiskBlobStore, error) {
	if err := disk.EnsureDirectoryExists(*rootDirectory); err != nil {
		return nil, err
	}
	return &DiskBlobStore{
		rootDir: *rootDirectory,
	}, nil
}

func (d *DiskBlobStore) blobPath(blobName string) (string, error) {
	// Probably could be more careful here but we are generating these ourselves
	// for now.
	if strings.Contains(blobName, "..") {
		return "", fmt.Errorf("blobName (%s) must not contain ../", blobName)
	}
	return filepath.Join(d.rootDir, util.BlobPath(blobName)), nil
}

func (d *DiskBlobStore) WriteBlob(ctx context.Context, blobName string, data []byte) (int, error) {
	fullPath, err := d.blobPath(blobName)
	if err != nil {
		return 0, err
	}

	compressedData, err := util.Compress(data)
	if err != nil {
		return 0, err
	}
	start := time.Now()
	ctx, spn := tracing.StartSpan(ctx)
	n, err := disk.WriteFile(ctx, fullPath, compressedData)
	spn.End()
	util.RecordWriteMetrics(diskLabel, start, n, err)
	return n, err
}

func (d *DiskBlobStore) ReadBlob(ctx context.Context, blobName string) ([]byte, error) {
	fullPath, err := d.blobPath(blobName)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	ctx, spn := tracing.StartSpan(ctx)
	b, err := disk.ReadFile(ctx, fullPath)
	spn.End()
	util.RecordReadMetrics(diskLabel, start, b, err)
	return util.Decompress(b, err)
}

func (d *DiskBlobStore) DeleteBlob(ctx context.Context, blobName string) error {
	fullPath, err := d.blobPath(blobName)
	if err != nil {
		return err
	}
	start := time.Now()
	ctx, spn := tracing.StartSpan(ctx)

	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if fileInfo.IsDir() {
		err = os.RemoveAll(fullPath)
	} else {
		err = disk.DeleteFile(ctx, fullPath)
	}
	spn.End()
	util.RecordDeleteMetrics(diskLabel, start, err)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (d *DiskBlobStore) BlobExists(ctx context.Context, blobName string) (bool, error) {
	fullPath, err := d.blobPath(blobName)
	if err != nil {
		return false, err
	}
	return disk.FileExists(ctx, fullPath)
}
