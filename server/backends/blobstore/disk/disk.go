package disk

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/util"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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
	return filepath.Join(d.rootDir, blobName), nil
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

func (d *DiskBlobStore) ReadBlob(ctx context.Context, blobName string) (rb []byte, retError error) {
	fullPath, err := d.blobPath(blobName)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	_, spn := tracing.StartSpan(ctx)
	f, err := os.Open(fullPath)

	var duration time.Duration
	counter := &ioutil.Counter{}

	defer func() {
		if spn.IsRecording() {
			spn.End()
		}
		util.RecordReadMetrics(diskLabel, duration, counter.Count(), retError)
	}()

	if os.IsNotExist(err) {
		return nil, status.NotFoundError(err.Error())
	}
	if err != nil {
		return nil, err
	}
	spn.End()
	defer f.Close()
	reader := io.TeeReader(f, counter)
	duration = time.Since(start)
	return util.Decompress(reader)
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

func (d *DiskBlobStore) Writer(ctx context.Context, blobName string) (interfaces.CommittedWriteCloser, error) {
	fullPath, err := d.blobPath(blobName)
	if err != nil {
		return nil, err
	}

	fw, err := disk.FileWriter(ctx, fullPath)
	if err != nil {
		return nil, err
	}
	zw := util.NewCompressWriter(fw)
	cwc := ioutil.NewCustomCommitWriteCloser(zw)
	cwc.CommitFn = func(int64) error {
		if compresserCloseErr := zw.Close(); compresserCloseErr != nil {
			if writerCloseErr := fw.Close(); writerCloseErr != nil {
				log.Errorf("Error encountered when closing FileWriter for %s: %s", blobName, err)
			}
			return compresserCloseErr
		}
		return fw.Commit()
	}
	cwc.CloseFn = fw.Close
	return cwc, nil
}
