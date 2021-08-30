package snaploader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/klauspost/pgzip"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// The name of a file that will be included in the snapshot and contains
	// metadata about the machine configuration that was snapshotted.
	ManifestFileName = "manifest.json"
)

type LoadSnapshotOptions struct {
	// The following fields are all required.
	ConfigurationData []byte
	MemSnapshotPath   string
	DiskSnapshotPath  string
	KernelImagePath   string
	InitrdImagePath   string
	ContainerFSPath   string

	// This field is optional -- a snapshot may have a filesystem
	// stored with it or it may have one attached at runtime.
	WorkspaceFSPath string

	// Callers may specify a snapshot ID; if set, the snapshot
	// will be stored in an action with this ID
	ForceSnapshotDigest *repb.Digest
}

// Digest computes the repb.Digest of a LoadSnapshotOptions struct.
func (o *LoadSnapshotOptions) Digest() *repb.Digest {
	if o.ForceSnapshotDigest != nil {
		return o.ForceSnapshotDigest
	}

	h := sha256.New()
	for _, f := range extractFiles(o) {
		h.Write([]byte(f))
	}
	h.Write(o.ConfigurationData)
	return &repb.Digest{
		Hash:      fmt.Sprintf("%x", h.Sum(nil)),
		SizeBytes: int64(101),
	}
}

func hardlinkFilesIntoDirectory(targetDir string, files ...string) error {
	for _, f := range files {
		fileName := filepath.Base(f)
		stat, err := os.Stat(f)
		if err != nil {
			return err
		}
		if stat.IsDir() {
			return status.FailedPreconditionErrorf("%q was dir, not file", f)
		}
		if err := os.Link(f, filepath.Join(targetDir, fileName)); err != nil {
			return err
		}
	}
	return nil
}

func extractFiles(snapOpts *LoadSnapshotOptions) []string {
	files := []string{
		snapOpts.MemSnapshotPath,
		snapOpts.DiskSnapshotPath,
		snapOpts.KernelImagePath,
		snapOpts.InitrdImagePath,
		snapOpts.ContainerFSPath,
	}
	if snapOpts.WorkspaceFSPath != "" {
		files = append(files, snapOpts.WorkspaceFSPath)
	}
	return files
}

func gzip(uncompressedFileName string) error {
	start := time.Now()
	defer func() {
		log.Debugf("gzip(%q) took %s", uncompressedFileName, time.Since(start))
	}()
	compressedFileName := uncompressedFileName + ".gz"
	if exists, err := disk.FileExists(compressedFileName); err == nil && exists {
		return status.FailedPreconditionErrorf("%q already exists.", compressedFileName)
	}
	reader, err := os.Open(uncompressedFileName)
	if err != nil {
		return err
	}
	defer reader.Close()
	compressedFile, err := os.OpenFile(compressedFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer compressedFile.Close()
	writer, err := pgzip.NewWriterLevel(compressedFile, pgzip.BestSpeed)
	if err != nil {
		return err
	}
	defer writer.Close()
	_, err = io.Copy(writer, reader)
	return err
}

func gunzip(compressedFileName string) error {
	start := time.Now()
	defer func() {
		log.Debugf("gunzip(%q) took %s", compressedFileName, time.Since(start))
	}()
	if !strings.HasSuffix(compressedFileName, ".gz") {
		return status.FailedPreconditionErrorf("%q does not end with .gz", compressedFileName)
	}
	uncompressedFileName := strings.TrimSuffix(compressedFileName, ".gz")
	if exists, err := disk.FileExists(uncompressedFileName); err == nil && exists {
		return status.FailedPreconditionErrorf("%q already exists.", uncompressedFileName)
	}
	uncompressedFile, err := os.OpenFile(uncompressedFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer uncompressedFile.Close()
	fileReader, err := os.Open(compressedFileName)
	if err != nil {
		return err
	}
	defer fileReader.Close()
	reader, err := pgzip.NewReader(fileReader)
	if err != nil {
		return err
	}
	defer reader.Close()
	_, err = io.Copy(uncompressedFile, reader)
	return err
}

type Loader struct {
	ctx              context.Context
	env              environment.Env
	instanceName     string
	workingDirectory string
	snapshotDigest   *repb.Digest

	tree *repb.Tree
}

// New returns a new snapshot.Loader that can be used to download the specified
// snapshot into the target chroot.
func New(ctx context.Context, env environment.Env, workingDirectory, instanceName string, snapshotDigest *repb.Digest) (*Loader, error) {
	l := &Loader{
		ctx:              ctx,
		env:              env,
		workingDirectory: workingDirectory,
		instanceName:     instanceName,
		snapshotDigest:   snapshotDigest,

		tree: &repb.Tree{},
	}
	if err := l.loadTree(); err != nil {
		return nil, err
	}
	return l, nil
}

// loadTree is called in New and downloads the snapshot index or returns an
// error.
func (l *Loader) loadTree() error {
	acClient := l.env.GetActionCacheClient()
	adInstanceDigest := digest.NewInstanceNameDigest(l.snapshotDigest, l.instanceName)
	actionResult, err := cachetools.GetActionResult(l.ctx, acClient, adInstanceDigest)
	if err != nil {
		return err
	}
	outs := actionResult.GetOutputDirectories()
	if len(outs) != 1 {
		return status.FailedPreconditionErrorf("No files found for snapshot: %s/%d", l.snapshotDigest.GetHash(), l.snapshotDigest.GetSizeBytes())
	}
	treeDigest := digest.NewInstanceNameDigest(outs[0].GetTreeDigest(), l.instanceName)
	return cachetools.GetBlobAsProto(l.ctx, l.env.GetByteStreamClient(), treeDigest, l.tree)
}

// GetConfigurationData returns the configuration data associated with a
// snapshot.
func (l *Loader) GetConfigurationData() ([]byte, error) {
	for _, f := range l.tree.GetRoot().GetFiles() {
		if f.GetName() == ManifestFileName {
			d := digest.NewInstanceNameDigest(f.GetDigest(), l.instanceName)
			data := new(bytes.Buffer)
			if err := cachetools.GetBlob(l.ctx, l.env.GetByteStreamClient(), d, data); err != nil {
				return nil, err
			}
			return data.Bytes(), nil
		}
	}
	return nil, status.FailedPreconditionErrorf("No manifest file found for snapshot: %s/%d", l.snapshotDigest.GetHash(), l.snapshotDigest.GetSizeBytes())
}

// UnpackSnapshot unpacks all of the files in a snapshot to the specified output
// directory.
func (l *Loader) UnpackSnapshot(outputDirectory string) error {
	if _, err := dirtools.DownloadTree(l.ctx, l.env, l.instanceName, l.tree, outputDirectory, &dirtools.DownloadTreeOpts{}); err != nil {
		return err
	}
	entries, err := os.ReadDir(outputDirectory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		f := filepath.Join(outputDirectory, entry.Name())
		if strings.HasSuffix(f, ".gz") {
			if err := gunzip(f); err != nil {
				return err
			}
			disk.DeleteLocalFileIfExists(f)
		}
	}
	return nil
}

// CacheSnapshot uploads a snapshot (described by snapOpts), to the cache.
// Each file is individually stored in the CAS and a parent directory is created
// that lists all files. Finally, an action result is cached that describes all
// files -- this allows for fast existence checking and easy download.
func CacheSnapshot(ctx context.Context, env environment.Env, instanceName, workingDirectory string, snapOpts *LoadSnapshotOptions) (*repb.Digest, error) {
	snapDir, err := os.MkdirTemp(workingDirectory, "snap-upload-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(snapDir) // clean up
	if err := hardlinkFilesIntoDirectory(snapDir, extractFiles(snapOpts)...); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(snapDir, ManifestFileName), snapOpts.ConfigurationData, 0644); err != nil {
		return nil, err
	}

	memSnapFile := filepath.Join(snapDir, "full-mem.snap")
	if err := gzip(memSnapFile); err != nil {
		return nil, err
	}
	disk.DeleteLocalFileIfExists(memSnapFile)
	if snapOpts.WorkspaceFSPath != "" {
		workspaceFile := filepath.Join(snapDir, "workspacefs.ext4")
		if err := gzip(workspaceFile); err != nil {
			return nil, err
		}
		disk.DeleteLocalFileIfExists(workspaceFile)
	}

	uploadStart := time.Now()
	_, td, err := cachetools.UploadDirectoryToCAS(ctx, env, instanceName, snapDir)
	if err != nil {
		return nil, err
	}
	log.Debugf("Uploading snapshot directory took %s", time.Since(uploadStart))
	actionResult := &repb.ActionResult{
		OutputDirectories: []*repb.OutputDirectory{{
			Path:       "snapshot",
			TreeDigest: td,
		}},
	}

	ad := snapOpts.Digest()
	adInstanceDigest := digest.NewInstanceNameDigest(ad, instanceName)
	acClient := env.GetActionCacheClient()
	if err := cachetools.UploadActionResult(ctx, acClient, adInstanceDigest, actionResult); err != nil {
		return nil, status.UnavailableErrorf("Error uploading action result: %s", err.Error())
	}
	return ad, nil
}
