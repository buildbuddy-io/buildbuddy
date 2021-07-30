package snaploader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

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
}

// Digest computes the repb.Digest of a LoadSnapshotOptions struct.
func (o *LoadSnapshotOptions) Digest() *repb.Digest {
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

func parseSnapshotID(snapshotID string) (*repb.Digest, error) {
	parts := strings.SplitN(snapshotID, "/", 2)
	if len(parts) != 2 || len(parts[0]) != 64 {
		return nil, status.InvalidArgumentErrorf("Error parsing snapshotID %q: should be of form 'f31e59431cdc5d631853e28151fb664f859b5f4c5dc94f0695408a6d31b84724/142'", snapshotID)
	}
	i, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("Error parsing actionID %q: %s", snapshotID, err)
	}
	return &repb.Digest{
		Hash:      parts[0],
		SizeBytes: i,
	}, nil
}

type Loader struct {
	ctx              context.Context
	env              environment.Env
	instanceName     string
	workingDirectory string
	snapshotID       string

	tree *repb.Tree
}

// New returns a new snapshot.Loader that can be used to download the specified
// snapshot into the target chroot.
func New(ctx context.Context, env environment.Env, workingDirectory, instanceName, snapshotID string) (*Loader, error) {
	l := &Loader{
		ctx:              ctx,
		env:              env,
		workingDirectory: workingDirectory,
		instanceName:     instanceName,
		snapshotID:       snapshotID,

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
	ad, err := parseSnapshotID(l.snapshotID)
	if err != nil {
		return err
	}
	adInstanceDigest := digest.NewInstanceNameDigest(ad, l.instanceName)
	actionResult, err := cachetools.GetActionResult(l.ctx, acClient, adInstanceDigest)
	if err != nil {
		return err
	}
	outs := actionResult.GetOutputDirectories()
	if len(outs) != 1 {
		return status.FailedPreconditionErrorf("No files found for snapshot: %q", l.snapshotID)
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
	return nil, status.FailedPreconditionErrorf("No manifest file found for snapshot: %s", l.snapshotID)
}

// UnpackSnapshot unpacks all of the files in a snapshot to the specified output
// directory.
func (l *Loader) UnpackSnapshot(outputDirectory string) error {
	if _, err := dirtools.GetTree(l.ctx, l.env, l.instanceName, l.tree, outputDirectory, &dirtools.GetTreeOpts{}); err != nil {
		return err
	}
	return nil
}

// CacheSnapshot uploads a snapshot (described by snapOpts), to the cache.
// Each file is individually stored in the CAS and a parent directory is created
// that lists all files. Finally, an action result is cached that describes all
// files -- this allows for fast existence checking and easy download.
func CacheSnapshot(ctx context.Context, env environment.Env, instanceName, workingDirectory string, snapOpts *LoadSnapshotOptions) (string, error) {
	snapDir, err := os.MkdirTemp(workingDirectory, "snap-upload-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(snapDir) // clean up
	if err := hardlinkFilesIntoDirectory(snapDir, extractFiles(snapOpts)...); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(snapDir, ManifestFileName), snapOpts.ConfigurationData, 0644); err != nil {
		return "", err
	}

	_, td, err := cachetools.UploadDirectoryToCAS(ctx, env, instanceName, snapDir)
	if err != nil {
		return "", err
	}
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
		return "", status.UnavailableErrorf("Error uploading action result: %s", err.Error())
	}
	snapshotID := fmt.Sprintf("%s/%d", ad.GetHash(), ad.GetSizeBytes())
	return snapshotID, nil
}
