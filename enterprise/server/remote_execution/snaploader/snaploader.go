package snaploader

import (
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
	ManifestFileName = "manifest.json"
)

type LoadSnapshotOptions struct {
	// The following fields are all required.
	ConfigurationString string
	MemSnapshotPath     string
	DiskSnapshotPath    string
	KernelImagePath     string
	InitrdImagePath     string
	ContainerFSPath     string

	// This field is optional -- a snapshot may have a filesystem
	// stored with it or it may have one attached at runtime.
	WorkspaceFSPath string
}

func (o *LoadSnapshotOptions) Digest() *repb.Digest {
	h := sha256.New()
	for _, f := range extractFiles(o) {
		h.Write([]byte(f))
	}
	h.Write([]byte(o.ConfigurationString))
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

func PutFilesIntoDir(snapOpts *LoadSnapshotOptions, dir string) error {
	if err := hardlinkFilesIntoDirectory(dir, extractFiles(snapOpts)...); err != nil {
		return err
	}
	manifestFile := filepath.Join(dir, ManifestFileName)
	return os.WriteFile(manifestFile, []byte(snapOpts.ConfigurationString), 0644)
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
}

func New(ctx context.Context, env environment.Env, instanceName, workingDirectory string) *Loader {
	return &Loader{
		ctx:              ctx,
		env:              env,
		instanceName:     instanceName,
		workingDirectory: workingDirectory,
	}
}

func (l *Loader) UnpackSnapshot(snapshotID, outputDirectory string) error {
	acClient := l.env.GetActionCacheClient()
	ad, err := parseSnapshotID(snapshotID)
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
		return status.FailedPreconditionErrorf("No files found for snapshot: %q", snapshotID)
	}
	treeDigest := digest.NewInstanceNameDigest(outs[0].GetTreeDigest(), l.instanceName)
	tree := &repb.Tree{}
	if err := cachetools.GetBlobAsProto(l.ctx, l.env.GetByteStreamClient(), treeDigest, tree); err != nil {
		return err
	}
	if _, err := dirtools.GetTree(l.ctx, l.env, l.instanceName, tree, outputDirectory, &dirtools.GetTreeOpts{}); err != nil {
		return err
	}
	return nil
}

func (l *Loader) OptionsToCache(snapOpts *LoadSnapshotOptions) (string, error) {
	snapDir, err := os.MkdirTemp(l.workingDirectory, "snap-upload-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(snapDir) // clean up

	if err := PutFilesIntoDir(snapOpts, snapDir); err != nil {
		return "", err
	}

	_, td, err := cachetools.UploadDirectoryToCAS(l.ctx, l.env, l.instanceName, snapDir)
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
	adInstanceDigest := digest.NewInstanceNameDigest(ad, l.instanceName)
	acClient := l.env.GetActionCacheClient()
	if err := cachetools.UploadActionResult(l.ctx, acClient, adInstanceDigest, actionResult); err != nil {
		return "", status.UnavailableErrorf("Error uploading action result: %s", err.Error())
	}
	snapshotID := fmt.Sprintf("%s/%d", ad.GetHash(), ad.GetSizeBytes())
	return snapshotID, nil
}
