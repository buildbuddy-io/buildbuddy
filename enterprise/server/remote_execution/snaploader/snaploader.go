package snaploader

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
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
			log.Errorf("File %q stat err: %s", f, err)
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
	log.Printf("extractFiles called with %+v", snapOpts)
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
	log.Printf("Files are: %s", files)
	return files
}

type Loader struct {
	ctx              context.Context
	env              environment.Env
	instanceName     string
	workingDirectory string
	snapshotDigest   *repb.Digest
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
	}
	return l, nil
}

// GetConfigurationData returns the configuration data associated with a
// snapshot.
func (l *Loader) GetConfigurationData() ([]byte, error) {
	snapDir := filepath.Join(l.workingDirectory, fmt.Sprintf("disk-snap-%s", l.snapshotDigest.GetHash()))
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.Name() == ManifestFileName {
			return os.ReadFile(filepath.Join(snapDir, entry.Name()))
		}
	}
	return nil, status.FailedPreconditionErrorf("No manifest file found for snapshot: %s/%d", l.snapshotDigest.GetHash(), l.snapshotDigest.GetSizeBytes())
}

// UnpackSnapshot unpacks all of the files in a snapshot to the specified output
// directory.
func (l *Loader) UnpackSnapshot(outputDirectory string) error {
	snapDir := filepath.Join(l.workingDirectory, fmt.Sprintf("disk-snap-%s", l.snapshotDigest.GetHash()))
	files := make([]string, 0)
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		files = append(files, filepath.Join(snapDir, entry.Name()))
	}
	return hardlinkFilesIntoDirectory(outputDirectory, files...)
}

// CacheSnapshot uploads a snapshot (described by snapOpts), to the cache.
// Each file is individually stored in the CAS and a parent directory is created
// that lists all files. Finally, an action result is cached that describes all
// files -- this allows for fast existence checking and easy download.
func CacheSnapshot(ctx context.Context, env environment.Env, instanceName, workingDirectory string, snapOpts *LoadSnapshotOptions) (*repb.Digest, error) {
	ad := snapOpts.Digest()
	snapDir := filepath.Join(workingDirectory, fmt.Sprintf("disk-snap-%s", ad.GetHash()))
	os.RemoveAll(snapDir)
	if err := disk.EnsureDirectoryExists(snapDir); err != nil {
		return nil, err
	}
	log.Printf("Caching Snapshot to disk! Snapdir is: %q", snapDir)
	if err := hardlinkFilesIntoDirectory(snapDir, extractFiles(snapOpts)...); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(snapDir, ManifestFileName), snapOpts.ConfigurationData, 0644); err != nil {
		return nil, err
	}
	return ad, nil
}
