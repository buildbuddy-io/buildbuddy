package snaploader

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/filecacheutil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// The name of a file that will be included in the snapshot and contains
	// metadata about the machine configuration that was snapshotted.
	ManifestFileName = "manifest.bin"
)

type SnapshotKey struct {
	ConfigurationHash string
	RunnerID          string
}

// NewKey returns the cache key for a snapshot.
// TODO(bduffany): once runners are capable of sharing snapshots, remove
// runnerID from the digest hash.
// TODO(bduffany): use ResourceName instead of digest and incorporate
// remote_instance_name. The instance name is currently implicit in the runnerID
// since we only match tasks to runners if their instance name matches the
// runner's instance name. But once we have snapshot sharing, runnerID will no
// longer be part of the snapshot key.
func NewKey(configurationHash, runnerID string) *SnapshotKey {
	return &SnapshotKey{
		ConfigurationHash: configurationHash,
		RunnerID:          runnerID,
	}
}

func (s *SnapshotKey) ManifestDigest() *repb.Digest {
	return &repb.Digest{
		Hash:      hashStrings(s.ConfigurationHash, s.RunnerID, ManifestFileName),
		SizeBytes: 1, // arbitrary
	}
}

type Snapshot struct {
	env environment.Env

	key *SnapshotKey

	manifestOnce sync.Once
	manifest     *fcpb.SnapshotManifest
	manifestErr  error
}

func (s *Snapshot) GetVMConfiguration() (*fcpb.VMConfiguration, error) {
	m, err := s.loadManifest()
	if err != nil {
		return nil, err
	}
	return m.GetVmConfiguration(), nil
}

func (s *Snapshot) loadManifest() (*fcpb.SnapshotManifest, error) {
	s.manifestOnce.Do(func() {
		manifestNode := fileNodeFromDigest(s.key.ManifestDigest())
		buf, err := filecacheutil.Read(s.env.GetFileCache(), manifestNode)
		if err != nil {
			s.manifestErr = status.UnavailableErrorf("failed to read snapshot manifest: %s", status.Message(err))
			return
		}
		manifest := &fcpb.SnapshotManifest{}
		if err := proto.Unmarshal(buf, manifest); err != nil {
			s.manifestErr = status.UnavailableErrorf("failed to unmarshal snapshot manifest: %s", status.Message(err))
			return
		}
		s.manifest = manifest
	})
	return s.manifest, s.manifestErr
}

type LoadSnapshotOptions struct {
	// The following fields are all required.
	VMConfiguration     *fcpb.VMConfiguration
	MemSnapshotPath     string
	VMStateSnapshotPath string
	KernelImagePath     string
	InitrdImagePath     string
	ContainerFSPath     string

	// This field is optional -- a snapshot may have a scratch filesystem
	// attached or it may have one attached at runtime.
	ScratchFSPath string

	// This field is optional -- a snapshot may have a filesystem
	// stored with it or it may have one attached at runtime.
	WorkspaceFSPath string
}

func enumerateFiles(snapOpts *LoadSnapshotOptions) []string {
	files := []string{
		snapOpts.MemSnapshotPath,
		snapOpts.VMStateSnapshotPath,
		snapOpts.KernelImagePath,
		snapOpts.InitrdImagePath,
		snapOpts.ContainerFSPath,
	}
	if snapOpts.ScratchFSPath != "" {
		files = append(files, snapOpts.ScratchFSPath)
	}
	if snapOpts.WorkspaceFSPath != "" {
		files = append(files, snapOpts.WorkspaceFSPath)
	}
	return files
}

type Loader interface {
	CacheSnapshot(ctx context.Context, snapshotDigest *repb.Digest, snapOpts *LoadSnapshotOptions) error
	UnpackSnapshot(ctx context.Context, snapshotDigest *repb.Digest, outputDirectory string) error
	DeleteSnapshot(ctx context.Context, snapshotDigest *repb.Digest) error
}

type FileCacheLoader struct {
	env environment.Env
}

// New returns a new snapshot.Loader that can be used to download the specified
// snapshot into the target chroot.
func New(env environment.Env) (Loader, error) {
	l := &FileCacheLoader{env: env}
	return l, nil
}

// GetConfigurationData returns the VM configuration associated with a snapshot.
func (l *FileCacheLoader) GetConfiguration(ctx context.Context, snapshotDigest *repb.Digest) (*fcpb.VMConfiguration, error) {
	if err := l.unpackManifest(snapshotDigest); err != nil {
		return nil, err
	}
	return l.manifest.VmConfiguration, nil
}

// UnpackSnapshot unpacks all of the files in a snapshot to the specified output
// directory.
func (l *FileCacheLoader) UnpackSnapshot(ctx context.Context, snapshotDigest *repb.Digest, outputDirectory string) error {
	if l.snapshotDigest != nil && l.snapshotDigest != snapshotDigest {
		return status.InvalidArgumentErrorf("Snapshot configuration already fetched with different digest %q", digest.String(l.snapshotDigest))
	}

	if l.manifest == nil {
		if err := l.unpackManifest(snapshotDigest); err != nil {
			return err
		}
	}
	for _, fileNode := range l.manifest.Files {
		if !l.env.GetFileCache().FastLinkFile(fileNode, filepath.Join(outputDirectory, fileNode.GetName())) {
			return status.UnavailableErrorf("snapshot artifact %q not found in local cache", fileNode.GetName())
		}
	}
	return nil
}

func (l *FileCacheLoader) DeleteSnapshot(ctx context.Context, snapshotDigest *repb.Digest) error {
	if l.snapshotDigest != nil && l.snapshotDigest != snapshotDigest {
		return status.InvalidArgumentErrorf("Snapshot configuration already fetched with different digest %q", digest.String(l.snapshotDigest))
	}
	if l.manifest == nil {
		if err := l.unpackManifest(snapshotDigest); err != nil {
			return err
		}
	}
	// Manually evict the manifest as well as all referenced files.
	l.env.GetFileCache().DeleteFile(fileNodeFromDigest(manifestDigest(snapshotDigest)))
	for _, fileNode := range l.manifest.Files {
		l.env.GetFileCache().DeleteFile(fileNode)
	}
	return nil
}

// CacheSnapshot stores a snapshot (described by snapOpts), in the filecache.
// Each file is individually stored in the filecache under a digest made from
// the snapshot ID and the file name. A manifest file that
// that lists all files. Finally, an action result is cached that describes all
// files -- this allows for fast existence checking and easy download.
func (l *FileCacheLoader) CacheSnapshot(ctx context.Context, snapshotDigest *repb.Digest, snapOpts *LoadSnapshotOptions) error {
	if l.env.GetFileCache() == nil {
		return status.FailedPreconditionErrorf("Unable to cache snapshot: FileCache not enabled")
	}

	manifest := &fcpb.SnapshotManifest{
		VmConfiguration: snapOpts.VMConfiguration,
	}

	// Put the files from the snapshot into the filecache and record their
	// names and digests in the manifest so they can be unpacked later.
	for _, f := range enumerateFiles(snapOpts) {
		info, err := os.Stat(f)
		if err != nil {
			return err
		}
		filename := filepath.Base(f)
		fileNameDigest := &repb.Digest{
			Hash:      hash.String(snapshotDigest.GetHash() + filename),
			SizeBytes: int64(info.Size()),
		}
		fileNode := fileNodeFromDigest(fileNameDigest)
		fileNode.Name = filename
		l.env.GetFileCache().AddFile(fileNode, f)
		manifest.Files = append(manifest.Files, fileNode)
	}

	// Write the manifest files and put it in the filecache too. We'll
	// retrieve this later in order to unpack the snapshot.
	b, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	manifestNode := fileNodeFromDigest(manifestDigest(snapshotDigest))
	if _, err := filecacheutil.Write(l.env.GetFileCache(), manifestNode, b); err != nil {
		return err
	}
	return nil
}

func fileNodeFromDigest(d *repb.Digest) *repb.FileNode {
	return &repb.FileNode{
		Digest:       d,
		IsExecutable: false,
	}
}

func hashStrings(strs ...string) string {
	out := ""
	for _, s := range strs {
		out += hash.String(s)
	}
	return hash.String(out)
}
