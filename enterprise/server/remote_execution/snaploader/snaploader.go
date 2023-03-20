package snaploader

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
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
	ConfigurationData   []byte
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
	for _, f := range enumerateFiles(o) {
		h.Write([]byte(f))
	}
	h.Write(o.ConfigurationData)
	return &repb.Digest{
		Hash:      fmt.Sprintf("%x", h.Sum(nil)),
		SizeBytes: int64(101),
	}
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
	CacheSnapshot(ctx context.Context, snapOpts *LoadSnapshotOptions) (*repb.Digest, error)
	UnpackSnapshot(ctx context.Context, snapshotDigest *repb.Digest, outputDirectory string) error
	DeleteSnapshot(ctx context.Context, snapshotDigest *repb.Digest) error
	GetConfigurationData(ctx context.Context, snapshotDigest *repb.Digest) ([]byte, error)
}

type FileCacheLoader struct {
	env              environment.Env
	workingDirectory string
	snapshotDigest   *repb.Digest
	manifest         *manifestData
}

// New returns a new snapshot.Loader that can be used to download the specified
// snapshot into the target chroot.
func New(env environment.Env, workingDirectory string) (Loader, error) {
	l := &FileCacheLoader{
		env:              env,
		workingDirectory: workingDirectory,
	}
	return l, nil
}

func (l *FileCacheLoader) unpackManifest(snapshotDigest *repb.Digest) error {
	if l.env.GetFileCache() == nil {
		return status.FailedPreconditionErrorf("Unable to load snapshot: FileCache not enabled")
	}
	l.snapshotDigest = snapshotDigest
	manifestDigest := manifestDigest(l.snapshotDigest)
	tmpDir, err := os.MkdirTemp(l.workingDirectory, "manifest-dir-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	manifestPath := filepath.Join(tmpDir, ManifestFileName)
	if !l.env.GetFileCache().FastLinkFile(fileNodeFromDigest(manifestDigest), manifestPath) {
		return status.UnavailableErrorf("snapshot manifest not found in local cache (digest: %s/%d)", l.snapshotDigest.GetHash(), l.snapshotDigest.GetSizeBytes())
	}
	buf, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}

	return json.Unmarshal(buf, &l.manifest)
}

// GetConfigurationData returns the configuration data associated with a
// snapshot.
func (l *FileCacheLoader) GetConfigurationData(ctx context.Context, snapshotDigest *repb.Digest) ([]byte, error) {
	if err := l.unpackManifest(snapshotDigest); err != nil {
		return nil, err
	}
	return l.manifest.ConfigurationData, nil
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
	for filename, dk := range l.manifest.CachedFiles {
		if !l.env.GetFileCache().FastLinkFile(fileNodeFromDigest(dk.ToDigest()), filepath.Join(outputDirectory, filename)) {
			return status.UnavailableErrorf("snapshot artifact %q not found in local cache", filename)
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
	for _, dk := range l.manifest.CachedFiles {
		l.env.GetFileCache().DeleteFile(fileNodeFromDigest(dk.ToDigest()))
	}
	return nil
}

func manifestDigest(snapshotDigest *repb.Digest) *repb.Digest {
	return &repb.Digest{
		Hash:      hash.String(snapshotDigest.GetHash() + ManifestFileName),
		SizeBytes: int64(101),
	}
}

type manifestData struct {
	ConfigurationData []byte
	CachedFiles       map[string]digest.Key
}

func (m *manifestData) String() string {
	s, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Sprintf("manifestData(!MarshalError: %q)", err)
	}
	return string(s)
}

// CacheSnapshot stores a snapshot (described by snapOpts), in the filecache.
// Each file is individually stored in the filecache under a digest made from
// the snapshot ID and the file name. A manifest file that
// that lists all files. Finally, an action result is cached that describes all
// files -- this allows for fast existence checking and easy download.
func (l *FileCacheLoader) CacheSnapshot(ctx context.Context, snapOpts *LoadSnapshotOptions) (*repb.Digest, error) {
	if l.env.GetFileCache() == nil {
		return nil, status.FailedPreconditionErrorf("Unable to cache snapshot: FileCache not enabled")
	}
	ad := snapOpts.Digest()

	tmpDir, err := os.MkdirTemp(filepath.Dir(snapOpts.MemSnapshotPath), "manifest-dir-*")
	if err != nil {
		return nil, status.InternalErrorf("failed to create manifest dir: %s", err)
	}
	defer os.RemoveAll(tmpDir)
	manifestPath := filepath.Join(tmpDir, ManifestFileName)

	manifest := &manifestData{
		ConfigurationData: snapOpts.ConfigurationData,
		CachedFiles:       make(map[string]digest.Key, 0),
	}

	// Put the files from the snapshot into the filecache and record their
	// names and digests in the manifest so they can be unpacked later.
	for _, f := range enumerateFiles(snapOpts) {
		info, err := os.Stat(f)
		if err != nil {
			return nil, err
		}
		filename := filepath.Base(f)
		fileNameDigest := &repb.Digest{
			Hash:      hash.String(ad.GetHash() + filename),
			SizeBytes: int64(info.Size()),
		}
		l.env.GetFileCache().AddFile(fileNodeFromDigest(fileNameDigest), f)
		manifest.CachedFiles[filename] = digest.NewKey(fileNameDigest)
	}

	// Write the manifest files and put it in the filecache too. We'll
	// retrieve this later in order to unpack the snapshot.
	b, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(manifestPath, b, 0644); err != nil {
		return nil, err
	}
	manifestDigest := manifestDigest(ad)
	l.env.GetFileCache().AddFile(fileNodeFromDigest(manifestDigest), manifestPath)
	return ad, nil
}

func fileNodeFromDigest(d *repb.Digest) *repb.FileNode {
	return &repb.FileNode{
		Digest:       d,
		IsExecutable: false,
	}
}
