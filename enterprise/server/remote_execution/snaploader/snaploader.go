package snaploader

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

const (
	// The name of a file that will be included in the snapshot and contains
	// metadata about the machine configuration that was snapshotted.
	ManifestFileName = "manifest.json"
)

// Key identifies a cached snapshot.
type Key struct {
	// ResourceName is the AC resource name identifying the snapshot metadata
	// in cache.
	ResourceName *digest.ResourceName

	// RunnerID identifies a snapshot specific to a single runner instance. If
	// set, the snapshot is considered mutable and may change while the runner
	// is executing a task. The runner whose ID matches this ID gets exclusive
	// access to the snapshot with this key.
	//
	// This is intentionally not encoded as part of ResourceName, since it is
	// only relevant to the local filecache-based snapshot store. If we share
	// snapshots using a remote cache, runner ID is not relevant since we'd
	// always download a copy of the snapshot.
	RunnerID string
}

// NewKey returns a snapshot key for the given task.
func NewKey(task *repb.ExecutionTask, runnerID string) (*Key, error) {
	action := &repb.Action{
		// Note: The version suffix here can be bumped as part of a new release
		// to effectively invalidate old snapshots, in case of an incompatible
		// VM configuration change.
		Salt: []byte("firecracker-snapshot/v0"),
		// TODO: Merge platform props from task platform overrides, both here
		// and in runner.go pool matching
		Platform: task.GetAction().GetPlatform(),
	}
	d, err := digest.ComputeForMessage(action, repb.DigestFunction_SHA256)
	if err != nil {
		return nil, err
	}
	rn := digest.NewResourceName(
		d, task.GetExecuteRequest().GetInstanceName(),
		rspb.CacheType_AC, repb.DigestFunction_SHA256)
	return &Key{ResourceName: rn, RunnerID: runnerID}, nil
}

// fileCacheKey returns the filecache key for the given snapshot resource.
func fileCacheKey(ctx context.Context, env environment.Env, k *Key, fileName string, sizeBytes int64) (*repb.FileNode, error) {
	u, err := perms.AuthenticatedUser(ctx, env)
	if err != nil {
		return nil, err
	}
	// TODO: Maybe replace filecache with a real disk_cache so that we don't
	// need to manually implement AC partitioning based on group ID/instance
	// name/etc. here.
	d := &repb.Digest{
		Hash: hash.String(
			hash.String(u.GetGroupID()) +
				hash.String(k.ResourceName.GetInstanceName()) +
				hash.String(k.ResourceName.GetDigest().GetHash()) +
				hash.String(k.RunnerID) +
				hash.String(fileName)),
		SizeBytes: sizeBytes,
	}
	return &repb.FileNode{Digest: d}, nil
}

func manifestFileCacheKey(ctx context.Context, env environment.Env, k *Key) (*repb.FileNode, error) {
	const arbitrarySizeBytes = 101
	return fileCacheKey(ctx, env, k, ManifestFileName, arbitrarySizeBytes)
}

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
	CacheSnapshot(ctx context.Context, key *Key, snapOpts *LoadSnapshotOptions) error
	UnpackSnapshot(ctx context.Context, key *Key, outputDirectory string) error
	DeleteSnapshot(ctx context.Context, key *Key) error
	GetConfigurationData(ctx context.Context, key *Key) ([]byte, error)
}

type FileCacheLoader struct {
	env              environment.Env
	workingDirectory string
	key              *Key
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

func (l *FileCacheLoader) unpackManifest(ctx context.Context, key *Key) error {
	if l.env.GetFileCache() == nil {
		return status.FailedPreconditionErrorf("Unable to load snapshot: FileCache not enabled")
	}
	manifestKey, err := manifestFileCacheKey(ctx, l.env, key)
	if err != nil {
		return err
	}

	l.key = key
	tmpDir, err := os.MkdirTemp(l.workingDirectory, "manifest-dir-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	manifestPath := filepath.Join(tmpDir, ManifestFileName)
	if !l.env.GetFileCache().FastLinkFile(manifestKey, manifestPath) {
		return status.UnavailableErrorf("snapshot manifest not found in local cache (hash: %s)", manifestKey.GetDigest().GetHash())
	}
	buf, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}

	return json.Unmarshal(buf, &l.manifest)
}

// GetConfigurationData returns the configuration data associated with a
// snapshot.
func (l *FileCacheLoader) GetConfigurationData(ctx context.Context, key *Key) ([]byte, error) {
	if err := l.unpackManifest(ctx, key); err != nil {
		return nil, err
	}
	return l.manifest.ConfigurationData, nil
}

// UnpackSnapshot unpacks all of the files in a snapshot to the specified output
// directory.
func (l *FileCacheLoader) UnpackSnapshot(ctx context.Context, key *Key, outputDirectory string) error {
	if l.key != nil && l.key != key {
		return status.InvalidArgumentError("Snapshot configuration already fetched with different key")
	}

	if l.manifest == nil {
		if err := l.unpackManifest(ctx, key); err != nil {
			return err
		}
	}
	for filename, dk := range l.manifest.CachedFiles {
		fk, err := fileCacheKey(ctx, l.env, key, filename, dk.SizeBytes)
		if err != nil {
			return err
		}
		if !l.env.GetFileCache().FastLinkFile(fk, filepath.Join(outputDirectory, filename)) {
			return status.UnavailableErrorf("snapshot artifact %q not found in local cache", filename)
		}
	}
	return nil
}

func (l *FileCacheLoader) DeleteSnapshot(ctx context.Context, key *Key) error {
	if l.key != nil && l.key != key {
		return status.InvalidArgumentError("Snapshot configuration already fetched with different key")
	}
	if l.manifest == nil {
		if err := l.unpackManifest(ctx, key); err != nil {
			return err
		}
	}
	// Manually evict the manifest as well as all referenced files.
	manifestKey, err := manifestFileCacheKey(ctx, l.env, key)
	if err != nil {
		return err
	}
	l.env.GetFileCache().DeleteFile(manifestKey)
	for filename, dk := range l.manifest.CachedFiles {
		fk, err := fileCacheKey(ctx, l.env, key, filename, dk.SizeBytes)
		if err != nil {
			return err
		}
		l.env.GetFileCache().DeleteFile(fk)
	}
	return nil
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
func (l *FileCacheLoader) CacheSnapshot(ctx context.Context, key *Key, snapOpts *LoadSnapshotOptions) error {
	// TODO(bduffany): Store an immutable copy of the snapshot (for use by new
	// runners with recycling enabled) if one doesn't exist, as well as a
	// mutable copy (for reuse only by key.RunnerID)

	if l.env.GetFileCache() == nil {
		return status.FailedPreconditionErrorf("Unable to cache snapshot: FileCache not enabled")
	}

	tmpDir, err := os.MkdirTemp(filepath.Dir(snapOpts.MemSnapshotPath), "manifest-dir-*")
	if err != nil {
		return status.InternalErrorf("failed to create manifest dir: %s", err)
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
			return err
		}
		filename := filepath.Base(f)

		fk, err := fileCacheKey(ctx, l.env, key, filename, info.Size())
		if err != nil {
			return err
		}
		l.env.GetFileCache().AddFile(fk, f)
		manifest.CachedFiles[filename] = digest.NewKey(fk.GetDigest())
	}

	// Write the manifest files and put it in the filecache too. We'll
	// retrieve this later in order to unpack the snapshot.
	b, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	if err := os.WriteFile(manifestPath, b, 0644); err != nil {
		return err
	}
	manifestKey, err := manifestFileCacheKey(ctx, l.env, key)
	if err != nil {
		return err
	}
	l.env.GetFileCache().AddFile(manifestKey, manifestPath)
	return nil
}
