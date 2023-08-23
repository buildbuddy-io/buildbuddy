package snaploader

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/blockio"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/filecacheutil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/proto"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var EnableLocalSnapshotSharing = flag.Bool("executor.enable_local_snapshot_sharing", false, "Enables local snapshot sharing for firecracker VMs. Also requires that executor.firecracker_enable_nbd is true.")

// NewKey returns the cache key for a snapshot.
// TODO: include a version number in the key somehow, so that
// if we make breaking changes e.g. to the vmexec API or firecracker
// version etc., we can ensure that incompatible snapshots don't get reused.
func NewKey(task *repb.ExecutionTask, configurationHash, runnerID string) (*fcpb.SnapshotKey, error) {
	pd, err := digest.ComputeForMessage(task.GetCommand().GetPlatform(), repb.DigestFunction_SHA256)
	if err != nil {
		return nil, status.WrapErrorf(err, "failed to compute platform hash")
	}
	return &fcpb.SnapshotKey{
		InstanceName:      task.GetExecuteRequest().GetInstanceName(),
		PlatformHash:      pd.GetHash(),
		ConfigurationHash: configurationHash,
		RunnerId:          runnerID,
	}, nil
}

// manifestFileCacheKey returns the filecache key for the snapshot manifest
// file.
//
// We always want runners to use the newest manifest (and corresponding
// snapshot), so they should overwrite any existing manifest when saving
// snapshots so that newer runners will read from the newer version
func manifestFileCacheKey(ctx context.Context, env environment.Env, s *fcpb.SnapshotKey) *repb.FileNode {
	// Note: .manifest is not a real file that we ever create on disk, it's
	// effectively just part of the cache key used to locate the manifest.
	key, _ := artifactFileCacheKey(ctx, env, false, s, ".manifest", 1 /*=arbitrary size*/)
	return key
}

// artifactFileCacheKey returns the cache key for a snapshot artifact.
// It reads the artifact using fileReader in order to compute a digest
// of its contents
//
// If you don't need a real digest - for example because computing digests
// of large snapshot files is expensive -  pass in a nil fileReader.
// This will return a hash of the file name and snapshot key instead
func artifactFileCacheKey(ctx context.Context, env environment.Env, computeDigest bool, s *fcpb.SnapshotKey, filePath string, sizeBytes int64) (*repb.FileNode, error) {
	if computeDigest {
		// TODO(Maggie): Add metrics for computing snapshot digests
		file, err := os.Open(filePath)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		fileReader := bufio.NewReader(file)
		d, err := digest.Compute(fileReader, repb.DigestFunction_BLAKE3)
		if err != nil {
			return nil, err
		}
		return &repb.FileNode{
			Digest: d,
		}, nil
	}
	// Note that this only works because filecache doesn't
	// verify digests. If you want to store these remotely in
	// CAS, you need to compute the full digest.
	var groupID string
	u, err := perms.AuthenticatedUser(ctx, env)
	if err == nil {
		groupID = u.GetGroupID()
	}
	fileName := filepath.Base(filePath)
	return &repb.FileNode{
		Digest: &repb.Digest{
			Hash:      hashStrings(groupID, s.InstanceName, s.PlatformHash, s.ConfigurationHash, s.RunnerId, fileName),
			SizeBytes: sizeBytes,
		},
	}, nil
}

// Snapshot holds a snapshot manifest along with the corresponding cache key.
type Snapshot struct {
	key      *fcpb.SnapshotKey
	manifest *fcpb.SnapshotManifest
}

func (s *Snapshot) GetVMConfiguration() *fcpb.VMConfiguration {
	return s.manifest.GetVmConfiguration()
}

type CacheSnapshotOptions struct {
	// The following fields are all required.
	VMConfiguration     *fcpb.VMConfiguration
	VMStateSnapshotPath string
	KernelImagePath     string
	InitrdImagePath     string
	ContainerFSPath     string

	// MemSnapshotPath is the memory snapshot file path. It is required if the
	// memory file is not represented as a ChunkedFile.
	MemSnapshotPath string

	// This field is optional -- a snapshot may have a scratch filesystem
	// attached or it may have one attached at runtime.
	ScratchFSPath string

	// This field is optional -- a snapshot may have a filesystem
	// stored with it or it may have one attached at runtime.
	WorkspaceFSPath string

	// Labeled map of chunked artifacts backed by blockio.COWStore storage.
	ChunkedFiles map[string]*blockio.COWStore
}

type UnpackedSnapshot struct {
	// ChunkedFiles holds any chunked files that were part of the snapshot.
	ChunkedFiles map[string]*blockio.COWStore
}

func enumerateFiles(snapOpts *CacheSnapshotOptions) []string {
	files := []string{
		snapOpts.VMStateSnapshotPath,
		snapOpts.KernelImagePath,
		snapOpts.InitrdImagePath,
		snapOpts.ContainerFSPath,
	}
	if snapOpts.MemSnapshotPath != "" {
		files = append(files, snapOpts.MemSnapshotPath)
	}
	if snapOpts.ScratchFSPath != "" {
		files = append(files, snapOpts.ScratchFSPath)
	}
	if snapOpts.WorkspaceFSPath != "" {
		files = append(files, snapOpts.WorkspaceFSPath)
	}
	return files
}

// Loader loads and stores snapshot artifacts to cache. Only a single loader
// instance is required - the loader is stateless and loader operations can be
// used concurrently by different snapshots.
type Loader interface {
	// CacheSnapshot saves a local snapshot with the given key to cache, with the
	// snapshot configuration and artifact paths specified by opts.
	CacheSnapshot(ctx context.Context, key *fcpb.SnapshotKey, opts *CacheSnapshotOptions) (*Snapshot, error)

	// GetSnapshot loads the metadata for the snapshot. It does not
	// unpack any snapshot artifacts.
	// It returns UnavailableError if the metadata has expired from cache.
	GetSnapshot(ctx context.Context, key *fcpb.SnapshotKey) (*Snapshot, error)

	// UnpackSnapshot unpacks a snapshot to the given directory.
	// It returns UnavailableError if any snapshot artifacts have expired
	// from cache.
	UnpackSnapshot(ctx context.Context, snapshot *Snapshot, outputDirectory string) (*UnpackedSnapshot, error)

	// DeleteSnapshot removes the snapshot artifacts from cache
	// as well as the manifest entry.
	// This is useful to free up cache space used by stale snapshots.
	// Snapshots are quite large (tens of GB) so a single VM being
	// paused and resumed can cause significant cache churn.
	DeleteSnapshot(ctx context.Context, snapshot *Snapshot) error
}

type FileCacheLoader struct {
	env environment.Env
}

func New(env environment.Env) (Loader, error) {
	if env.GetFileCache() == nil {
		return nil, status.InvalidArgumentError("missing FileCache in env")
	}
	return &FileCacheLoader{env: env}, nil
}

func (l *FileCacheLoader) GetSnapshot(ctx context.Context, key *fcpb.SnapshotKey) (*Snapshot, error) {
	manifestNode := manifestFileCacheKey(ctx, l.env, key)
	buf, err := filecacheutil.Read(l.env.GetFileCache(), manifestNode)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to read snapshot manifest: %s", status.Message(err))
	}
	manifest := &fcpb.SnapshotManifest{}
	if err := proto.Unmarshal(buf, manifest); err != nil {
		return nil, status.UnavailableErrorf("failed to unmarshal snapshot manifest: %s", status.Message(err))
	}
	return &Snapshot{key: key, manifest: manifest}, nil
}

func (l *FileCacheLoader) UnpackSnapshot(ctx context.Context, snapshot *Snapshot, outputDirectory string) (*UnpackedSnapshot, error) {
	if snapshot == nil {
		return nil, status.InvalidArgumentErrorf("no snapshot to unpack")
	}

	for _, fileNode := range snapshot.manifest.Files {
		if !l.env.GetFileCache().FastLinkFile(fileNode, filepath.Join(outputDirectory, fileNode.GetName())) {
			return nil, status.UnavailableErrorf("snapshot artifact %q not found in local cache", fileNode.GetName())
		}
	}

	unpacked := &UnpackedSnapshot{ChunkedFiles: map[string]*blockio.COWStore{}}
	// Construct COWs from chunks.
	for _, cf := range snapshot.manifest.ChunkedFiles {
		cow, err := l.unpackCOW(ctx, cf, outputDirectory)
		if err != nil {
			return nil, status.WrapError(err, "unpack COW")
		}
		unpacked.ChunkedFiles[cf.GetName()] = cow
	}

	return unpacked, nil
}

func (l *FileCacheLoader) DeleteSnapshot(ctx context.Context, snapshot *Snapshot) error {
	// Manually evict the manifest as well as all referenced files.
	l.env.GetFileCache().DeleteFile(manifestFileCacheKey(ctx, l.env, snapshot.key))
	for _, fileNode := range snapshot.manifest.Files {
		l.env.GetFileCache().DeleteFile(fileNode)
	}
	return nil
}

func (l *FileCacheLoader) CacheSnapshot(ctx context.Context, key *fcpb.SnapshotKey, opts *CacheSnapshotOptions) (*Snapshot, error) {
	manifest := &fcpb.SnapshotManifest{
		VmConfiguration: opts.VMConfiguration,
	}
	// Put the files from the snapshot into the filecache and record their
	// names and digests in the manifest so they can be unpacked later.
	for _, f := range enumerateFiles(opts) {
		info, err := os.Stat(f)
		if err != nil {
			return nil, err
		}
		// If snapshot sharing is disabled, don't compute the digest for the
		// file because it is costly. Because the runner ID is in the key
		// when snapshot sharing is disabled,  we don't need to worry about
		// multiple runners trying to access the same key simultaneously
		fileNode, err := artifactFileCacheKey(ctx, l.env, *EnableLocalSnapshotSharing, key, f, info.Size())
		if err != nil {
			return nil, err
		}
		fileNode.Name = filepath.Base(f)
		manifest.Files = append(manifest.Files, fileNode)

		// If EnableLocalSnapshotSharing=true and we're computing real digests,
		// the files will be immutable. We won't need to re-save them to file cache
		if !*EnableLocalSnapshotSharing || !l.env.GetFileCache().ContainsFile(fileNode) {
			l.env.GetFileCache().AddFile(fileNode, f)
		}
	}
	for name, cow := range opts.ChunkedFiles {
		cf, err := l.cacheCOW(ctx, name, cow)
		if err != nil {
			return nil, status.WrapErrorf(err, "cache %q COW", name)
		}
		manifest.ChunkedFiles = append(manifest.ChunkedFiles, cf)
	}
	// Write the manifest file and put it in the filecache too. We'll
	// retrieve this later in order to unpack the snapshot.
	b, err := proto.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	manifestNode := manifestFileCacheKey(ctx, l.env, key)
	if _, err := filecacheutil.Write(l.env.GetFileCache(), manifestNode, b); err != nil {
		return nil, err
	}
	return &Snapshot{key: key, manifest: manifest}, nil
}

func (l *FileCacheLoader) unpackCOW(ctx context.Context, file *fcpb.ChunkedFile, outputDirectory string) (cow *blockio.COWStore, err error) {
	dataDir := filepath.Join(outputDirectory, file.GetName())
	if err := os.Mkdir(dataDir, 0755); err != nil {
		return nil, status.InternalErrorf("failed to create COW data dir %q: %s", dataDir, err)
	}
	var chunks []*blockio.Chunk
	defer func() {
		// If there was an error, clean up any chunks we created.
		if err == nil {
			return
		}
		for _, c := range chunks {
			c.Close()
		}
	}()
	for _, chunk := range file.Chunks {
		size := file.GetChunkSize()
		if remainder := file.GetSize() - chunk.GetOffset(); size > remainder {
			size = remainder
		}
		d := &repb.Digest{Hash: chunk.GetDigestHash(), SizeBytes: size}
		node := &repb.FileNode{Digest: d}
		path := filepath.Join(dataDir, fmt.Sprintf("%d", chunk.GetOffset()))
		if !l.env.GetFileCache().FastLinkFile(node, path) {
			return nil, status.UnavailableErrorf("snapshot chunk %s/%d not found in local cache", file.GetName(), chunk.GetOffset())
		}
		mm, err := blockio.NewLazyMmap(path)
		if err != nil {
			return nil, status.WrapError(err, "create mmap for chunk")
		}
		c := &blockio.Chunk{Offset: chunk.GetOffset(), Store: mm}
		chunks = append(chunks, c)
	}
	return blockio.NewCOWStore(chunks, file.GetChunkSize(), file.GetSize(), dataDir)
}

func (l *FileCacheLoader) cacheCOW(ctx context.Context, name string, cow *blockio.COWStore) (*fcpb.ChunkedFile, error) {
	size, err := cow.SizeBytes()
	if err != nil {
		return nil, err
	}
	cf := &fcpb.ChunkedFile{
		Name:      name,
		Size:      size,
		ChunkSize: cow.ChunkSizeBytes(),
	}
	for _, c := range cow.Chunks() {
		if cow.Dirty(c.Offset) {
			// Sync dirty chunks to make sure the underlying file is up to date
			// before we add it to cache.
			if err := c.Sync(); err != nil {
				return nil, status.WrapError(err, "sync dirty chunk")
			}
		}
		d, err := digest.Compute(blockio.Reader(c), repb.DigestFunction_BLAKE3)
		if err != nil {
			return nil, err
		}
		node := &repb.FileNode{Digest: d}
		path := filepath.Join(cow.DataDir(), cow.ChunkName(c.Offset))
		// TODO: if the file is already cached, then instead of adding the file,
		// just record a file access (to avoid the syscall overhead of
		// unlink/relink).
		l.env.GetFileCache().AddFile(node, path)
		cf.Chunks = append(cf.Chunks, &fcpb.Chunk{
			Offset:     c.Offset,
			DigestHash: d.GetHash(),
		})
	}
	return cf, nil
}

func hashStrings(strs ...string) string {
	out := ""
	for _, s := range strs {
		out += hash.String(s)
	}
	return hash.String(out)
}
