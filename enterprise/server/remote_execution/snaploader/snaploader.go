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
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
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
	fileName := filepath.Base(filePath)
	gid, err := groupID(ctx, env)
	if err != nil {
		return nil, err
	}
	// Note that this only works because filecache doesn't
	// verify digests. If you want to store these remotely in
	// CAS, you need to compute the full digest.
	return &repb.FileNode{
		Digest: &repb.Digest{
			Hash:      hashStrings(gid, s.InstanceName, s.PlatformHash, s.ConfigurationHash, s.RunnerId, fileName),
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
	// memory file is not represented as a blockio.COWStore.
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

// LazyMmap is a memory-mapped file that lazily loads data from the local
// filecache or remote cache
// It also lazily computes its digest
type LazyMmap struct {
	*blockio.Mmap

	loadFn func(d *repb.Digest) (*blockio.Mmap, error)

	lazyDigest *repb.Digest
}

func (m *LazyMmap) init() error {
	if m.lazyDigest == nil {
		return status.InternalError("cannot load chunk without a digest")
	}
	mmap, err := m.loadFn(m.lazyDigest)
	if err != nil {
		// TODO(Maggie) - Fetch chunk remotely if remote snapshot sharing is on
		//if status.IsNotFoundError(err) {
		//}
		return err
	}
	m.Mmap = mmap

	return nil
}

func (m *LazyMmap) ReadAt(p []byte, off int64) (int, error) {
	if m.Mmap == nil {
		err := m.init()
		if err != nil {
			return 0, status.WrapError(err, "fetch missing chunk")
		}
	}
	return m.Mmap.ReadAt(p, off)
}

func (m *LazyMmap) WriteAt(p []byte, off int64) (int, error) {
	if m.Mmap == nil {
		err := m.init()
		if err != nil {
			return 0, status.WrapError(err, "fetch missing chunk")
		}
	}
	n, err := m.Mmap.WriteAt(p, off)
	if err != nil {
		return 0, err
	}
	m.lazyDigest = nil
	return n, nil
}

func (m *LazyMmap) Sync() error {
	if m.Mmap == nil {
		err := m.init()
		if err != nil {
			return status.WrapError(err, "fetch missing chunk")
		}
	}
	return m.Mmap.Sync()
}

func (m *LazyMmap) Close() error {
	if m.Mmap == nil {
		return nil
	}
	return m.Mmap.Close()
}

func (m *LazyMmap) SizeBytes() (int64, error) {
	if m.lazyDigest != nil {
		return m.lazyDigest.GetSizeBytes(), nil
	}

	if m.Mmap == nil {
		err := m.init()
		if err != nil {
			return 0, status.WrapError(err, "fetch missing chunk")
		}
	}

	return m.Mmap.SizeBytes()
}

func (m *LazyMmap) StartAddress() (uintptr, error) {
	if m.Mmap == nil {
		err := m.init()
		if err != nil {
			return 0, status.WrapError(err, "fetch missing chunk")
		}
	}
	return m.Mmap.StartAddress()
}

func (m *LazyMmap) Clone(copyBuf []byte, copyPath string, chunkSize int64, ioBlockSize int64) (blockio.Store, error) {
	if m.Mmap == nil {
		err := m.init()
		if err != nil {
			return nil, status.WrapError(err, "fetch missing chunk")
		}
	}
	mmapCopy, err := m.Mmap.Clone(copyBuf, copyPath, chunkSize, ioBlockSize)
	if err != nil {
		return nil, err
	}

	return &LazyMmap{
		Mmap:       mmapCopy.(*blockio.Mmap),
		loadFn:     m.loadFn,
		lazyDigest: m.lazyDigest,
	}, nil
}

func (m *LazyMmap) digest() (*repb.Digest, error) {
	if m.lazyDigest != nil {
		return m.lazyDigest, nil
	}

	r := blockio.Reader(m)
	d, err := digest.Compute(r, repb.DigestFunction_BLAKE3)
	if err != nil {
		return nil, err
	}
	m.lazyDigest = d
	return d, nil
}

func (l *FileCacheLoader) ConvertFileToLazyCOW(filePath string, fileName string, chunkSizeBytes int64, dataDir string) (store *blockio.COWStore, err error) {
	// Create a COWStore with Mmap chunks
	cow, err := blockio.ConvertFileToCOW(filePath, fileName, chunkSizeBytes, dataDir)
	if err != nil {
		return nil, err
	}
	chunks := cow.SortedChunks()

	// Convert all chunks to LazyMmaps
	lazyChunks := make([]*blockio.Chunk, 0, len(chunks))
	for _, chunk := range chunks {
		mmap, ok := chunk.Store.(*blockio.Mmap)
		if !ok {
			return nil, status.InternalErrorf("chunk must be of type Mmap to convert to type LazyMmap (chunk is of type %T)", chunk.Store)
		}

		chunkPath := filepath.Join(cow.DataDir(), blockio.ChunkName(chunk.Offset, false /*dirty*/))
		lazyChunks = append(lazyChunks, &blockio.Chunk{
			Store: &LazyMmap{
				Mmap:       mmap,
				loadFn:     l.chunkLoadFn(chunkPath),
				lazyDigest: nil,
			},
			Offset: chunk.Offset,
		})
	}

	totalSizeBytes, err := fileSize(filePath)
	if err != nil {
		return nil, err
	}
	return blockio.NewCOWStore(lazyChunks, chunkSizeBytes, totalSizeBytes, cow.DataDir(), l.initEmptyChunkFn)
}

// Initializes an empty mmap containing all 0s at `outputPath`
func (l *FileCacheLoader) initEmptyChunkFn(outputPath string, size int64) (blockio.Store, error) {
	emptyMmap, err := blockio.InitEmptyMmap(outputPath, size)
	if err != nil {
		return nil, err
	}
	return &LazyMmap{
		Mmap:       emptyMmap.(*blockio.Mmap),
		loadFn:     l.chunkLoadFn(outputPath),
		lazyDigest: nil,
	}, nil
}

func fileSize(filePath string) (int64, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	totalSizeBytes := stat.Size()
	return totalSizeBytes, nil
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

func New(env environment.Env) (*FileCacheLoader, error) {
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

	// Check whether all artifacts in the manifest are available. This helps
	// make sure that the snapshot we return can actually be loaded. This also
	// updates the last access time of all the artifacts, which helps prevent
	// the snapshot artifacts from expiring just after we've returned it.
	if err := l.checkAllArtifactsExist(ctx, manifest); err != nil {
		return nil, err
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

	unpacked := &UnpackedSnapshot{
		ChunkedFiles: make(map[string]*blockio.COWStore, len(snapshot.manifest.ChunkedFiles)),
	}
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
		l.cacheLocally(fileNode, f)
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

func (l *FileCacheLoader) checkAllArtifactsExist(ctx context.Context, manifest *fcpb.SnapshotManifest) error {
	for _, f := range manifest.GetFiles() {
		if !l.env.GetFileCache().ContainsFile(f) {
			return status.NotFoundErrorf("file %q not found (digest %q)", f.GetName(), digest.String(f.GetDigest()))
		}
	}
	for _, cf := range manifest.GetChunkedFiles() {
		for _, c := range cf.GetChunks() {
			node := &repb.FileNode{
				Digest: &repb.Digest{
					Hash:      c.GetDigestHash(),
					SizeBytes: chunkDigestSize(cf, c),
				},
			}
			if !l.env.GetFileCache().ContainsFile(node) {
				return status.NotFoundErrorf("chunked file %q missing chunk at offset 0x%x (digest %q)", cf.GetName(), c.GetOffset(), digest.String(node.Digest))
			}
		}
	}
	return nil
}

func (l *FileCacheLoader) unpackCOW(ctx context.Context, file *fcpb.ChunkedFile, outputDirectory string) (store *blockio.COWStore, err error) {
	cowPath := blockio.COWPath(outputDirectory, file.GetName())
	if err := os.Mkdir(cowPath, 0755); err != nil {
		return nil, status.InternalErrorf("failed to create COW data dir %q: %s", cowPath, err)
	}

	chunks := make([]*blockio.Chunk, 0, len(file.Chunks))
	for _, chunk := range file.Chunks {
		size := file.GetChunkSize()
		if remainder := file.GetSize() - chunk.GetOffset(); size > remainder {
			size = remainder
		}
		chunkPath := filepath.Join(cowPath, blockio.ChunkName(chunk.Offset, false))
		chunks = append(chunks, &blockio.Chunk{
			Store: &LazyMmap{
				Mmap:       nil,
				loadFn:     l.chunkLoadFn(chunkPath),
				lazyDigest: &repb.Digest{Hash: chunk.GetDigestHash(), SizeBytes: size},
			},
			Offset: chunk.Offset,
		})
	}

	cow, err := blockio.NewCOWStore(chunks, file.GetChunkSize(), file.GetSize(), cowPath, l.initEmptyChunkFn)
	if err != nil {
		return nil, err
	}
	return cow, nil
}

func (l *FileCacheLoader) cacheCOW(ctx context.Context, name string, cow *blockio.COWStore) (*fcpb.ChunkedFile, error) {
	size, err := cow.SizeBytes()
	if err != nil {
		return nil, err
	}
	pb := &fcpb.ChunkedFile{
		Name:      name,
		Size:      size,
		ChunkSize: cow.ChunkSizeBytes(),
	}

	dirtyChunkCount := 0
	var dirtyBytes int64

	// Populate manifest
	chunks := cow.SortedChunks()
	for _, c := range chunks {
		if cow.Dirty(c.Offset) {
			dirtyChunkCount++
			chunkSize, err := c.SizeBytes()
			if err != nil {
				return nil, status.WrapError(err, "dirty chunk size")
			}
			dirtyBytes += chunkSize

			// Sync dirty chunks to make sure the underlying file is up to date
			// before we add it to cache.
			if err := c.Sync(); err != nil {
				return nil, status.WrapError(err, "sync dirty chunk")
			}
		}

		mmap, ok := c.Store.(*LazyMmap)
		if !ok {
			return nil, status.InternalErrorf("chunk must be of type LazyMmap (chunk is of type %T)", c.Store)
		}

		d, err := mmap.digest()
		if err != nil {
			return nil, err
		}

		node := &repb.FileNode{Digest: d, Name: fmt.Sprintf("%s/%d", name, c.Offset)}
		chunkName := blockio.ChunkName(c.Offset, cow.Dirty(c.Offset))
		chunkPath := filepath.Join(cow.DataDir(), chunkName)
		l.cacheLocally(node, chunkPath)
		pb.Chunks = append(pb.Chunks, &fcpb.Chunk{
			Offset:     c.Offset,
			DigestHash: d.GetHash(),
		})
	}

	gid, err := groupID(ctx, l.env)
	if err != nil {
		return nil, err
	}
	metrics.COWSnapshotDirtyChunkRatio.With(prometheus.Labels{
		metrics.GroupID:  gid,
		metrics.FileName: name,
	}).Observe(float64(dirtyChunkCount) / float64(len(chunks)))
	metrics.COWSnapshotDirtyBytes.With(prometheus.Labels{
		metrics.GroupID:  gid,
		metrics.FileName: name,
	}).Add(float64(dirtyBytes))

	return pb, nil
}

// chunkLoadFn Returns a loader function that fetches a chunk
// from the local file cache, fast links it to `outputPath`, and returns a
// memory mapped struct with the data
func (l *FileCacheLoader) chunkLoadFn(outputPath string) func(d *repb.Digest) (*blockio.Mmap, error) {
	return func(d *repb.Digest) (*blockio.Mmap, error) {
		node := &repb.FileNode{Digest: d}
		inLocalCache := l.env.GetFileCache().FastLinkFile(node, outputPath)
		if !inLocalCache {
			return nil, status.NotFoundErrorf("snapshot chunk for digest %s not found in local cache", d.GetHash())
		}

		mm, err := blockio.NewMmap(outputPath)
		if err != nil {
			return nil, status.WrapError(err, "create mmap for chunk")
		}
		return mm, nil
	}
}

// cacheLocally copies the data at `path` to the local filecache with
// the given `key`
func (l *FileCacheLoader) cacheLocally(key *repb.FileNode, path string) {
	// If EnableLocalSnapshotSharing=true and we're computing real unloadedChunks,
	// the files will be immutable. We won't need to re-save them to file cache
	if !*EnableLocalSnapshotSharing || !l.env.GetFileCache().ContainsFile(key) {
		l.env.GetFileCache().AddFile(key, path)
	}
}

func chunkDigestSize(chunkedFile *fcpb.ChunkedFile, chunk *fcpb.Chunk) int64 {
	size := chunkedFile.GetChunkSize()
	if remainder := chunkedFile.GetSize() - chunk.GetOffset(); remainder < size {
		size = remainder
	}
	return size
}

func hashStrings(strs ...string) string {
	out := ""
	for _, s := range strs {
		out += hash.String(s)
	}
	return hash.String(out)
}

func groupID(ctx context.Context, env environment.Env) (string, error) {
	var gid string
	u, err := perms.AuthenticatedUser(ctx, env)
	if err == nil {
		gid = u.GetGroupID()
	} else if err != nil && !authutil.IsAnonymousUserError(err) {
		return "", err
	}
	return gid, nil
}
