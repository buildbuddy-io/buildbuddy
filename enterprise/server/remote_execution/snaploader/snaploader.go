package snaploader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaputil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

const (
	// File name used for the rootfs snapshot artifact.
	rootfsFileName = "rootfs.ext4"

	// Max number of goroutines allowed to run concurrently when uploading a
	// chunked file's contents to cache (one goroutine is spawned per chunk, and
	// this limit applies per-file).
	chunkedFileWriteConcurrency = 4
)

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

// localManifestKey returns the key for the local snapshot manifest.
//
// Because we always want runners to use the newest manifest/snapshot, this
// doesn't actually create a digest of the manifest contents. It just takes a
// hash of shared properties, for which the snapshot should be shared
func localManifestKey(ctx context.Context, env environment.Env, s *fcpb.SnapshotKey) (*repb.Digest, error) {
	// Note: filecache does not have explicit group AC partitioning unlike
	// remote cache, so we need to manually hash in the group ID as part of the
	// key.
	gid, err := groupID(ctx, env)
	if err != nil {
		return nil, err
	}
	kd, err := digest.ComputeForMessage(s, repb.DigestFunction_SHA256)
	if err != nil {
		return nil, err
	}
	// Note: .manifest is not a real file that we ever create on disk, it's
	// effectively just part of the cache key used to locate the manifest.
	return &repb.Digest{
		Hash:      hashStrings(gid, kd.GetHash(), ".manifest"),
		SizeBytes: 1, /*=arbitrary size*/
	}, nil
}

// remoteManifestKey returns the key for the remote snapshot manifest.
func remoteManifestKey(s *fcpb.SnapshotKey) (*repb.Digest, error) {
	kd, err := digest.ComputeForMessage(s, repb.DigestFunction_SHA256)
	if err != nil {
		return nil, err
	}
	// Note: .manifest is not a real file that we ever create on disk, it's
	// effectively just part of the cache key used to locate the manifest.
	return &repb.Digest{
		Hash:      hashStrings(kd.GetHash(), ".manifest"),
		SizeBytes: 1, /*=arbitrary size*/
	}, nil
}

func keyDebugString(ctx context.Context, env environment.Env, s *fcpb.SnapshotKey) string {
	d, err := remoteManifestKey(s)
	var dStr string
	if err != nil {
		dStr = fmt.Sprintf("<error: %s>", err)
	} else {
		dStr = digest.String(d)
	}
	gid, err := groupID(ctx, env)
	if err != nil {
		gid = fmt.Sprintf("<error: %s>", err)
	}
	jb, err := protojson.Marshal(s)
	if err != nil {
		jb = []byte(fmt.Sprintf("%q", err))
	}
	return fmt.Sprintf(`{"group_id": %q, "instance_name": %q, "key_digest": %q, "key": %s}`, gid, s.InstanceName, dStr, string(jb))
}

func fileDigest(filePath string) (*repb.Digest, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return digest.Compute(file, repb.DigestFunction_BLAKE3)
}

// Snapshot holds a snapshot manifest along with the corresponding cache key.
type Snapshot struct {
	key           *fcpb.SnapshotKey
	manifest      *fcpb.SnapshotManifest
	remoteEnabled bool
}

func (s *Snapshot) GetVMConfiguration() *fcpb.VMConfiguration {
	return s.manifest.GetVmConfiguration()
}

func (s *Snapshot) GetFiles() []*repb.FileNode {
	return s.manifest.GetFiles()
}

func (s *Snapshot) GetChunkedFiles() []*fcpb.ChunkedFile {
	return s.manifest.GetChunkedFiles()
}

// CacheSnapshotOptions contains any assets or configuration to be associated
// with a stored snapshot.
//
// All fields are optional, as snapshots may represent different things, such as
// an asset shared across VMs (such as the containerfs), or a fully snapshotted
// VM.
type CacheSnapshotOptions struct {
	VMConfiguration     *fcpb.VMConfiguration
	VMStateSnapshotPath string
	KernelImagePath     string
	InitrdImagePath     string
	MemSnapshotPath     string

	// TODO: remove these 3 in favor of a single rootfs.
	ContainerFSPath string
	ScratchFSPath   string
	WorkspaceFSPath string

	// Labeled map of chunked artifacts backed by copy_on_write.COWStore storage.
	ChunkedFiles map[string]*copy_on_write.COWStore

	// Whether the snapshot is from a recycled VM
	Recycled bool

	// Whether to save the snapshot to the remote cache (in addition to locally)
	Remote bool
}

type UnpackedSnapshot struct {
	// ChunkedFiles holds any chunked files that were part of the snapshot.
	ChunkedFiles map[string]*copy_on_write.COWStore
}

func enumerateFiles(snapOpts *CacheSnapshotOptions) []string {
	var out []string
	for _, p := range []string{
		snapOpts.VMStateSnapshotPath,
		snapOpts.KernelImagePath,
		snapOpts.InitrdImagePath,
		snapOpts.MemSnapshotPath,
		snapOpts.ContainerFSPath,
		snapOpts.ScratchFSPath,
		snapOpts.WorkspaceFSPath,
	} {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Loader loads and stores snapshot artifacts to cache. Only a single loader
// instance is required - the loader is stateless and loader operations can be
// used concurrently by different snapshots.
type Loader interface {
	// CacheSnapshot saves a local snapshot with the given key to cache, with the
	// snapshot configuration and artifact paths specified by opts.
	CacheSnapshot(ctx context.Context, key *fcpb.SnapshotKey, opts *CacheSnapshotOptions) error

	// GetSnapshot loads the metadata for the snapshot. It does not
	// unpack any snapshot artifacts.
	// It returns UnavailableError if the metadata has expired from cache.
	GetSnapshot(ctx context.Context, key *fcpb.SnapshotKey, remoteEnabled bool) (*Snapshot, error)

	// UnpackSnapshot unpacks a snapshot to the given directory.
	// It returns UnavailableError if any snapshot artifacts have expired
	// from cache.
	UnpackSnapshot(ctx context.Context, snapshot *Snapshot, outputDirectory string) (*UnpackedSnapshot, error)
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

func (l *FileCacheLoader) GetSnapshot(ctx context.Context, key *fcpb.SnapshotKey, remoteEnabled bool) (*Snapshot, error) {
	var manifest *fcpb.SnapshotManifest
	var err error
	if *snaputil.EnableRemoteSnapshotSharing && remoteEnabled {
		manifest, err = l.fetchRemoteManifest(ctx, key)
		if err != nil {
			log.CtxInfof(ctx, "Failed to fetch remote snapshot manifest: %s", err)
			return nil, status.WrapError(err, "fetch remote manifest")
		}
		log.CtxInfof(ctx, "Fetched remote snapshot manifest %s", keyDebugString(ctx, l.env, key))
	} else {
		manifest, err = l.getLocalManifest(ctx, key)
		if err != nil {
			return nil, status.WrapError(err, "get local manifest")
		}
	}

	return &Snapshot{key: key, manifest: manifest, remoteEnabled: remoteEnabled}, nil
}

// fetchRemoteManifest fetches the most recent snapshot manifest from the remote
// cache.
// The ActionResult fetch will automatically validate that all referenced
// artifacts exist in the cache.
func (l *FileCacheLoader) fetchRemoteManifest(ctx context.Context, key *fcpb.SnapshotKey) (*fcpb.SnapshotManifest, error) {
	d, err := remoteManifestKey(key)
	if err != nil {
		return nil, err
	}
	rn := digest.NewResourceName(d, key.InstanceName, rspb.CacheType_AC, repb.DigestFunction_BLAKE3)
	acResult, err := cachetools.GetActionResult(ctx, l.env.GetActionCacheClient(), rn)
	if err != nil {
		if status.IsNotFoundError(err) {
			metrics.RecycleRunnerRequests.With(prometheus.Labels{
				metrics.RecycleRunnerRequestStatusLabel: metrics.MissStatusLabel,
			}).Inc()
		}
		return nil, err
	}

	metrics.RecycleRunnerRequests.With(prometheus.Labels{
		metrics.RecycleRunnerRequestStatusLabel: metrics.HitStatusLabel,
	}).Inc()

	tmpDir := l.env.GetFileCache().TempDir()
	return l.actionResultToManifest(ctx, key.InstanceName, acResult, tmpDir)
}

func (l *FileCacheLoader) getLocalManifest(ctx context.Context, key *fcpb.SnapshotKey) (*fcpb.SnapshotManifest, error) {
	d, err := localManifestKey(ctx, l.env, key)
	if err != nil {
		return nil, err
	}
	manifestNode := &repb.FileNode{Digest: d}
	buf, err := l.env.GetFileCache().Read(ctx, manifestNode)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to read snapshot manifest: %s", status.Message(err))
	}
	acResult := &repb.ActionResult{}
	if err := proto.Unmarshal(buf, acResult); err != nil {
		return nil, status.UnavailableErrorf("failed to unmarshal snapshot manifest: %s", status.Message(err))
	}

	tmpDir := l.env.GetFileCache().TempDir()
	manifest, err := l.actionResultToManifest(ctx, key.InstanceName, acResult, tmpDir)
	if err != nil {
		return nil, err
	}

	// Check whether all artifacts in the manifest are available. This helps
	// make sure that the snapshot we return can actually be loaded. This also
	// updates the last access time of all the artifacts, which helps prevent
	// the snapshot artifacts from expiring just after we've returned it.
	if err := l.checkAllArtifactsExist(ctx, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

func (l *FileCacheLoader) actionResultToManifest(ctx context.Context, remoteInstanceName string, snapshotActionResult *repb.ActionResult, tmpDir string) (*fcpb.SnapshotManifest, error) {
	snapMetadata := snapshotActionResult.GetExecutionMetadata().GetAuxiliaryMetadata()
	if len(snapMetadata) != 1 {
		return nil, status.InternalErrorf("expected vm config in snapshot auxiliary metadata")
	}

	vmConfig := &fcpb.VMConfiguration{}
	if err := snapMetadata[0].UnmarshalTo(vmConfig); err != nil {
		return nil, status.WrapErrorf(err, "unmarshall vm config")
	}

	manifest := &fcpb.SnapshotManifest{
		VmConfiguration: vmConfig,
		Files:           []*repb.FileNode{},
		ChunkedFiles:    []*fcpb.ChunkedFile{},
	}

	for _, fileMetadata := range snapshotActionResult.OutputFiles {
		manifest.Files = append(manifest.Files, &repb.FileNode{
			Name:   fileMetadata.GetPath(),
			Digest: fileMetadata.GetDigest(),
		})
	}

	for _, chunkedFileMetadata := range snapshotActionResult.OutputDirectories {
		tree, err := l.chunkedFileTree(ctx, remoteInstanceName, chunkedFileMetadata, tmpDir)
		if err != nil {
			return nil, err
		}

		fileName := chunkedFileName(chunkedFileMetadata)
		fileSize, err := chunkedFileProperty(tree, "size")
		if err != nil {
			return nil, err
		}
		chunkSize, err := chunkedFileProperty(tree, "chunk_size")
		if err != nil {
			return nil, err
		}

		chunks := make([]*fcpb.Chunk, 0, len(tree.GetRoot().GetFiles()))
		for _, chunk := range tree.GetRoot().GetFiles() {
			offset, err := strconv.ParseInt(chunk.GetName(), 10, 64)
			if err != nil {
				return nil, status.InternalErrorf("parse chunk offset: %s", chunk.GetName())
			}
			chunks = append(chunks, &fcpb.Chunk{
				Offset: offset,
				Digest: chunk.GetDigest(),
			})
		}

		manifest.ChunkedFiles = append(manifest.ChunkedFiles, &fcpb.ChunkedFile{
			Name:      fileName,
			Size:      fileSize,
			ChunkSize: chunkSize,
			Chunks:    chunks,
		})
	}
	return manifest, nil
}

func chunkedFileName(chunkedFileMetadata *repb.OutputDirectory) string {
	return chunkedFileMetadata.Path
}

func chunkedFileProperty(chunkedFileTree *repb.Tree, propertyName string) (int64, error) {
	for _, prop := range chunkedFileTree.GetRoot().GetNodeProperties().GetProperties() {
		if prop.GetName() == propertyName {
			return strconv.ParseInt(prop.GetValue(), 10, 64)
		}
	}
	return 0, status.InternalErrorf("chunked file metadata missing %s property", propertyName)
}

func (l *FileCacheLoader) chunkedFileTree(ctx context.Context, remoteInstanceName string, chunkedFileMetadata *repb.OutputDirectory, tmpDir string) (*repb.Tree, error) {
	b, err := snaputil.GetBytes(ctx, l.env.GetFileCache(), l.env.GetByteStreamClient(), true /*remoteEnabled*/, chunkedFileMetadata.GetTreeDigest(), remoteInstanceName, tmpDir)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to read chunked file tree: %s", status.Message(err))
	}
	tree := &repb.Tree{}
	if err := proto.Unmarshal(b, tree); err != nil {
		return nil, status.WrapError(err, "unmarshall chunked file tree")
	}
	return tree, nil
}

func (l *FileCacheLoader) UnpackSnapshot(ctx context.Context, snapshot *Snapshot, outputDirectory string) (*UnpackedSnapshot, error) {
	if snapshot == nil {
		return nil, status.InvalidArgumentErrorf("no snapshot to unpack")
	}

	for _, fileNode := range snapshot.manifest.Files {
		outputPath := filepath.Join(outputDirectory, fileNode.GetName())
		if _, err := snaputil.GetArtifact(ctx, l.env.GetFileCache(), l.env.GetByteStreamClient(), snapshot.remoteEnabled, fileNode.GetDigest(), snapshot.key.InstanceName, outputPath); err != nil {
			return nil, err
		}
	}

	unpacked := &UnpackedSnapshot{
		ChunkedFiles: make(map[string]*copy_on_write.COWStore, len(snapshot.manifest.ChunkedFiles)),
	}
	// Construct COWs from chunks.
	for _, cf := range snapshot.manifest.ChunkedFiles {
		cow, err := l.unpackCOW(ctx, cf, snapshot.key.InstanceName, outputDirectory, snapshot.remoteEnabled)
		if err != nil {
			return nil, status.WrapError(err, "unpack COW")
		}
		unpacked.ChunkedFiles[cf.GetName()] = cow
	}

	return unpacked, nil
}

func (l *FileCacheLoader) CacheSnapshot(ctx context.Context, key *fcpb.SnapshotKey, opts *CacheSnapshotOptions) error {
	vmConfig, err := anypb.New(opts.VMConfiguration)
	if err != nil {
		return err
	}
	ar := &repb.ActionResult{
		ExecutionMetadata: &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{vmConfig},
		},
		OutputFiles:       []*repb.OutputFile{},
		OutputDirectories: []*repb.OutputDirectory{},
	}

	eg, egCtx := errgroup.WithContext(ctx)

	// Put the files from the snapshot into the cache and record their
	// names and digests in an ActionResult so they can be unpacked later.
	for _, filePath := range enumerateFiles(opts) {
		filePath := filePath
		out := &repb.OutputFile{
			Path: filepath.Base(filePath),
			// Digest is computed in goroutine.
		}
		ar.OutputFiles = append(ar.OutputFiles, out)
		eg.Go(func() error {
			ctx := egCtx
			var d *repb.Digest
			if *snaputil.EnableLocalSnapshotSharing || *snaputil.EnableRemoteSnapshotSharing {
				var err error
				d, err = fileDigest(filePath)
				if err != nil {
					return err
				}
			} else {
				// If snapshot sharing is disabled, don't compute the digest for the
				// file because it is costly. Because the runner ID is in the key
				// when snapshot sharing is disabled,  we don't need to worry about
				// multiple runners trying to access the same key simultaneously
				gid, err := groupID(ctx, l.env)
				if err != nil {
					return err
				}
				fileName := filepath.Base(filePath)
				info, err := os.Stat(filePath)
				if err != nil {
					return err
				}
				d = &repb.Digest{
					Hash:      hashStrings(gid, key.InstanceName, key.PlatformHash, key.ConfigurationHash, key.RunnerId, fileName),
					SizeBytes: info.Size(),
				}
			}
			out.Digest = d
			return snaputil.Cache(ctx, l.env.GetFileCache(), l.env.GetByteStreamClient(), opts.Remote, d, key.InstanceName, filePath)
		})
	}
	for name, cow := range opts.ChunkedFiles {
		name, cow := name, cow
		dir := &repb.OutputDirectory{
			Path: name,
			// TreeDigest is computed in goroutine.
		}
		ar.OutputDirectories = append(ar.OutputDirectories, dir)
		eg.Go(func() error {
			ctx := egCtx
			treeDigest, err := l.cacheCOW(ctx, name, key.InstanceName, cow, opts)
			if err != nil {
				return status.WrapErrorf(err, "cache %q COW", name)
			}
			dir.TreeDigest = treeDigest
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	// Write the ActionResult to the cache only after we've successfully
	// uploaded all snapshot related artifacts. We'll retrieve this later in
	// order to unpack the snapshot.
	return l.cacheActionResult(ctx, key, ar)
}

func (l *FileCacheLoader) cacheActionResult(ctx context.Context, key *fcpb.SnapshotKey, ar *repb.ActionResult) error {
	b, err := proto.Marshal(ar)
	if err != nil {
		return err
	}
	if *snaputil.EnableRemoteSnapshotSharing && !*snaputil.RemoteSnapshotReadonly {
		d, err := remoteManifestKey(key)
		if err != nil {
			return err
		}
		acDigest := digest.NewResourceName(d, key.InstanceName, rspb.CacheType_AC, repb.DigestFunction_BLAKE3)
		if err := cachetools.UploadActionResult(ctx, l.env.GetActionCacheClient(), acDigest, ar); err != nil {
			return err
		}
		log.CtxInfof(ctx, "Cached remote snapshot manifest %s", keyDebugString(ctx, l.env, key))
		return nil
	}

	d, err := localManifestKey(ctx, l.env, key)
	if err != nil {
		return err
	}
	manifestNode := &repb.FileNode{Digest: d}
	_, localCacheErr := l.env.GetFileCache().Write(ctx, manifestNode, b)
	return localCacheErr
}

// TODO(Maggie): We can delete this with remote snapshot sharing because
// ActionResult validation will check this
func (l *FileCacheLoader) checkAllArtifactsExist(ctx context.Context, manifest *fcpb.SnapshotManifest) error {
	for _, f := range manifest.GetFiles() {
		if !l.env.GetFileCache().ContainsFile(ctx, f) {
			return status.NotFoundErrorf("file %q not found (digest %q)", f.GetName(), digest.String(f.GetDigest()))
		}
	}
	for _, cf := range manifest.GetChunkedFiles() {
		for _, c := range cf.GetChunks() {
			node := &repb.FileNode{
				Digest: c.GetDigest(),
			}
			if !l.env.GetFileCache().ContainsFile(ctx, node) {
				return status.NotFoundErrorf("chunked file %q missing chunk at offset 0x%x (digest %q)", cf.GetName(), c.GetOffset(), digest.String(node.Digest))
			}
		}
	}
	return nil
}

func (l *FileCacheLoader) unpackCOW(ctx context.Context, file *fcpb.ChunkedFile, remoteInstanceName string, outputDirectory string, remoteEnabled bool) (cf *copy_on_write.COWStore, err error) {
	dataDir := filepath.Join(outputDirectory, file.GetName())
	if err := os.Mkdir(dataDir, 0755); err != nil {
		return nil, status.InternalErrorf("failed to create COW data dir %q: %s", dataDir, err)
	}
	var chunks []*copy_on_write.Mmap
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
		// TODO: Make a unit test where there is less data in a chunk than the chunk size
		// But when we fetch from the remote cache, it will need the actual
		// data size in the digest
		c, err := copy_on_write.NewLazyMmap(ctx, l.env, dataDir, chunk.GetOffset(), chunk.GetDigest(), remoteInstanceName, remoteEnabled)
		if err != nil {
			return nil, status.WrapError(err, "create mmap for chunk")
		}
		chunks = append(chunks, c)
	}
	cow, err := copy_on_write.NewCOWStore(ctx, l.env, chunks, file.GetChunkSize(), file.GetSize(), dataDir, remoteInstanceName, remoteEnabled)
	if err != nil {
		return nil, err
	}
	return cow, nil
}

// cacheCOW represents a COWStore as an action result tree and saves the store
// to the cache. Returns the digest of the tree
func (l *FileCacheLoader) cacheCOW(ctx context.Context, name string, remoteInstanceName string, cow *copy_on_write.COWStore, cacheOpts *CacheSnapshotOptions) (*repb.Digest, error) {
	var dirtyBytes, dirtyChunkCount int64
	start := time.Now()
	defer func() {
		log.CtxDebugf(ctx, "Cached %q in %s - %d MB (%d chunks) dirty", name, time.Since(start), dirtyBytes/(1024*1024), dirtyChunkCount)
	}()

	size, err := cow.SizeBytes()
	if err != nil {
		return nil, err
	}

	tree := &repb.Tree{
		Root: &repb.Directory{
			Files: []*repb.FileNode{},
			NodeProperties: &repb.NodeProperties{
				Properties: []*repb.NodeProperty{
					{
						Name:  "size",
						Value: fmt.Sprintf("%d", size),
					},
					{
						Name:  "chunk_size",
						Value: fmt.Sprintf("%d", cow.ChunkSizeBytes()),
					},
				},
			},
		},
	}

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(chunkedFileWriteConcurrency)

	chunks := cow.SortedChunks()
	var mu sync.RWMutex
	chunkSourceCounter := make(map[snaputil.ChunkSource]int, len(chunks))
	for _, c := range chunks {
		c := c
		fn := &repb.FileNode{
			Name: fmt.Sprintf("%d", c.Offset),
			// Digest is computed in goroutine.
		}
		tree.Root.Files = append(tree.Root.Files, fn)
		eg.Go(func() error {
			ctx := egCtx
			dirty := cow.Dirty(c.Offset)
			if dirty {
				chunkSize, err := c.SizeBytes()
				if err != nil {
					return status.WrapError(err, "dirty chunk size")
				}
				atomic.AddInt64(&dirtyChunkCount, 1)
				atomic.AddInt64(&dirtyBytes, chunkSize)

				// Sync dirty chunks to make sure the underlying file is up to date
				// before we add it to cache.
				if err := c.Sync(); err != nil {
					return status.WrapError(err, "sync dirty chunk")
				}
			}

			// Get or compute the digest.
			d, err := c.Digest()
			if err != nil {
				return status.WrapError(err, "compute digest")
			}
			fn.Digest = d

			chunkSrc := c.Source()
			// If the chunk was pulled from a cache and is not dirty, we don't need
			// to re-cache it.
			// If it was chunked directly from a snapshot file, it may not exist
			// in the cache yet, and we should cache it.
			shouldCache := dirty || (chunkSrc == snaputil.ChunkSourceLocalFile)
			if shouldCache {
				path := filepath.Join(cow.DataDir(), copy_on_write.ChunkName(c.Offset, cow.Dirty(c.Offset)))
				if err := snaputil.Cache(ctx, l.env.GetFileCache(), l.env.GetByteStreamClient(), cacheOpts.Remote, d, remoteInstanceName, path); err != nil {
					return status.WrapError(err, "write chunk to cache")
				}
			}
			mu.Lock()
			chunkSourceCounter[chunkSrc]++
			mu.Unlock()

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, status.WrapError(err, "cache chunks")
	}

	// Save ActionCache Tree to the cache
	treeDigest, err := digest.ComputeForMessage(tree, repb.DigestFunction_BLAKE3)
	if err != nil {
		return nil, err
	}
	treeBytes, err := proto.Marshal(tree)
	if err != nil {
		return nil, err
	}
	if err := snaputil.CacheBytes(ctx, l.env.GetFileCache(), l.env.GetByteStreamClient(), cacheOpts.Remote, treeDigest, remoteInstanceName, treeBytes); err != nil {
		return nil, err
	}

	gid, err := groupID(ctx, l.env)
	if err != nil {
		return nil, err
	}
	recycleStatus := "clean"
	if cacheOpts.Recycled {
		recycleStatus = "recycled"
	}
	metrics.COWSnapshotDirtyChunkRatio.With(prometheus.Labels{
		metrics.GroupID:              gid,
		metrics.FileName:             name,
		metrics.RecycledRunnerStatus: recycleStatus,
	}).Observe(float64(dirtyChunkCount) / float64(len(chunks)))
	metrics.COWSnapshotDirtyBytes.With(prometheus.Labels{
		metrics.GroupID:              gid,
		metrics.FileName:             name,
		metrics.RecycledRunnerStatus: recycleStatus,
	}).Add(float64(dirtyBytes))

	for chunkSrc, count := range chunkSourceCounter {
		metrics.COWSnapshotChunkSourceRatio.With(prometheus.Labels{
			metrics.GroupID:              gid,
			metrics.FileName:             name,
			metrics.RecycledRunnerStatus: recycleStatus,
			metrics.ChunkSource:          snaputil.ChunkSourceLabel(chunkSrc),
		}).Observe(float64(count / len(chunks)))
	}

	return treeDigest, nil
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

// UnpackContainerImage returns a ChunkedFile representing the given container
// image. The chunk dir is stored as a child directory of the given outDir.
//
// If the image is not cached, this func will split up the given ext4 image
// file and create a new ChunkedFile from it, then add that to cache.
func UnpackContainerImage(ctx context.Context, l *FileCacheLoader, imageRef, imageExt4Path string, outDir string, chunkSize int64) (*copy_on_write.COWStore, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	// TODO: use an Action for this key instead (to allow remote snapshot
	// sharing).
	key := &fcpb.SnapshotKey{
		ConfigurationHash: hashStrings("__UnpackContainerImage", imageRef),
	}

	snap, err := l.GetSnapshot(ctx, key, *snaputil.EnableRemoteSnapshotSharing)
	if err != nil && !(status.IsNotFoundError(err) || status.IsUnavailableError(err)) {
		return nil, err
	}
	if snap != nil {
		unpacked, err := l.UnpackSnapshot(ctx, snap, outDir)
		if err != nil {
			return nil, err
		}
		cf := unpacked.ChunkedFiles[rootfsFileName]
		if cf == nil {
			return nil, status.InternalError("missing rootfs artifact in snapshot")
		}
		return cf, nil
	}
	// containerfs is not available in cache; convert the EXT4 image to a
	// ChunkedFile then add it to cache.
	// TODO(bduffany): single-flight this.
	start := time.Now()
	cow, err := copy_on_write.ConvertFileToCOW(ctx, l.env, imageExt4Path, chunkSize, outDir, key.InstanceName, *snaputil.EnableRemoteSnapshotSharing)
	if err != nil {
		return nil, status.WrapError(err, "convert image to COW")
	}
	// Add the COW to cache. This will also compute chunk digests.
	opts := &CacheSnapshotOptions{
		ChunkedFiles: map[string]*copy_on_write.COWStore{rootfsFileName: cow},
		Recycled:     false,
	}
	if err := l.CacheSnapshot(ctx, key, opts); err != nil {
		return nil, status.WrapError(err, "cache containerfs snapshot")
	}
	log.CtxDebugf(ctx, "Converted containerfs to COW in %s", time.Since(start))
	return cow, nil
}
