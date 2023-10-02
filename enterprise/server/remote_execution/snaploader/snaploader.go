package snaploader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaploader_utils"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/filecacheutil"
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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

const (
	// File name used for the rootfs snapshot artifact.
	rootfsFileName = "rootfs.ext4"
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

// manifestDigest returns the digest for the snapshot manifest
//
// Because we always want runners to use the newest manifest/snapshot, this
// doesn't actually create a digest of the manifest contents. It just takes a
// hash of shared properties, for which the snapshot should be shared
func manifestDigest(ctx context.Context, env environment.Env, s *fcpb.SnapshotKey) (*repb.Digest, error) {
	gid, err := groupID(ctx, env)
	if err != nil {
		return nil, err
	}
	// Note: .manifest is not a real file that we ever create on disk, it's
	// effectively just part of the cache key used to locate the manifest.
	return &repb.Digest{
		Hash:      hashStrings(gid, s.InstanceName, s.PlatformHash, s.ConfigurationHash, s.RunnerId, ".manifest"),
		SizeBytes: 1, /*=arbitrary size*/
	}, nil
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
	key      *fcpb.SnapshotKey
	manifest *fcpb.SnapshotManifest
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
	GetSnapshot(ctx context.Context, key *fcpb.SnapshotKey) (*Snapshot, error)

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

func (l *FileCacheLoader) GetSnapshot(ctx context.Context, key *fcpb.SnapshotKey) (*Snapshot, error) {
	var manifest *fcpb.SnapshotManifest
	var err error
	if *snaploader_utils.EnableRemoteSnapshotSharing {
		manifest, err = l.fetchRemoteManifest(ctx, key)
		if err != nil {
			return nil, err
		}
	} else {
		manifest, err = l.fetchLocalManifest(ctx, key)
		if err != nil {
			return nil, err
		}
	}

	return &Snapshot{key: key, manifest: manifest}, nil
}

// fetchRemoteManifest fetches the most recent snapshot manifest from the remote
// cache.
// The ActionResult fetch will automatically validate that all referenced
// artifacts exist in the cache.
func (l *FileCacheLoader) fetchRemoteManifest(ctx context.Context, key *fcpb.SnapshotKey) (*fcpb.SnapshotManifest, error) {
	d, err := manifestDigest(ctx, l.env, key)
	if err != nil {
		return nil, err
	}
	rn := digest.NewResourceName(d, key.InstanceName, rspb.CacheType_AC, repb.DigestFunction_BLAKE3)
	acResult, err := cachetools.GetActionResult(ctx, l.env.GetActionCacheClient(), rn)
	if err != nil {
		return nil, err
	}

	return l.actionResultToManifest(acResult)
}

func (l *FileCacheLoader) fetchLocalManifest(ctx context.Context, key *fcpb.SnapshotKey) (*fcpb.SnapshotManifest, error) {
	d, err := manifestDigest(ctx, l.env, key)
	if err != nil {
		return nil, err
	}
	manifestNode := &repb.FileNode{Digest: d}
	buf, err := filecacheutil.Read(l.env.GetFileCache(), manifestNode)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to read snapshot manifest: %s", status.Message(err))
	}
	acResult := &repb.ActionResult{}
	if err := proto.Unmarshal(buf, acResult); err != nil {
		return nil, status.UnavailableErrorf("failed to unmarshal snapshot manifest: %s", status.Message(err))
	}

	manifest, err := l.actionResultToManifest(acResult)
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

func (l *FileCacheLoader) actionResultToManifest(snapshotActionResult *repb.ActionResult) (*fcpb.SnapshotManifest, error) {
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
		tree, err := l.chunkedFileTree(chunkedFileMetadata)
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

func (l *FileCacheLoader) chunkedFileTree(chunkedFileMetadata *repb.OutputDirectory) (*repb.Tree, error) {
	node := &repb.FileNode{Digest: chunkedFileMetadata.GetTreeDigest()}
	buf, err := filecacheutil.Read(l.env.GetFileCache(), node)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to read chunked file tree: %s", status.Message(err))
	}
	tree := &repb.Tree{}
	if err := proto.Unmarshal(buf, tree); err != nil {
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
		if err := snaploader_utils.FetchArtifact(ctx, l.env.GetFileCache(), l.env.GetByteStreamClient(), fileNode.GetDigest(), outputPath); err != nil {
			return nil, err
		}
	}

	unpacked := &UnpackedSnapshot{
		ChunkedFiles: make(map[string]*copy_on_write.COWStore, len(snapshot.manifest.ChunkedFiles)),
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

	// Put the files from the snapshot into the cache and record their
	// names and digests in an ActionResult so they can be unpacked later.
	for _, filePath := range enumerateFiles(opts) {
		fileName := filepath.Base(filePath)
		d, err := fileDigest(filePath)
		if err != nil {
			return err
		}
		ar.OutputFiles = append(ar.OutputFiles, &repb.OutputFile{
			Path:         fileName,
			Digest:       d,
			IsExecutable: false,
		})

		if err := snaploader_utils.Cache(ctx, l.env.GetFileCache(), l.env.GetByteStreamClient(), d, filePath); err != nil {
			return err
		}
	}
	for name, cow := range opts.ChunkedFiles {
		treeDigest, err := l.cacheCOW(ctx, name, cow)
		if err != nil {
			return status.WrapErrorf(err, "cache %q COW", name)
		}
		ar.OutputDirectories = append(ar.OutputDirectories, &repb.OutputDirectory{
			Path:       name,
			TreeDigest: treeDigest,
		})
	}

	// Write the ActionResult to the cache. We'll retrieve this later in order
	// to unpack the snapshot.
	return l.cacheActionResult(ctx, key, ar)
}

func (l *FileCacheLoader) cacheActionResult(ctx context.Context, key *fcpb.SnapshotKey, ar *repb.ActionResult) error {
	b, err := proto.Marshal(ar)
	if err != nil {
		return err
	}
	d, err := manifestDigest(ctx, l.env, key)
	if err != nil {
		return err
	}

	if *snaploader_utils.EnableRemoteSnapshotSharing {
		acDigest := digest.NewResourceName(d, key.InstanceName, rspb.CacheType_AC, repb.DigestFunction_BLAKE3)
		return cachetools.UploadActionResult(ctx, l.env.GetActionCacheClient(), acDigest, ar)
	}

	manifestNode := &repb.FileNode{Digest: d}
	_, localCacheErr := filecacheutil.Write(l.env.GetFileCache(), manifestNode, b)
	return localCacheErr
}

// TODO(Maggie): We can delete this with remote snapshot sharing because
// ActionResult validation will check this
func (l *FileCacheLoader) checkAllArtifactsExist(ctx context.Context, manifest *fcpb.SnapshotManifest) error {
	for _, f := range manifest.GetFiles() {
		if !l.env.GetFileCache().ContainsFile(f) {
			return status.NotFoundErrorf("file %q not found (digest %q)", f.GetName(), digest.String(f.GetDigest()))
		}
	}
	for _, cf := range manifest.GetChunkedFiles() {
		for _, c := range cf.GetChunks() {
			node := &repb.FileNode{
				Digest: c.GetDigest(),
			}
			if !l.env.GetFileCache().ContainsFile(node) {
				return status.NotFoundErrorf("chunked file %q missing chunk at offset 0x%x (digest %q)", cf.GetName(), c.GetOffset(), digest.String(node.Digest))
			}
		}
	}
	return nil
}

func (l *FileCacheLoader) unpackCOW(ctx context.Context, file *fcpb.ChunkedFile, outputDirectory string) (cf *copy_on_write.COWStore, err error) {
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
		c, err := copy_on_write.NewLazyMmap(ctx, l.env, dataDir, chunk.GetOffset(), chunk.GetDigest())
		if err != nil {
			return nil, status.WrapError(err, "create mmap for chunk")
		}
		chunks = append(chunks, c)
	}
	cow, err := copy_on_write.NewCOWStore(ctx, l.env, chunks, file.GetChunkSize(), file.GetSize(), dataDir)
	if err != nil {
		return nil, err
	}
	return cow, nil
}

// cacheCOW represents a COWStore as an action result tree and saves the store
// to the cache. Returns the digest of the tree
func (l *FileCacheLoader) cacheCOW(ctx context.Context, name string, cow *copy_on_write.COWStore) (*repb.Digest, error) {
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

	dirtyChunkCount := 0
	var dirtyBytes int64
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
		d, err := c.Digest()
		if err != nil {
			return nil, err
		}

		if c.Mapped() {
			path := filepath.Join(cow.DataDir(), copy_on_write.ChunkName(c.Offset, cow.Dirty(c.Offset)))
			if err := snaploader_utils.Cache(ctx, l.env.GetFileCache(), l.env.GetByteStreamClient(), d, path); err != nil {
				return nil, err
			}
		}

		tree.Root.Files = append(tree.Root.Files, &repb.FileNode{
			Name:         fmt.Sprintf("%d", c.Offset),
			Digest:       d,
			IsExecutable: false,
		})
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
	if err := snaploader_utils.CacheBytes(ctx, l.env.GetFileCache(), l.env.GetByteStreamClient(), treeDigest, treeBytes, cow.DataDir()); err != nil {
		return nil, err
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

	snap, err := l.GetSnapshot(ctx, key)
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
	cow, err := copy_on_write.ConvertFileToCOW(ctx, l.env, imageExt4Path, chunkSize, outDir)
	if err != nil {
		return nil, status.WrapError(err, "convert image to COW")
	}
	// Add the COW to cache. This will also compute chunk digests.
	opts := &CacheSnapshotOptions{
		ChunkedFiles: map[string]*copy_on_write.COWStore{rootfsFileName: cow},
	}
	if err := l.CacheSnapshot(ctx, key, opts); err != nil {
		return nil, status.WrapError(err, "cache containerfs snapshot")
	}
	log.CtxDebugf(ctx, "Converted containerfs to COW in %s", time.Since(start))
	return cow, nil
}
