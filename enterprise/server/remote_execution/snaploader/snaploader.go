package snaploader

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaputil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// File name used for the rootfs snapshot artifact.
	rootfsFileName = "rootfs.ext4"

	// Min number of goroutines to run concurrently when uploading a
	// chunked file's contents to cache (one goroutine is spawned per chunk).
	minChunkedFileWriteConcurrency = 8
)

// SnapshotKeySet returns the cache keys for potential snapshot matches,
// as well as the key that should be written to.
func (l *FileCacheLoader) SnapshotKeySet(ctx context.Context, task *repb.ExecutionTask, configurationHash, runnerID string) (*fcpb.SnapshotKeySet, error) {
	pd, err := digest.ComputeForMessage(platform.GetProto(task.GetAction(), task.GetCommand()), repb.DigestFunction_SHA256)
	if err != nil {
		return nil, status.WrapErrorf(err, "failed to compute platform hash")
	}
	branchRef, fallbackRefs := gitRefs(task)
	branchKey := &fcpb.SnapshotKey{
		InstanceName:      task.GetExecuteRequest().GetInstanceName(),
		PlatformHash:      pd.GetHash(),
		ConfigurationHash: configurationHash,
		Ref:               branchRef,
		RunnerId:          runnerID,
	}

	snapshotVersion, err := l.currentSnapshotVersion(ctx, branchKey)
	if err != nil {
		return nil, status.WrapError(err, "get snapshot version")
	}
	branchKey.VersionId = snapshotVersion

	keys := &fcpb.SnapshotKeySet{
		BranchKey: branchKey,
	}
	for _, ref := range fallbackRefs {
		fallbackKey := keys.BranchKey.CloneVT()
		fallbackKey.Ref = ref
		keys.FallbackKeys = append(keys.FallbackKeys, fallbackKey)
	}

	// We only write a snapshot for the pushed git branch.
	// We do not update the snapshot for any fallback key(s) that we may have
	// read from, i.e. ones corresponding to the PR's base branch or the repo's
	// default branch.
	writeKey := branchKey

	// For merge queue branches, we should save the snapshot to the default branch (like `main`),
	// to support our fallback branch behavior.
	//
	// If someone is using merge queues, they're unlikely to run CI on the default
	// branch itself, so there will never be a fallback branch hit. Instead, we should
	// fallback to the latest merge queue snapshot.
	if strings.HasPrefix(branchRef, "gh-readonly-queue") {
		writeKey = branchKey.CloneVT()
		// We're expecting branches in the format `gh-readonly-queue/<default branch name>/<pr branch name>
		splitBranch := strings.Split(branchRef, "/")
		if len(splitBranch) == 3 {
			defaultBranch := splitBranch[1]
			writeKey.Ref = defaultBranch
		} else {
			log.Errorf("Unexpected merge queue branch name %s: expected 3 '/' separated parts", branchRef)
		}
	}
	keys.WriteKey = writeKey

	return keys, nil
}

// currentSnapshotVersion returns the current valid snapshot version. Snapshots on
// different versions should not be used.
func (l *FileCacheLoader) currentSnapshotVersion(ctx context.Context, key *fcpb.SnapshotKey) (string, error) {
	versionKey, err := SnapshotVersionKey(key)
	if err != nil {
		return "", err
	}
	rn := digest.NewACResourceName(versionKey, key.InstanceName, repb.DigestFunction_BLAKE3)
	// NOTE: We don't use `proxy_util.SetSkipRemote` here because the snapshot
	// version data should always live in the authoritative cache, to ensure that
	// any updates are applied universally.
	acResult, err := cachetools.GetActionResult(ctx, l.env.GetActionCacheClient(), rn)
	if status.IsNotFoundError(err) {
		// Version metadata might not exist in the cache if:
		// * The snapshot version has never been set (Ex. if you've never invalidated
		//   a snapshot with this key)
		// * The version metadata has expired from the cache
		// In the latter case, we want to be careful to not fallback to an older,
		// invalid snapshot. So here we generate a new version ID to guarantee
		// we start from a clean snapshot.
		ss := NewSnapshotService(l.env)
		return ss.InvalidateSnapshot(ctx, key)
	} else if err != nil {
		return "", err
	}

	snapMetadata := acResult.GetExecutionMetadata().GetAuxiliaryMetadata()
	if len(snapMetadata) < 1 {
		return "", status.InternalErrorf("expected version metadata in auxiliary metadata")
	}
	versionMetadata := &fcpb.SnapshotVersionMetadata{}
	if err := snapMetadata[0].UnmarshalTo(versionMetadata); err != nil {
		return "", status.WrapErrorf(err, "unmarshal version metadata")
	}
	return versionMetadata.VersionId, nil
}

func gitRefs(task *repb.ExecutionTask) (branchRef string, fallbackRefs []string) {
	if !snaputil.IsChunkedSnapshotSharingEnabled() {
		return "", nil
	}

	// NOTE: keep these names in sync with workflow service
	branchRef = getEnv(task, "GIT_BRANCH")
	if branchRef == "" {
		// Workflow actions should always have GIT_BRANCH set, so if we don't
		// see this env var then assume we're not dealing with a workflow, and
		// so we shouldn't respect fallback refs.
		return "", nil
	}
	for _, fallback := range []string{"GIT_BASE_BRANCH", "GIT_REPO_DEFAULT_BRANCH"} {
		v := getEnv(task, fallback)
		if v != "" && v != branchRef && !slices.Contains(fallbackRefs, v) {
			fallbackRefs = append(fallbackRefs, v)
		}
	}
	return branchRef, fallbackRefs
}

func getEnv(task *repb.ExecutionTask, name string) string {
	for _, e := range task.GetCommand().GetEnvironmentVariables() {
		if e.GetName() == name {
			return e.GetValue()
		}
	}
	return ""
}

// LocalManifestKey returns the key for the local snapshot manifest.
//
// If snapshotId is set, this is for a specific run of a snapshot.
// If snapshotId is not set, this is for the most recent version of the snapshot.
//
// Because we always want runners to use the newest manifest/snapshot, this
// doesn't actually create a digest of the manifest contents. It just takes a
// hash of shared properties, for which the snapshot should be shared.
//
// Note: filecache does not have explicit group AC partitioning unlike
// remote cache, so we need to manually hash in the group ID as part of the
// key.
func LocalManifestKey(gid string, s *fcpb.SnapshotKey) (*repb.Digest, error) {
	kd, err := digest.ComputeForMessage(s, repb.DigestFunction_SHA256)
	if err != nil {
		return nil, err
	}
	// Note: .manifest is not a real file that we ever create on disk, it's
	// effectively just part of the cache key used to locate the manifest.
	return &repb.Digest{
		Hash:      hashStrings(gid, kd.GetHash(), s.SnapshotId, s.VersionId, ".manifest"),
		SizeBytes: 1, /*=arbitrary size*/
	}, nil
}

// RemoteManifestKey returns the key for the remote snapshot manifest.
//
// If snapshotId is set, this is for a specific run of a snapshot.
// If snapshotId is not set, this is for the most recent version of the snapshot.
func RemoteManifestKey(s *fcpb.SnapshotKey) (*repb.Digest, error) {
	kd, err := digest.ComputeForMessage(s, repb.DigestFunction_SHA256)
	if err != nil {
		return nil, err
	}
	// Note: .manifest is not a real file that we ever create on disk, it's
	// effectively just part of the cache key used to locate the manifest.
	return &repb.Digest{
		Hash:      hashStrings(kd.GetHash(), s.SnapshotId, s.VersionId, ".manifest"),
		SizeBytes: 1, /*=arbitrary size*/
	}, nil
}

// SnapshotVersionKey returns the key to fetch snapshot version metadata.
//
// This key intentionally does not include the ref, so that all related
// branch and fallback keys are invalidated simultaneously.
func SnapshotVersionKey(s *fcpb.SnapshotKey) (*repb.Digest, error) {
	// Note: .version is not a real file that we ever create, it's
	// effectively just part of the cache key used to locate the version metadata.
	return &repb.Digest{
		Hash:      hashStrings(s.InstanceName, s.PlatformHash, s.ConfigurationHash, ".version"),
		SizeBytes: 1, /*=arbitrary size*/
	}, nil
}

func snapshotDebugString(ctx context.Context, env environment.Env, s *fcpb.SnapshotKey, remote bool, snapshotID string) string {
	gid, err := groupID(ctx, env)
	if err != nil {
		gid = fmt.Sprintf("<error: %s>", err)
	}
	var d *repb.Digest
	if remote {
		d, err = RemoteManifestKey(s)
	} else {
		d, err = LocalManifestKey(gid, s)
	}
	var dStr string
	if err != nil {
		dStr = fmt.Sprintf("<error: %s>", err)
	} else {
		dStr = digest.String(d)
	}
	jb, err := protojson.Marshal(s)
	if err != nil {
		jb = []byte(fmt.Sprintf("%q", err))
	}
	idStr := ""
	if snapshotID != "" {
		idStr = fmt.Sprintf(`"snapshot_id": %q, `, snapshotID)
	}
	return fmt.Sprintf(`{"group_id": %q, "instance_name": %q, %s"key_digest": %q, "key": %s}`, gid, s.InstanceName, idStr, dStr, string(jb))

}

func KeysetDebugString(ctx context.Context, env environment.Env, s *fcpb.SnapshotKeySet, remote bool) string {
	keySetStr := snapshotDebugString(ctx, env, s.GetBranchKey(), remote, "" /*snapshotID*/)
	for _, key := range s.FallbackKeys {
		keySetStr += fmt.Sprintf(", %s", snapshotDebugString(ctx, env, key, remote, "" /*snapshotID*/))
	}
	return keySetStr
}

func SnapshotDebugString(ctx context.Context, env environment.Env, s *Snapshot) string {
	return snapshotDebugString(ctx, env, s.GetKey(), s.remoteEnabled, s.GetVMMetadata().GetSnapshotId())
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

func (s *Snapshot) GetKey() *fcpb.SnapshotKey {
	return s.key.CloneVT()
}

func (s *Snapshot) GetVMMetadata() *fcpb.VMMetadata {
	return s.manifest.GetVmMetadata()
}

func (s *Snapshot) SetVMMetadata(md *fcpb.VMMetadata) {
	s.manifest.VmMetadata = md
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
	VMMetadata          *fcpb.VMMetadata
	VMConfiguration     *fcpb.VMConfiguration
	VMStateSnapshotPath string
	KernelImagePath     string
	InitrdImagePath     string
	MemSnapshotPath     string

	// TODO: remove these in favor of a single rootfs.
	ContainerFSPath string
	ScratchFSPath   string

	// Labeled map of chunked artifacts backed by copy_on_write.COWStore storage.
	ChunkedFiles map[string]*copy_on_write.COWStore

	// Whether the snapshot is from a recycled VM
	Recycled bool

	// Whether to save the snapshot to the remote cache (in addition to locally)
	Remote bool

	// Whether we skipped caching remotely due to newly applied remote snapshot limits.
	SkippedCacheRemotely bool
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

	// GetSnapshot loads the metadata for the snapshot. If the main key is not
	// found, it tries falling back to one of the fallback keys. It does not
	// unpack any snapshot artifacts.
	// It returns UnavailableError if the metadata has expired from cache.
	GetSnapshot(ctx context.Context, key *fcpb.SnapshotKeySet, remoteEnabled bool) (*Snapshot, error)

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

func (l *FileCacheLoader) GetSnapshot(ctx context.Context, keys *fcpb.SnapshotKeySet, remoteEnabled bool) (*Snapshot, error) {
	var lastErr error
	allKeys := append([]*fcpb.SnapshotKey{keys.GetBranchKey()}, keys.FallbackKeys...)
	for _, key := range allKeys {
		manifest, err := l.getSnapshot(ctx, key, remoteEnabled)
		if err != nil {
			lastErr = err
			continue
		}
		return &Snapshot{
			key:           key,
			manifest:      manifest,
			remoteEnabled: remoteEnabled,
		}, nil
	}
	return nil, lastErr
}

func (l *FileCacheLoader) getSnapshot(ctx context.Context, key *fcpb.SnapshotKey, remoteEnabled bool) (*fcpb.SnapshotManifest, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	// Always prioritize the local manifest if it exists. It may point to a more
	// updated snapshot than the remote manifest, which is updated less frequently.
	// Note that the master snapshot is never cached locally.
	if *snaputil.EnableLocalSnapshotSharing {
		supportsRemoteFallback := remoteEnabled && *snaputil.EnableRemoteSnapshotSharing
		manifest, err := l.getLocalManifest(ctx, key, supportsRemoteFallback)
		if err == nil || !remoteEnabled || !*snaputil.EnableRemoteSnapshotSharing {
			if err == nil {
				log.CtxInfof(ctx, "Using local manifest")
			}
			return manifest, err
		}
		log.CtxInfof(ctx, "Fetch local manifest err: %s", err)
	} else if !remoteEnabled {
		return nil, status.InternalErrorf("invalid state: EnableLocalSnapshotSharing=false and remoteEnabled=false")
	}

	// Fall back to fetching remote manifest.
	log.CtxInfof(ctx, "Fetching remote manifest")
	return l.fetchRemoteManifest(ctx, key)
}

// fetchRemoteManifest fetches the most recent snapshot manifest from the remote
// cache.
// The ActionResult fetch will automatically validate that all referenced
// artifacts exist in the cache.
func (l *FileCacheLoader) fetchRemoteManifest(ctx context.Context, key *fcpb.SnapshotKey) (*fcpb.SnapshotManifest, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	manifestKey, err := RemoteManifestKey(key)
	if err != nil {
		return nil, err
	}
	rn := digest.NewACResourceName(manifestKey, key.InstanceName, repb.DigestFunction_BLAKE3)

	// Modify the context for snapshot fetch.
	ctx = snaputil.GetSnapshotAccessContext(ctx)

	acResult, err := cachetools.GetActionResult(ctx, l.env.GetActionCacheClient(), rn)
	if err != nil {
		return nil, err
	}
	tmpDir := l.env.GetFileCache().TempDir()
	return l.actionResultToManifest(ctx, key.InstanceName, acResult, tmpDir, true /*remoteEnabled*/)
}

func (l *FileCacheLoader) GetLocalManifestACResult(ctx context.Context, manifestDigest *repb.Digest) (*repb.ActionResult, error) {
	manifestNode := &repb.FileNode{Digest: manifestDigest}
	buf, err := l.env.GetFileCache().Read(ctx, manifestNode)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to read snapshot manifest: %s", status.Message(err))
	}
	acResult := &repb.ActionResult{}
	if err := proto.Unmarshal(buf, acResult); err != nil {
		return nil, status.UnavailableErrorf("failed to unmarshal snapshot manifest: %s", status.Message(err))
	}
	return acResult, nil
}

func (l *FileCacheLoader) getLocalManifest(ctx context.Context, key *fcpb.SnapshotKey, supportsRemoteFallback bool) (*fcpb.SnapshotManifest, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	gid, err := groupID(ctx, l.env)
	if err != nil {
		return nil, err
	}
	d, err := LocalManifestKey(gid, key)
	if err != nil {
		return nil, err
	}
	acResult, err := l.GetLocalManifestACResult(ctx, d)
	if err != nil {
		return nil, status.WrapError(err, "get local snapshot manifest")
	}

	tmpDir := l.env.GetFileCache().TempDir()
	manifest, err := l.actionResultToManifest(ctx, key.InstanceName, acResult, tmpDir, false /*remoteEnabled*/)
	if err != nil {
		return nil, status.WrapError(err, "parse local snapshot manifest")
	}

	// Check whether all artifacts in the manifest are available. This helps
	// make sure that the snapshot we return can actually be loaded. This also
	// updates the last access time of all the artifacts, which helps prevent
	// the snapshot artifacts from expiring just after we've returned it.
	if err := l.checkAllArtifactsExist(ctx, manifest, key.InstanceName, supportsRemoteFallback); err != nil {
		return nil, status.WrapError(err, "check all artifacts exist for local snapshot manifest")
	}
	return manifest, nil
}

func (l *FileCacheLoader) actionResultToManifest(ctx context.Context, remoteInstanceName string, snapshotActionResult *repb.ActionResult, tmpDir string, remoteEnabled bool) (*fcpb.SnapshotManifest, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	snapMetadata := snapshotActionResult.GetExecutionMetadata().GetAuxiliaryMetadata()
	if len(snapMetadata) < 1 {
		return nil, status.InternalErrorf("expected vm config in snapshot auxiliary metadata")
	}
	vmConfig := &fcpb.VMConfiguration{}
	if err := snapMetadata[0].UnmarshalTo(vmConfig); err != nil {
		return nil, status.WrapErrorf(err, "unmarshall vm config")
	}

	var vmMetadata *fcpb.VMMetadata
	if len(snapMetadata) == 2 {
		vmMetadata = &fcpb.VMMetadata{}
		if err := snapMetadata[1].UnmarshalTo(vmMetadata); err != nil {
			return nil, status.WrapErrorf(err, "unmarshall vm metadata")
		}
	}

	manifest := &fcpb.SnapshotManifest{
		VmMetadata:      vmMetadata,
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
		tree, err := l.ChunkedFileTree(ctx, remoteInstanceName, chunkedFileMetadata, tmpDir, remoteEnabled)
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

func (l *FileCacheLoader) ChunkedFileTree(ctx context.Context, remoteInstanceName string, chunkedFileMetadata *repb.OutputDirectory, tmpDir string, remoteEnabled bool) (*repb.Tree, error) {
	b, err := snaputil.GetBytes(ctx, l.env.GetFileCache(), l.env.GetByteStreamClient(), remoteEnabled, chunkedFileMetadata.GetTreeDigest(), remoteInstanceName, tmpDir)
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
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	if key == nil {
		return status.InvalidArgumentErrorf("key required to cache snapshot")
	}

	vmConfig, err := anypb.New(opts.VMConfiguration)
	if err != nil {
		return err
	}
	vmMetadata, err := anypb.New(opts.VMMetadata)
	if err != nil {
		return err
	}
	ar := &repb.ActionResult{
		ExecutionMetadata: &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{vmConfig, vmMetadata},
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
			// This should be the name of the file - i.e. "initrd.cpio", "vmstate.snap", "vmlinux"
			fileType := filepath.Base(filePath)
			var d *repb.Digest
			if snaputil.IsChunkedSnapshotSharingEnabled() {
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
				info, err := os.Stat(filePath)
				if err != nil {
					return err
				}
				d = &repb.Digest{
					Hash:      hashStrings(gid, key.InstanceName, key.PlatformHash, key.ConfigurationHash, key.RunnerId, fileType),
					SizeBytes: info.Size(),
				}
			}
			out.Digest = d
			if _, err := snaputil.Cache(ctx, l.env.GetFileCache(), l.env.GetByteStreamClient(), opts.Remote, d, key.InstanceName, filePath, fileType); err != nil {
				return err
			}
			return nil
		})
	}
	for name, cow := range opts.ChunkedFiles {
		treeDigest, err := l.cacheCOW(egCtx, name, key.InstanceName, cow, opts)
		if err != nil {
			return status.WrapErrorf(err, "cache %q COW", name)
		}
		dir := &repb.OutputDirectory{
			Path:       name,
			TreeDigest: treeDigest,
		}
		ar.OutputDirectories = append(ar.OutputDirectories, dir)
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	// Write the ActionResult to the cache only after we've successfully
	// uploaded all snapshot related artifacts. We'll retrieve this later in
	// order to unpack the snapshot.
	return l.cacheActionResult(ctx, key, ar, opts)
}

func (l *FileCacheLoader) cacheActionResult(ctx context.Context, key *fcpb.SnapshotKey, ar *repb.ActionResult, opts *CacheSnapshotOptions) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	ctx = snaputil.GetSnapshotAccessContext(ctx)
	b, err := proto.Marshal(ar)
	if err != nil {
		return err
	}
	if *snaputil.EnableRemoteSnapshotSharing && !*snaputil.RemoteSnapshotReadonly && opts.Remote {
		// Cache master snapshot manifest
		d, err := RemoteManifestKey(key)
		if err != nil {
			return err
		}
		acDigest := digest.NewACResourceName(d, key.InstanceName, repb.DigestFunction_BLAKE3)
		if err := cachetools.UploadActionResult(ctx, l.env.GetActionCacheClient(), acDigest, ar); err != nil {
			return err
		}
		log.CtxInfof(ctx, "Updated remote master snapshot manifest %s", snapshotDebugString(ctx, l.env, key, true /*remote*/, "" /*=snapshotID*/))

		// Cache snapshot manifest for this specific snapshot ID
		if opts.VMMetadata.GetSnapshotId() != "" {
			snapshotID := opts.VMMetadata.SnapshotId
			snapshotSpecificKey := &fcpb.SnapshotKey{
				InstanceName: key.InstanceName,
				SnapshotId:   snapshotID,
			}
			snapshotSpecificManifestKey, err := RemoteManifestKey(snapshotSpecificKey)
			if err != nil {
				log.CtxWarningf(ctx, "Failed to generate snapshot specific remote manifest key for snapshot ID %s: %s", snapshotID, err)
				return nil
			}
			snapshotSpecificAcDigest := digest.NewACResourceName(snapshotSpecificManifestKey, key.InstanceName, repb.DigestFunction_BLAKE3)
			if err := cachetools.UploadActionResult(ctx, l.env.GetActionCacheClient(), snapshotSpecificAcDigest, ar); err != nil {
				log.CtxWarningf(ctx, "Failed to cache remote snapshot specific manifest for snapshot ID %s: %s", snapshotID, err)
				return nil
			}
			log.CtxInfof(ctx, "Cached remote snapshot manifest %s", snapshotDebugString(ctx, l.env, snapshotSpecificKey, true /*remote*/, snapshotID))
		}

		return nil
	}

	gid, err := groupID(ctx, l.env)
	if err != nil {
		return err
	}
	d, err := LocalManifestKey(gid, key)
	if err != nil {
		return err
	}
	manifestNode := &repb.FileNode{Digest: d}
	if _, err := l.env.GetFileCache().Write(ctx, manifestNode, b); err != nil {
		return err
	}
	log.CtxInfof(ctx, "Updated local master snapshot manifest %s", snapshotDebugString(ctx, l.env, key, false /*remote*/, "" /*=snapshotID*/))

	// Cache snapshot manifest for this specific snapshot ID
	if opts.VMMetadata.GetSnapshotId() != "" {
		snapshotID := opts.VMMetadata.GetSnapshotId()
		snapshotSpecificKey := &fcpb.SnapshotKey{
			InstanceName: key.InstanceName,
			SnapshotId:   snapshotID,
		}

		snapshotSpecificManifestKey, err := LocalManifestKey(gid, snapshotSpecificKey)
		if err != nil {
			log.Warningf("Failed to generate snapshot specific local manifest key for snapshot ID %s: %s", snapshotID, err)
			return nil
		}

		snapshotSpecificManifestNode := &repb.FileNode{Digest: snapshotSpecificManifestKey}
		if _, err := l.env.GetFileCache().Write(ctx, snapshotSpecificManifestNode, b); err != nil {
			log.Warningf("Failed to cache local snapshot specific manifest for snapshot ID %s: %s", snapshotID, err)
			return nil
		}

		log.CtxInfof(ctx, "Cached local snapshot manifest %s", snapshotDebugString(ctx, l.env, snapshotSpecificKey, false /*remote*/, snapshotID))
	}

	return nil
}

func (l *FileCacheLoader) checkAllArtifactsExist(ctx context.Context, manifest *fcpb.SnapshotManifest, instanceName string, supportsRemoteFallback bool) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	missingDigests := make([]*repb.Digest, 0)
	for _, f := range manifest.GetFiles() {
		if !l.env.GetFileCache().ContainsFile(ctx, f) {
			if supportsRemoteFallback {
				missingDigests = append(missingDigests, f.GetDigest())
			} else {
				return status.NotFoundErrorf("file %q not found (digest %q)", f.GetName(), digest.String(f.GetDigest()))
			}
		}
	}
	for _, cf := range manifest.GetChunkedFiles() {
		for _, c := range cf.GetChunks() {
			node := &repb.FileNode{
				Digest: c.GetDigest(),
			}
			if !l.env.GetFileCache().ContainsFile(ctx, node) {
				if supportsRemoteFallback {
					missingDigests = append(missingDigests, c.GetDigest())
				} else {
					return status.NotFoundErrorf("chunked file %q missing chunk at offset 0x%x (digest %q)", cf.GetName(), c.GetOffset(), digest.String(node.Digest))
				}
			}
		}
	}

	// If `supportsRemoteFallback` is enabled, allow using a local manifest even
	// if all snapshot chunks don't exist locally. The snaploader can fallback
	// to fetching chunks from the remote cache.
	if supportsRemoteFallback && len(missingDigests) > 0 {
		rsp, err := l.env.GetContentAddressableStorageClient().FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
			InstanceName:   instanceName,
			BlobDigests:    missingDigests,
			DigestFunction: repb.DigestFunction_BLAKE3,
		})
		if err != nil {
			return status.WrapError(err, "querying remote cache to check for snapshot artifacts")
		} else if len(rsp.MissingBlobDigests) > 0 {
			return status.NotFoundErrorf("digests not found when querying remote cache to check for snapshot artifacts")
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
	cow, err := copy_on_write.NewCOWStore(ctx, l.env, file.GetName(), chunks, file.GetChunkSize(), file.GetSize(), dataDir, remoteInstanceName, remoteEnabled)
	if err != nil {
		return nil, err
	}
	return cow, nil
}

// cacheCOW represents a COWStore as an action result tree and saves the store
// to the cache. Returns the digest of the tree
func (l *FileCacheLoader) cacheCOW(ctx context.Context, name string, remoteInstanceName string, cow *copy_on_write.COWStore, cacheOpts *CacheSnapshotOptions) (*repb.Digest, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	var dirtyBytes, dirtyChunkCount, emptyBytes, emptyChunkCount, compressedBytesWrittenRemotely int64
	start := time.Now()
	defer func() {
		log.CtxDebugf(ctx, "Cached %q in %s - %d MB (%d chunks) dirty, %d MB (%d chunks) empty, %d MB compressed data written to the remote cache", name, time.Since(start), dirtyBytes/(1024*1024), dirtyChunkCount, emptyBytes/(1024*1024), emptyChunkCount, compressedBytesWrittenRemotely/(1024*1024))
	}()
	size, err := cow.SizeBytes()
	if err != nil {
		return nil, err
	}
	if span.IsRecording() {
		defer func() {
			span.SetAttributes(
				attribute.String("cow_name", name),
				attribute.Int64("cow_size_bytes", size), // This includes non-dirty and all-zero chunks
				attribute.Int64("dirty_bytes", dirtyBytes),
				attribute.Int64("dirty_chunks", dirtyChunkCount),
				attribute.Int64("empty_bytes", emptyBytes),
				attribute.Int64("empty_chunks", emptyChunkCount),
				attribute.Int64("compressed_bytes_written_remotely", compressedBytesWrittenRemotely),
			)
		}()
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

	emptyData := make([]byte, cow.ChunkSizeBytes())
	allZerosDigest, err := digest.Compute(bytes.NewReader(emptyData), repb.DigestFunction_BLAKE3)
	if err != nil {
		return nil, err
	}

	eg, egCtx := errgroup.WithContext(ctx)
	earlyExitCtx, cancelForEarlyExit := context.WithCancel(egCtx)
	defer cancelForEarlyExit()

	writeConcurrency := int(math.Max(minChunkedFileWriteConcurrency, float64(cacheOpts.VMConfiguration.GetNumCpus())))
	eg.SetLimit(writeConcurrency)

	chunks := cow.SortedChunks()
	var mu sync.RWMutex
	chunkSourceCounter := make(map[snaputil.ChunkSource]int, len(chunks))
	chunkNodes := make([]*repb.FileNode, 0, len(chunks))
	for i, c := range chunks {
		if earlyExitCtx.Err() != nil {
			break
		}

		i := i
		c := c

		// Make sure chunks are appended in order
		fn := &repb.FileNode{
			Name: fmt.Sprintf("%d", c.Offset),
			// Digest is computed in goroutine.
		}
		chunkNodes = append(chunkNodes, fn)

		eg.Go(func() error {
			returnError := func(err error) error {
				cancelForEarlyExit()
				return status.WrapError(err, fmt.Sprintf("cache chunk %d/%d", i, len(chunks)))
			}

			ctx := earlyExitCtx

			chunkSrc := c.Source()
			mu.Lock()
			chunkSourceCounter[chunkSrc]++
			mu.Unlock()

			// Get or compute the digest.
			d, err := c.Digest()
			if err != nil {
				return returnError(status.WrapError(err, "compute digest"))
			}
			fn.Digest = d

			chunkSize, err := c.SizeBytes()
			if err != nil {
				return returnError(status.WrapError(err, "chunk size"))
			}

			// Skip caching chunks of all 0s
			if d.GetHash() == allZerosDigest.GetHash() {
				atomic.AddInt64(&emptyChunkCount, 1)
				atomic.AddInt64(&emptyBytes, chunkSize)
			} else {
				dirty := cow.Dirty(c.Offset)
				if dirty {
					atomic.AddInt64(&dirtyChunkCount, 1)
					atomic.AddInt64(&dirtyBytes, chunkSize)

					// Sync dirty chunks to make sure the underlying file is up to date
					// before we add it to cache.
					if err := c.Sync(); err != nil {
						return returnError(status.WrapError(err, "sync dirty chunk"))
					}
				}

				// If the chunk was pulled from a cache and is not dirty, we don't need
				// to re-cache it.
				// If it was chunked directly from a snapshot file, it may not exist
				// in the cache yet, and we should cache it.
				shouldCache := dirty || (chunkSrc == snaputil.ChunkSourceLocalFile)
				if shouldCache {
					path := filepath.Join(cow.DataDir(), copy_on_write.ChunkName(c.Offset, cow.Dirty(c.Offset)))
					bytesWritten, err := snaputil.Cache(ctx, l.env.GetFileCache(), l.env.GetByteStreamClient(), cacheOpts.Remote, d, remoteInstanceName, path, name)
					if err != nil {
						return returnError(status.WrapError(err, "write chunk to cache"))
					}
					atomic.AddInt64(&compressedBytesWrittenRemotely, bytesWritten)
				} else if *snaputil.VerboseLogging {
					log.CtxDebugf(ctx, "Not caching snapshot artifact: dirty=%t src=%s file=%s hash=%s", dirty, chunkSrc, snaputil.StripChroot(cow.DataDir()), d.GetHash())
				}

			}

			// After processing each chunk, we won't still need
			// the data in memory, so unmap it to reduce memory usage on the executor
			if err := c.Unmap(); err != nil {
				return returnError(status.WrapError(err, "unmap chunk"))
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// Filter out empty chunks
	filteredChunkNodes := make([]*repb.FileNode, 0)
	for _, c := range chunkNodes {
		if c.Digest.GetHash() != allZerosDigest.GetHash() {
			filteredChunkNodes = append(filteredChunkNodes, c)
		}
	}
	tree.Root.Files = filteredChunkNodes

	// Save ActionCache Tree to the cache
	treeDigest, err := digest.ComputeForMessage(tree, repb.DigestFunction_BLAKE3)
	if err != nil {
		return nil, err
	}
	treeBytes, err := proto.Marshal(tree)
	if err != nil {
		return nil, err
	}
	if err := snaputil.CacheBytes(ctx, l.env.GetFileCache(), l.env.GetByteStreamClient(), cacheOpts.Remote, treeDigest, remoteInstanceName, treeBytes, "snapshot_tree"); err != nil {
		return nil, err
	}

	metrics.COWSnapshotDirtyChunkRatio.With(prometheus.Labels{
		metrics.FileName: name,
	}).Observe(float64(dirtyChunkCount) / float64(len(chunks)))
	metrics.COWSnapshotDirtyBytes.With(prometheus.Labels{
		metrics.FileName: name,
	}).Add(float64(dirtyBytes))
	metrics.COWSnapshotEmptyChunkRatio.With(prometheus.Labels{
		metrics.FileName: name,
	}).Observe(float64(emptyChunkCount) / float64(len(chunks)))

	if cacheOpts.SkippedCacheRemotely {
		metrics.COWSnapshotSkippedRemoteBytes.With(prometheus.Labels{
			metrics.FileName: name,
		}).Add(float64(dirtyBytes))
	}

	for chunkSrc, count := range chunkSourceCounter {
		metrics.COWSnapshotChunkSourceRatio.With(prometheus.Labels{
			metrics.FileName:    name,
			metrics.ChunkSource: snaputil.ChunkSourceLabel(chunkSrc),
		}).Observe(float64(count) / float64(len(chunks)))
	}

	return treeDigest, nil
}

type SnapshotService struct {
	env environment.Env
}

func NewSnapshotService(env environment.Env) *SnapshotService {
	return &SnapshotService{env: env}
}

// InvalidateSnapshot returns the new valid version ID for snapshots to be based off.
func (l *SnapshotService) InvalidateSnapshot(ctx context.Context, key *fcpb.SnapshotKey) (string, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	// Update the snapshot version to a random value. This will invalidate all past
	// snapshots that have a different version.
	newVersion, err := random.RandomString(10)
	if err != nil {
		return "", err
	}
	versionMetadata, err := anypb.New(&fcpb.SnapshotVersionMetadata{VersionId: newVersion})
	if err != nil {
		return "", err
	}
	versionMetadataActionResult := &repb.ActionResult{
		ExecutionMetadata: &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{versionMetadata},
		},
	}

	versionKey, err := SnapshotVersionKey(key)
	if err != nil {
		return "", err
	}

	acDigest := digest.NewACResourceName(versionKey, key.InstanceName, repb.DigestFunction_BLAKE3)

	// NOTE: We don't use `proxy_util.SetSkipRemote` here because the snapshot
	// version data should always live in the authoritative cache, to ensure that
	// any updates are applied universally.
	if err := cachetools.UploadActionResult(ctx, l.env.GetActionCacheClient(), acDigest, versionMetadataActionResult); err != nil {
		return "", err
	}
	log.CtxInfof(ctx, "Invalidated all snapshots for key %s. New version is %s.", key, newVersion)
	return newVersion, nil
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
	u, err := env.GetAuthenticator().AuthenticatedUser(ctx)
	if err == nil {
		gid = u.GetGroupID()
	} else if !authutil.IsAnonymousUserError(err) && !container.AnonymousRecyclingEnabled() {
		return "", err
	}
	return gid, nil
}

// UnpackContainerImage returns a ChunkedFile representing the given container
// image. The chunk dir is stored as a child directory of the given outDir.
//
// If the image is not cached, this func will split up the given ext4 image
// file and create a new ChunkedFile from it, then add that to cache.
func UnpackContainerImage(ctx context.Context, l *FileCacheLoader, instanceName, imageRef, imageExt4Path string, outDir string, chunkSize int64, remoteEnabled bool) (*copy_on_write.COWStore, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	// TODO: use an Action for this key instead (to allow remote snapshot
	// sharing).
	key := &fcpb.SnapshotKeySet{BranchKey: &fcpb.SnapshotKey{
		InstanceName:      instanceName,
		ConfigurationHash: hashStrings("__UnpackContainerImage", imageRef),
	}}

	snap, err := l.GetSnapshot(ctx, key, remoteEnabled)
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
	start := time.Now()
	cow, err := copy_on_write.ConvertFileToCOW(ctx, l.env, imageExt4Path, chunkSize, outDir, instanceName, remoteEnabled)
	if err != nil {
		return nil, status.WrapError(err, "convert image to COW")
	}
	// Add the COW to cache. This will also compute chunk digests.
	opts := &CacheSnapshotOptions{
		ChunkedFiles: map[string]*copy_on_write.COWStore{rootfsFileName: cow},
		Recycled:     false,
		Remote:       remoteEnabled,
	}
	if err := l.CacheSnapshot(ctx, key.GetBranchKey(), opts); err != nil {
		return nil, status.WrapError(err, "cache containerfs snapshot")
	}
	log.CtxDebugf(ctx, "Converted containerfs to COW in %s", time.Since(start))
	return cow, nil
}
