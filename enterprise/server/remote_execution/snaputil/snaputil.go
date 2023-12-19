package snaputil

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/genproto/googleapis/bytestream"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

var EnableLocalSnapshotSharing = flag.Bool("executor.enable_local_snapshot_sharing", false, "Enables local snapshot sharing for firecracker VMs. Also requires that executor.firecracker_enable_nbd is true.")
var EnableRemoteSnapshotSharing = flag.Bool("executor.enable_remote_snapshot_sharing", false, "Enables remote snapshot sharing for firecracker VMs. Also requires that executor.firecracker_enable_nbd and executor.firecracker_enable_uffd are true.")
var RemoteSnapshotReadonly = flag.Bool("executor.remote_snapshot_readonly", false, "Disables remote snapshot writes.")
var VerboseLogging = flag.Bool("executor.verbose_snapshot_logs", false, "Enables extra-verbose snapshot logs (even at debug log level)")

// ChunkSource represents how a snapshot chunk was initialized
type ChunkSource int

const (
	// MemoryFileName is the fixed file name of the memory snapshot file.
	// We rely on this name to locate the memory file in snapshots. Do not
	// change!
	MemoryFileName = "memory"

	// ChunkSourceUnmapped means the lazy chunk has not been initialized yet
	ChunkSourceUnmapped ChunkSource = iota
	// ChunkSourceHole means the chunk was initialized as a hole - i.e. it started
	// with empty data, though it may have been written to since
	ChunkSourceHole
	// ChunkSourceLocalFile means the chunk was created by splitting a snapshot file on disk
	// into chunks (i.e. this is the first time we're using this snapshot, and it's
	// not yet cached)
	ChunkSourceLocalFile
	// ChunkSourceLocalFilecache means the chunk was fetched from the local filecache
	ChunkSourceLocalFilecache
	// ChunkSourceRemoteCache means the chunk was fetched from the remote cache
	ChunkSourceRemoteCache
)

func (s ChunkSource) String() string {
	switch s {
	case ChunkSourceUnmapped:
		return "Unmapped"
	case ChunkSourceHole:
		return "Hole"
	case ChunkSourceLocalFile:
		return "LocalFile"
	case ChunkSourceLocalFilecache:
		return "LocalFilecache"
	case ChunkSourceRemoteCache:
		return "RemoteCache"
	default:
		return ""
	}
}

func GetArtifact(ctx context.Context, localCache interfaces.FileCache, bsClient bytestream.ByteStreamClient, remoteEnabled bool, d *repb.Digest, instanceName string, outputPath string) (ChunkSource, error) {
	node := &repb.FileNode{Digest: d}
	fetchedLocally := localCache.FastLinkFile(ctx, node, outputPath)
	if fetchedLocally {
		return ChunkSourceLocalFilecache, nil
	}

	if !*EnableRemoteSnapshotSharing || !remoteEnabled {
		return 0, status.UnavailableErrorf("snapshot artifact with digest %v not found in local cache", d)
	}

	if *VerboseLogging {
		start := time.Now()
		log.CtxDebugf(ctx, "Fetching snapshot artifact: instance=%q file=%s hash=%s", instanceName, StripChroot(outputPath), d.GetHash())
		defer func() { log.CtxDebugf(ctx, "Fetched remote snapshot artifact in %s", time.Since(start)) }()
	}

	// Fetch from remote cache
	f, err := os.Create(outputPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := digest.NewResourceName(d, instanceName, rspb.CacheType_CAS, repb.DigestFunction_BLAKE3)
	r.SetCompressor(repb.Compressor_ZSTD)
	if err := cachetools.GetBlob(ctx, bsClient, r, f); err != nil {
		return 0, status.WrapError(err, "remote fetch snapshot artifact")
	}

	// Save to local cache so next time fetching won't require a remote get
	if err := cacheLocally(ctx, localCache, d, outputPath); err != nil {
		log.Warningf("saving %s to local filecache failed: %s", outputPath, err)
	}

	return ChunkSourceRemoteCache, nil
}

func GetBytes(ctx context.Context, localCache interfaces.FileCache, bsClient bytestream.ByteStreamClient, remoteEnabled bool, d *repb.Digest, instanceName string, tmpDir string) ([]byte, error) {
	randStr, err := random.RandomString(10)
	if err != nil {
		return nil, err
	}
	tmpPath := filepath.Join(tmpDir, fmt.Sprintf("%s.%s.tmp", d.Hash, randStr))
	defer func() {
		if err := os.Remove(tmpPath); err != nil {
			log.CtxWarningf(ctx, "Failed to remove temp file in snaputil::GetBytes: %s", err)
		}
	}()

	if _, err := GetArtifact(ctx, localCache, bsClient, remoteEnabled, d, instanceName, tmpPath); err != nil {
		return nil, err
	}

	return os.ReadFile(tmpPath)
}

// Cache saves a file written to `path` to the local cache, and the remote cache
// if remote snapshot sharing is enabled
func Cache(ctx context.Context, localCache interfaces.FileCache, bsClient bytestream.ByteStreamClient, remoteEnabled bool, d *repb.Digest, remoteInstanceName string, path string) error {
	localCacheErr := cacheLocally(ctx, localCache, d, path)
	if !*EnableRemoteSnapshotSharing || *RemoteSnapshotReadonly || !remoteEnabled {
		return localCacheErr
	}

	if *VerboseLogging {
		start := time.Now()
		log.CtxDebugf(ctx, "Uploading snapshot artifact: instance=%q file=%s hash=%s", remoteInstanceName, StripChroot(path), d.GetHash())
		defer func() { log.CtxDebugf(ctx, "Uploaded snapshot artifact in %s", time.Since(start)) }()
	}

	rn := digest.NewResourceName(d, remoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_BLAKE3)
	rn.SetCompressor(repb.Compressor_ZSTD)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = cachetools.UploadFromReader(ctx, bsClient, rn, file)
	return err
}

// CacheBytes saves bytes to the cache.
// It does this by writing the bytes to a temporary file in tmpDir.
func CacheBytes(ctx context.Context, localCache interfaces.FileCache, bsClient bytestream.ByteStreamClient, remoteEnabled bool, d *repb.Digest, remoteInstanceName string, b []byte) error {
	// Write temp file containing bytes
	randStr, err := random.RandomString(10)
	if err != nil {
		return err
	}
	tmpPath := filepath.Join(localCache.TempDir(), fmt.Sprintf("%s.%s.tmp", d.Hash, randStr))
	if err := os.WriteFile(tmpPath, b, 0777); err != nil {
		return err
	}
	defer func() {
		if err := os.Remove(tmpPath); err != nil {
			log.CtxWarningf(ctx, "Failed to remove temp file: %s", err)
		}
	}()

	return Cache(ctx, localCache, bsClient, remoteEnabled, d, remoteInstanceName, tmpPath)
}

var chrootPrefix = regexp.MustCompile("^.*/firecracker/[^/]+/root/")

// StripChroot removes the jailer chroot directory from a given snapshot
// artifact path. Intended only for debugging purposes (to make paths more
// readable).
func StripChroot(path string) string {
	return chrootPrefix.ReplaceAllLiteralString(path, "")
}

// cacheLocally copies the data at `path` to the local filecache with
// the given `key`
func cacheLocally(ctx context.Context, localCache interfaces.FileCache, d *repb.Digest, path string) error {
	fileNode := &repb.FileNode{Digest: d}
	// If EnableLocalSnapshotSharing=true and we're computing real unloadedChunks,
	// the files will be immutable. We won't need to re-save them to file cache
	if !*EnableLocalSnapshotSharing || !localCache.ContainsFile(ctx, fileNode) {
		return localCache.AddFile(ctx, fileNode, path)
	}
	return nil
}

func ChunkSourceLabel(c ChunkSource) string {
	switch c {
	case ChunkSourceUnmapped:
		return "unmapped"
	case ChunkSourceLocalFile:
		return "local_file"
	case ChunkSourceLocalFilecache:
		return "local_filecache"
	case ChunkSourceHole:
		return "hole"
	case ChunkSourceRemoteCache:
		return "remote_cache"
	default:
		return "invalid_chunk_source"
	}
}
