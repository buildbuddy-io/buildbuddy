package snaploader_utils

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

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

func FetchArtifact(ctx context.Context, localCache interfaces.FileCache, bsClient bytestream.ByteStreamClient, d *repb.Digest, instanceName string, outputPath string) error {
	node := &repb.FileNode{Digest: d}
	fetchedLocally := localCache.FastLinkFile(node, outputPath)
	if fetchedLocally {
		return nil
	}

	if !*EnableRemoteSnapshotSharing {
		return status.UnavailableErrorf("snapshot artifact with digest %v not found in local cache", d)
	}

	// Fetch from remote cache
	buf := bytes.NewBuffer(make([]byte, 0, d.GetSizeBytes()))
	r := digest.NewResourceName(d, instanceName, rspb.CacheType_CAS, repb.DigestFunction_BLAKE3)
	if err := cachetools.GetBlob(ctx, bsClient, r, buf); err != nil {
		return status.WrapError(err, "remote fetch snapshot artifact")
	}

	// Write file to outputDir so it can be used by the VM
	writeErr := writeFile(outputPath, buf.Bytes())

	// Save to local cache so next time fetching won't require a remote get
	if err := CacheLocally(localCache, d, outputPath); err != nil {
		log.Warningf("saving %s to local filecache failed: %s", outputPath, err)
	}

	return writeErr
}

func writeFile(path string, b []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		return err
	}
	return nil
}

// Cache saves a file written to `path` to the local cache, and the remote cache
// if remote snapshot sharing is enabled
func Cache(ctx context.Context, localCache interfaces.FileCache, bsClient bytestream.ByteStreamClient, d *repb.Digest, remoteInstanceName string, path string) error {
	localCacheErr := CacheLocally(localCache, d, path)
	if !*EnableRemoteSnapshotSharing {
		return localCacheErr
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
func CacheBytes(ctx context.Context, localCache interfaces.FileCache, bsClient bytestream.ByteStreamClient, d *repb.Digest, remoteInstanceName string, b []byte, tmpDir string) error {
	// Write temp file containing bytes
	randStr, err := random.RandomString(10)
	if err != nil {
		return err
	}
	tmpPath := filepath.Join(tmpDir, fmt.Sprintf("%s.%s.tmp", d.Hash, randStr))
	if err := writeFile(tmpPath, b); err != nil {
		return err
	}
	defer func() {
		if err := os.Remove(tmpPath); err != nil {
			log.Warningf("Failed to remove temp file: %s", err)
		}
	}()

	return Cache(ctx, localCache, bsClient, d, remoteInstanceName, tmpPath)
}

// CacheLocally copies the data at `path` to the local filecache with
// the given `key`
func CacheLocally(localCache interfaces.FileCache, d *repb.Digest, path string) error {
	fileNode := &repb.FileNode{Digest: d}
	// If EnableLocalSnapshotSharing=true and we're computing real unloadedChunks,
	// the files will be immutable. We won't need to re-save them to file cache
	if !*EnableLocalSnapshotSharing || !localCache.ContainsFile(fileNode) {
		return localCache.AddFile(fileNode, path)
	}
	return nil
}
