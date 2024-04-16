package filecache

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/fastcopy"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// executableSuffix is a suffix appended to files in filecache which
	// have the executable bit set. This allows distinguishing between executable
	// and non-executable files which have the same content digests.
	executableSuffix = "executable"

	// hitMetricLabel is the prometheus metric label applied to filecache hits.
	hitMetricLabel = "hit"
	// missMetricLabel is the prometheus metric label applied to filecache misses.
	missMetricLabel = "miss"

	// Temporary directory under the filecache root.
	tmpDir = "_tmp"
)

var enableAlwaysClone = flag.Bool("executor.local_cache_always_clone", false, "If true, files from the filecache will always be cloned instead of hardlinked")

// fileCache implements a fixed-size, filesystem backed, LRU cache.
//
// The gist of it is that fileCache maintains an LRU of files recently
// requested, but rather than store the file bytes directly in memory
// or on disk, fileCache just keeps hardlinks to them. This ensures
// that when a previously downloaded temp file is deleted, if it is
// tracked in filecache, the underlying bytes are not deleted from the
// filesystem. When items are evicted from the LRU, the hardlinks owned
// by fileCache are unlinked, possibly freeing the OS to delete the file if
// no other references to it remain.
//
// Usage:
// When files are downloaded, clients notify fileCache by calling:
//
//	AddFile(d *repb.Digest, existingFilePath string)
//
// at which point they are tracked by digest in the fileCache LRU.
//
// When clients need to download a file, they can do so by calling:
//
//	FastLinkFile(d *repb.Digest, outputPath string)
//
// which will, if the file is present in fileCache, create a hardlink at
// outputPath and return true. If no file is found in the cache, fileCache
// will return false.
type fileCache struct {
	rootDir     string
	lock        sync.RWMutex
	l           interfaces.LRU[*entry]
	dirScanDone chan struct{}
}

// entry is used to hold a value in the evictList
type entry struct {
	// addedAtUsec is the time that the file was added to the file cache, in
	// microseconds since the Unix epoch.
	addedAtUsec int64
	// sizeBytes is the file size as reported by the original FileNode metadata
	// when the file was added to the file cache.
	sizeBytes int64
	// value is the absolute path to the file.
	value string
}

func sizeFn(v *entry) int64 {
	return v.sizeBytes
}

func evictFn(v *entry, reason lru.EvictionReason) {
	if err := syscall.Unlink(v.value); err != nil {
		log.Errorf("Failed to unlink filecache entry %q: %s", v.value, err)
	}
	if reason == lru.SizeEviction {
		age := time.Since(time.UnixMicro(v.addedAtUsec)).Microseconds()
		metrics.FileCacheLastEvictionAgeUsec.Set(float64(age))
	}
}

// NewFileCache constructs an fileCache with maxSize that will cache files
// in rootDir.
// If deleteContent is true, the root dir will be deleted and recreated.
func NewFileCache(rootDir string, maxSizeBytes int64, deleteContent bool) (*fileCache, error) {
	if maxSizeBytes <= 0 {
		return nil, errors.New("Must provide a positive size")
	}
	if deleteContent {
		log.Infof("Cleaning up filecache %q", rootDir)
		if err := disk.ForceRemove(context.Background(), rootDir); err != nil {
			return nil, err
		}
	}
	if err := disk.EnsureDirectoryExists(rootDir); err != nil {
		return nil, err
	}
	l, err := lru.NewLRU[*entry](&lru.Config[*entry]{MaxSize: maxSizeBytes, OnEvict: evictFn, SizeFn: sizeFn})
	if err != nil {
		return nil, err
	}
	c := &fileCache{
		rootDir:     rootDir,
		l:           l,
		dirScanDone: make(chan struct{}),
	}
	if err := os.RemoveAll(c.TempDir()); err != nil {
		return nil, status.WrapErrorf(err, "failed to clear filecache temp dir")
	}
	if err := os.MkdirAll(c.TempDir(), 0755); err != nil {
		return nil, status.WrapErrorf(err, "failed to create filecache temp dir")
	}
	go c.scanDir()
	return c, nil
}

func (c *fileCache) TempDir() string {
	return filepath.Join(c.rootDir, tmpDir)
}

func (c *fileCache) filecachePath(key string) string {
	return filepath.Join(c.rootDir, key)
}

func (c *fileCache) nodeFromPathAndSize(fullPath string, sizeBytes int64) (string, *repb.FileNode, error) {
	if !strings.HasPrefix(fullPath, c.rootDir) {
		return "", nil, status.FailedPreconditionErrorf("Path %q not in rootDir: %q", fullPath, c.rootDir)
	}

	subdirPath := strings.TrimPrefix(fullPath, c.rootDir)
	groupID, name := filepath.Split(subdirPath)
	groupID = strings.Trim(groupID, string(filepath.Separator))

	nameParts := strings.Split(name, ".")
	return groupID, &repb.FileNode{
		IsExecutable: len(nameParts) > 1 && nameParts[1] == executableSuffix,
		Digest: &repb.Digest{
			Hash:      nameParts[0],
			SizeBytes: sizeBytes,
		}}, nil
}

func (c *fileCache) scanDir() {
	dirCount := 0
	fileCount := 0
	scanStart := time.Now()
	walkFn := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			dirCount += 1
			return nil
		}
		fileCount += 1
		// addFileToGroup uses the physical size, not the digest size, so just
		// pass 0 for size here.
		groupID, node, err := c.nodeFromPathAndSize(path, 0)
		if err != nil {
			return err
		}
		return c.addFileToGroup(groupID, node, path)
	}
	if err := filepath.WalkDir(c.rootDir, walkFn); err != nil {
		log.Errorf("Error reading existing filecache dir: %q: %s", c.rootDir, err)
	}
	c.lock.Lock()
	lruSize := c.l.Size()
	c.lock.Unlock()

	log.Infof("filecache(%q) scanned %d dirs, %d files in %s. Total tracked bytes: %d", c.rootDir, dirCount, fileCount, time.Since(scanStart), lruSize)
	close(c.dirScanDone)
}

func groupSpecificKey(groupID string, node *repb.FileNode) string {
	suffix := ""
	if node.GetIsExecutable() {
		suffix = "." + executableSuffix
	}
	return groupID + "/" + node.GetDigest().GetHash() + suffix
}

func groupIDStringFromContext(ctx context.Context) string {
	if c, err := claims.ClaimsFromContext(ctx); err == nil {
		return c.GroupID
	}
	return interfaces.AuthAnonymousUser
}

func key(ctx context.Context, node *repb.FileNode) string {
	groupID := groupIDStringFromContext(ctx)
	k := groupSpecificKey(groupID, node)
	return k
}

func (c *fileCache) FastLinkFile(ctx context.Context, node *repb.FileNode, outputPath string) (hit bool) {
	defer func() {
		label := missMetricLabel
		if hit {
			label = hitMetricLabel
		}
		metrics.FileCacheRequests.
			With(prometheus.Labels{metrics.FileCacheRequestStatusLabel: label}).
			Inc()
	}()

	groupID := groupIDStringFromContext(ctx)
	key := groupSpecificKey(groupID, node)

	c.lock.Lock()
	v, ok := c.l.Get(key)
	c.lock.Unlock()
	if !ok {
		return false
	}

	if groupID == interfaces.AuthAnonymousUser || *enableAlwaysClone {
		if err := fastcopy.Clone(v.value, outputPath); err != nil {
			log.Warningf("Error fast linking file: %s", err.Error())
			return false
		}
	} else {
		if err := fastcopy.FastCopy(v.value, outputPath); err != nil {
			log.Warningf("Error fast linking file: %s", err.Error())
			return false
		}
	}
	return true
}

func (c *fileCache) addFileToGroup(groupID string, node *repb.FileNode, existingFilePath string) error {
	// Remove any existing entry. We can't update in-place because if we
	// overwrite an existing link with different contents, all pointers
	// to the old link would suddenly change to point to the new content,
	// which is not good.
	k := groupSpecificKey(groupID, node)

	info, err := os.Stat(existingFilePath)
	if err != nil {
		return status.WrapError(err, "stat")
	}
	sizeOnDisk, err := disk.EstimatedFileDiskUsage(info)
	if err != nil {
		return status.WrapError(err, "estimate disk usage")
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	c.l.Remove(k)

	fp := c.filecachePath(k)
	if err := disk.EnsureDirectoryExists(filepath.Dir(fp)); err != nil {
		return err
	}
	if err := fastcopy.FastCopy(existingFilePath, fp); err != nil {
		log.Errorf("Error fastcopying %q => %q: %s", existingFilePath, fp, err)
		return status.WrapError(err, "adding file to filecache")
	}
	e := &entry{
		addedAtUsec: time.Now().UnixMicro(),
		sizeBytes:   sizeOnDisk,
		value:       fp,
	}
	metrics.FileCacheAddedFileSizeBytes.Observe(float64(e.sizeBytes))
	success := c.l.Add(k, e)
	if !success {
		return status.InternalErrorf("could not add key %s to filecache lru", k)
	}
	return nil
}

func (c *fileCache) AddFile(ctx context.Context, node *repb.FileNode, existingFilePath string) error {
	groupID := groupIDStringFromContext(ctx)
	// Locking happens in addFileToGroup().
	return c.addFileToGroup(groupID, node, existingFilePath)
}

func (c *fileCache) ContainsFile(ctx context.Context, node *repb.FileNode) bool {
	k := key(ctx, node)

	c.lock.Lock()
	defer c.lock.Unlock()
	return c.l.Contains(k)
}

func (c *fileCache) DeleteFile(ctx context.Context, node *repb.FileNode) bool {
	k := key(ctx, node)

	c.lock.Lock()
	defer c.lock.Unlock()
	return c.l.Remove(k)
}

func (c *fileCache) WaitForDirectoryScanToComplete() {
	<-c.dirScanDone
}

// Read atomically reads a file from filecache.
func (c *fileCache) Read(ctx context.Context, node *repb.FileNode) ([]byte, error) {
	tmp, err := c.tempPath(node.GetDigest().GetHash())
	if err != nil {
		return nil, err
	}
	if !c.FastLinkFile(ctx, node, tmp) {
		return nil, status.NotFoundErrorf("digest %s not found", node.GetDigest().GetHash())
	}
	defer func() {
		if err := os.Remove(tmp); err != nil {
			log.Warningf("Failed to remove filecache temp file: %s", err)
		}
	}()
	return os.ReadFile(tmp)
}

// Write atomically writes the given bytes to filecache.
func (c *fileCache) Write(ctx context.Context, node *repb.FileNode, b []byte) (n int, err error) {
	tmp, err := c.tempPath(node.GetDigest().GetHash())
	if err != nil {
		return 0, err
	}
	f, err := os.Create(tmp)
	if err != nil {
		return 0, status.InternalErrorf("filecache temp file creation failed: %s", err)
	}
	defer func() {
		if err := os.Remove(tmp); err != nil {
			log.Warningf("Failed to remove filecache temp file: %s", err)
		}
	}()
	n, err = f.Write(b)
	if err != nil {
		return n, err
	}
	if err := c.AddFile(ctx, node, tmp); err != nil {
		return 0, err
	}
	return n, nil
}

func (c *fileCache) tempPath(name string) (string, error) {
	randStr, err := random.RandomString(10)
	if err != nil {
		return "", err
	}
	return filepath.Join(c.TempDir(), fmt.Sprintf("%s.%s.tmp", name, randStr)), nil
}
