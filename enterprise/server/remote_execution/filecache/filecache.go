package filecache

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
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

var (
	enableAlwaysClone   = flag.Bool("executor.local_cache_always_clone", false, "If true, files from the filecache will always be cloned instead of hardlinked")
	includeSubdirPrefix = flag.Bool("executor.include_subdir_prefix", false, "If true, store files under subdirs named by the first 4 chars of file digest")
)

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
	lock        sync.Mutex
	l           *lru.LRU[*entry]
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
}

func sizeFn(v *entry) int64 {
	return v.sizeBytes
}

func evictFn(rootDir string) func(string, *entry, lru.EvictionReason) {
	return func(key string, v *entry, reason lru.EvictionReason) {
		fp := filecachePath(rootDir, key)
		if err := syscall.Unlink(fp); err != nil {
			log.Errorf("Failed to unlink filecache entry %q: %s", fp, err)
		}
		if reason == lru.SizeEviction {
			age := time.Since(time.UnixMicro(v.addedAtUsec)).Microseconds()
			metrics.FileCacheLastEvictionAgeUsec.Set(float64(age))
		}
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
	l, err := lru.NewLRU[*entry](&lru.Config[*entry]{MaxSize: maxSizeBytes, OnEvict: evictFn(rootDir), SizeFn: sizeFn})
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

func filecachePath(rootDir, key string) string {
	// Don't use filepath.Join since it's relatively slow and allocates more.
	if *includeSubdirPrefix {
		groupDir, file := filepath.Split(key)
		return rootDir + "/" + groupDir + file[:4] + "/" + file
	}
	return rootDir + "/" + key
}

const sep = string(filepath.Separator)

func (c *fileCache) nodeFromPathAndSize(fullPath string, sizeBytes int64) (string, *repb.FileNode, error) {
	if !strings.HasPrefix(fullPath, c.rootDir) {
		return "", nil, status.FailedPreconditionErrorf("Path %q not in rootDir: %q", fullPath, c.rootDir)
	}

	subdirPath := strings.TrimPrefix(fullPath, c.rootDir)
	groupID, name := filepath.Split(subdirPath)
	groupID = strings.Trim(groupID, sep)

	// Backwards compatible: scan files that are written in the new
	// format OR the old format.
	//
	// old format: GROUP/abcdefghijklmnopqrstuv
	// new format: GROUP/abcd/abcdefghijklmnopqrstuv
	if strings.Contains(groupID, sep) {
		groupID = strings.TrimSuffix(groupID, sep)
		groupID = strings.Trim(filepath.Dir(groupID), sep)
	}

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
		if err := c.addFileToGroup(groupID, node, path); err != nil {
			// Any errors here are unexpected - this addFileToGroup call should
			// just be updating LRU state. There is a chance that it will
			// trigger an eviction, but any error from the associated unlink
			// operation is just logged and not returned.
			return status.WrapError(err, "add file from initial scan")
		}
		return nil
	}

	entries, err := os.ReadDir(c.rootDir)
	if err != nil && !os.IsNotExist(err) {
		log.Errorf("Error reading existing filecache dir: %q: %s", c.rootDir, err)
	}
	for _, entry := range entries {
		// Only scan the group-specific dirs (ignore _tmp and other dirs)
		if entry.Name() != "ANON" && !strings.HasPrefix(entry.Name(), "GR") {
			continue
		}
		if !entry.IsDir() {
			continue
		}

		groupDir := filepath.Join(c.rootDir, entry.Name())
		if err := filepath.WalkDir(groupDir, walkFn); err != nil {
			log.Errorf("Error scanning filecache dir: %q: %s", groupDir, err)
		}
	}

	c.lock.Lock()
	lruSize := c.l.Size()
	c.lock.Unlock()

	log.Infof("filecache(%q) scanned %d dirs, %d files in %s. Total tracked bytes: %d", c.rootDir, dirCount, fileCount, time.Since(scanStart), lruSize)
	close(c.dirScanDone)
}

func groupSpecificKey(groupID string, node *repb.FileNode) string {
	if node.GetIsExecutable() {
		return groupID + "/" + node.GetDigest().GetHash() + "." + executableSuffix
	}
	return groupID + "/" + node.GetDigest().GetHash()
}

func groupIDStringFromContext(ctx context.Context) string {
	if c, err := claims.ClaimsFromContext(ctx); err == nil {
		if len(c.GroupID) == 0 {
			log.CtxWarning(ctx, "Empty group id")
		}
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
	ok := c.l.Contains(key)
	c.lock.Unlock()
	if !ok {
		return false
	}
	start := time.Now()
	if err := cloneOrLink(groupID, filecachePath(c.rootDir, key), outputPath); err != nil {
		log.Warningf("Failed to link file from cache: %s", err)
		return false
	}
	metrics.FileCacheLinkLatencyUsec.Observe(float64(time.Since(start).Microseconds()))
	return true
}

func (c *fileCache) Open(ctx context.Context, node *repb.FileNode) (f *os.File, err error) {
	defer func() {
		label := missMetricLabel
		if f != nil {
			label = hitMetricLabel
		}
		metrics.FileCacheRequests.
			With(prometheus.Labels{metrics.FileCacheRequestStatusLabel: label}).
			Inc()
	}()

	groupID := groupIDStringFromContext(ctx)
	key := groupSpecificKey(groupID, node)

	c.lock.Lock()
	ok := c.l.Contains(key)
	c.lock.Unlock()
	if !ok {
		return nil, status.NotFoundError("not found")
	}
	return os.Open(filecachePath(c.rootDir, key))
}

func (c *fileCache) addFileToGroup(groupID string, node *repb.FileNode, existingFilePath string) error {
	// Remove any existing entry. We can't update in-place because if we
	// overwrite an existing link with different contents, all pointers
	// to the old link would suddenly change to point to the new content,
	// which is not good.

	info, err := os.Stat(existingFilePath)
	if err != nil {
		return wrapOSError(err, "stat")
	}
	sizeOnDisk, err := disk.EstimatedFileDiskUsage(info)
	if err != nil {
		return wrapOSError(err, "estimate disk usage")
	}

	k := groupSpecificKey(groupID, node)
	fp := filecachePath(c.rootDir, k)

	// Ensure the parent directory exists. (We can skip this if the source and
	// dest are the same, which happens during the initial directory scan.)
	// TODO(vanja) Consider doing this ahead of time, or just once per
	// group. With includeSubdirPrefix=false, this could make the Add path
	// 7% faster, but it's more complicated with includeSubdirPrefix=true.
	if fp != existingFilePath {
		if err := disk.EnsureDirectoryExists(filepath.Dir(fp)); err != nil {
			return err
		}
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// If we're adding the file from the path where it should already exist
	// (i.e. during the initial directory scan), and if it's already tracked in
	// the LRU (i.e. another action added it concurrently with the scan), then
	// short-circuit to avoid evicting the file.
	if fp == existingFilePath {
		if c.l.Contains(k) {
			return nil
		}
	} else {
		// If the file being added is not already at the path where we expect it
		// (i.e. it's being added from some action workspace), then remove and
		// unlink any existing entry, then hardlink to the destination path.
		c.l.Remove(k)
		if err := cloneOrLink(groupID, existingFilePath, fp); err != nil {
			return err
		}

		// If the file being added is inside the filecache dir, and it
		// is stored in an "old-style" location, then remove it.
		if strings.HasPrefix(existingFilePath, c.rootDir) && filepath.Base(fp) == filepath.Base(existingFilePath) {
			if err := syscall.Unlink(existingFilePath); err != nil {
				log.Errorf("Failed to unlink existing filecache path: %q: %s", existingFilePath, err)
			}
		}
	}

	e := &entry{
		addedAtUsec: time.Now().UnixMicro(),
		sizeBytes:   sizeOnDisk,
	}
	metrics.FileCacheAddedFileSizeBytes.Observe(float64(e.sizeBytes))
	metrics.FileCacheAddedFileBytesCount.With(prometheus.Labels{
		metrics.GroupID: groupID,
	}).Add(float64(e.sizeBytes))
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

type verifiedWriter struct {
	ctx context.Context
	fc  *fileCache

	csum hash.Hash
	node *repb.FileNode
	file *os.File
}

func newVerifiedWriter(ctx context.Context, fc *fileCache, node *repb.FileNode, digestFunction repb.DigestFunction_Value, file *os.File) (*verifiedWriter, error) {
	csum, err := digest.HashForDigestType(digestFunction)
	if err != nil {
		return nil, err
	}
	return &verifiedWriter{
		ctx:  ctx,
		fc:   fc,
		csum: csum,
		node: node,
		file: file,
	}, nil
}

func (v *verifiedWriter) Write(p []byte) (n int, err error) {
	if v.file == nil {
		return 0, status.FailedPreconditionError("writer is closed")
	}
	if _, err := v.csum.Write(p); err != nil {
		return 0, err
	}
	return v.file.Write(p)
}

func (v *verifiedWriter) Commit() error {
	if v.file == nil {
		return status.FailedPreconditionError("writer is closed")
	}
	hashStr := fmt.Sprintf("%x", v.csum.Sum(nil))
	if v.node.GetDigest().GetHash() != hashStr {
		return status.DataLossErrorf("data checksum %q does not match expected checksum %q", hashStr, v.node.GetDigest().GetHash())
	}
	defer v.Close()
	return v.fc.AddFile(v.ctx, v.node, v.file.Name())
}

func (v *verifiedWriter) Close() error {
	if v.file == nil {
		return nil
	}
	defer func() {
		if err := os.Remove(v.file.Name()); err != nil {
			log.Warningf("Failed to remove filecache temp file: %s", err)
		}
		v.file = nil
	}()
	return v.file.Close()
}

func (c *fileCache) Writer(ctx context.Context, node *repb.FileNode, digestFunction repb.DigestFunction_Value) (interfaces.CommittedWriteCloser, error) {
	tmp, err := c.tempPath(node.GetDigest().GetHash())
	if err != nil {
		return nil, err
	}

	perms := 0644
	if node.IsExecutable {
		perms |= 0111
	}

	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE, os.FileMode(perms))
	if err != nil {
		return nil, status.InternalErrorf("filecache temp file creation failed: %s", err)
	}
	return newVerifiedWriter(ctx, c, node, digestFunction, f)
}

func (c *fileCache) tempPath(name string) (string, error) {
	randStr, err := random.RandomString(10)
	if err != nil {
		return "", err
	}
	return filepath.Join(c.TempDir(), fmt.Sprintf("%s.%s.tmp", name, randStr)), nil
}

func cloneOrLink(groupID, source, destination string) error {
	if groupID == interfaces.AuthAnonymousUser || *enableAlwaysClone {
		if err := fastcopy.Clone(source, destination); err != nil {
			return wrapOSError(err, "clone")
		}
	} else {
		if err := fastcopy.FastCopy(source, destination); err != nil {
			return wrapOSError(err, "fastcopy")
		}
	}
	return nil
}

func wrapOSError(err error, message string) error {
	if os.IsNotExist(err) {
		return status.NotFoundErrorf("%s: %s", message, err)
	}
	return status.InternalErrorf("%s: %s", message, err)
}
