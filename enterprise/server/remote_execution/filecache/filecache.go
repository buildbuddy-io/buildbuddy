package filecache

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/fastcopy"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
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
	l           interfaces.LRU
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

func sizeFn(value interface{}) int64 {
	if v, ok := value.(*entry); ok {
		return v.sizeBytes
	}
	return 0
}

func evictFn(value interface{}) {
	if v, ok := value.(*entry); ok {
		syscall.Unlink(v.value)
		age := time.Since(time.UnixMicro(v.addedAtUsec)).Microseconds()
		metrics.FileCacheLastEvictionAgeUsec.Set(float64(age))
	}
}

// NewFileCache constructs an fileCache with maxSize that will cache files
// in rootDir.
func NewFileCache(rootDir string, maxSizeBytes int64) (*fileCache, error) {
	if maxSizeBytes <= 0 {
		return nil, errors.New("Must provide a positive size")
	}
	hostID, err := uuid.GetHostID()
	if err != nil {
		log.Warning("Unable to get stable BuildBuddy HostID; filecache will not be reused across process restarts.")
		hostID = uuid.GetFailsafeHostID()
	}
	rootDir = filepath.Join(rootDir, hostID)
	if err := disk.EnsureDirectoryExists(rootDir); err != nil {
		return nil, err
	}
	l, err := lru.NewLRU(&lru.Config{MaxSize: maxSizeBytes, OnEvict: evictFn, SizeFn: sizeFn})
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

func (c *fileCache) filecachePath(node *repb.FileNode) string {
	return filepath.Join(c.rootDir, key(node))
}

func (c *fileCache) nodeFromPathAndSize(fullPath string, sizeBytes int64) (*repb.FileNode, error) {
	if !strings.HasPrefix(fullPath, c.rootDir) {
		return nil, status.FailedPreconditionErrorf("Path %q not in rootDir: %q", fullPath, c.rootDir)
	}

	name := filepath.Base(fullPath)
	nameParts := strings.Split(name, ".")
	return &repb.FileNode{
		IsExecutable: len(nameParts) > 1 && nameParts[1] == executableSuffix,
		Digest: &repb.Digest{
			Hash:      nameParts[0],
			SizeBytes: sizeBytes,
		}}, nil
}

func (c *fileCache) scanDir() {
	scanCount := 0
	scanStart := time.Now()
	walkFn := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		scanCount += 1
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		node, err := c.nodeFromPathAndSize(path, info.Size())
		if err != nil {
			return err
		}
		c.AddFile(node, path)
		return nil
	}
	if err := filepath.WalkDir(c.rootDir, walkFn); err != nil {
		log.Errorf("Error reading existing filecache dir: %q: %s", c.rootDir, err)
	}
	c.lock.Lock()
	lruSize := c.l.Size()
	c.lock.Unlock()

	log.Infof("filecache(%q) scanned %d files in %s. Total tracked bytes: %d", c.rootDir, scanCount, time.Since(scanStart), lruSize)
	close(c.dirScanDone)
}

func key(node *repb.FileNode) string {
	suffix := ""
	if node.GetIsExecutable() {
		suffix = "." + executableSuffix
	}
	return node.GetDigest().GetHash() + suffix
}

func (c *fileCache) FastLinkFile(node *repb.FileNode, outputPath string) (hit bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	defer func() {
		label := missMetricLabel
		if hit {
			label = hitMetricLabel
		}
		metrics.FileCacheRequests.
			With(prometheus.Labels{metrics.FileCacheRequestStatusLabel: label}).
			Inc()
	}()

	e, ok := c.l.Get(key(node))
	if !ok {
		return false
	}
	v, ok := e.(*entry)
	if !ok {
		return false
	}
	if err := fastcopy.FastCopy(v.value, outputPath); err != nil {
		log.Warningf("Error fast linking file: %s", err.Error())
		return false
	}
	return true
}

func (c *fileCache) AddFile(node *repb.FileNode, existingFilePath string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Remove any existing entry. We can't update in-place because if we
	// overwrite an existing link with different contents, all pointers
	// to the old link would suddenly change to point to the new content,
	// which is not good.
	k := key(node)
	c.l.Remove(k)

	fp := c.filecachePath(node)
	if err := fastcopy.FastCopy(existingFilePath, fp); err != nil {
		log.Warningf("Error adding file to filecache: %s", err.Error())
		return
	}
	e := &entry{
		addedAtUsec: time.Now().UnixMicro(),
		sizeBytes:   node.GetDigest().GetSizeBytes(),
		value:       fp,
	}
	metrics.FileCacheAddedFileSizeBytes.Observe(float64(e.sizeBytes))
	c.l.Add(k, e)
}

func (c *fileCache) DeleteFile(node *repb.FileNode) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.l.Remove(key(node))
}

func (c *fileCache) WaitForDirectoryScanToComplete() {
	<-c.dirScanDone
}
