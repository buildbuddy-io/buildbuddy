package filecache

import (
	"container/list"
	"encoding/base64"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/fastcopy"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/google/uuid"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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
//   AddFile(d *repb.Digest, existingFilePath string)
// at which point they are tracked by digest in the fileCache LRU.
//
// When clients need to download a file, they can do so by calling:
//   FastLinkFile(d *repb.Digest, outputPath string)
// which will, if the file is present in fileCache, create a hardlink at
// outputPath and return true. If no file is found in the cache, fileCache
// will return false.

type fileCache struct {
	rootDir      string
	maxSizeBytes int64
	sizeBytes    int64
	evictList    *list.List
	items        map[string]*list.Element
	lock         sync.RWMutex
}

// entry is used to hold a value in the evictList
type entry struct {
	key       string
	sizeBytes int64
	value     string
}

func deleteDirContents(dir string) error {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, f := range files {
		if err := os.RemoveAll(filepath.Join(dir, f.Name())); err != nil {
			return err
		}
	}
	return nil
}

// NewFileCache constructs an fileCache with maxSize that will cache files
// in rootDir.
func NewFileCache(rootDir string, maxSizeBytes int64) (*fileCache, error) {
	if maxSizeBytes <= 0 {
		return nil, errors.New("Must provide a positive size")
	}
	// A little trashy, but there it is. We create our own directory inside
	// of the root, and we *delete everything in it* on startup. This
	// ensures that filecache does not just grow without bound across runs
	// of the program.
	rootDir = filepath.Join(rootDir, base64.StdEncoding.EncodeToString(uuid.NodeID()))
	if err := disk.EnsureDirectoryExists(rootDir); err != nil {
		return nil, err
	}
	if err := deleteDirContents(rootDir); err != nil {
		return nil, err
	}
	c := &fileCache{
		rootDir:      rootDir,
		sizeBytes:    0,
		maxSizeBytes: maxSizeBytes,
		evictList:    list.New(),
		items:        make(map[string]*list.Element),
	}
	return c, nil
}

func (c *fileCache) filePath(d *repb.Digest) string {
	return filepath.Join(c.rootDir, d.GetHash())
}

func (c *fileCache) FastLinkFile(d *repb.Digest, outputPath string) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if path, ok := c.get(d); ok {
		if err := fastcopy.FastCopy(path, outputPath); err != nil {
			log.Warningf("Error fast linking file: %s", err.Error())
			return false
		}
		return true
	}
	return false
}

func (c *fileCache) AddFile(d *repb.Digest, existingFilePath string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	fp := c.filePath(d)
	if err := fastcopy.FastCopy(existingFilePath, fp); err != nil {
		log.Warningf("Error adding file to filecache: %s", err.Error())
		return
	}
	c.add(d, fp)
}

// Add adds a value to the cache.  Returns true if an eviction occurred.
func (c *fileCache) add(d *repb.Digest, value string) bool {
	// Check for existing item. If it's there, re-up it and
	// overwrite the value.
	if ent, ok := c.items[d.GetHash()]; ok {
		c.evictList.MoveToFront(ent)
		return false
	}

	c.addElement(&entry{d.GetHash(), d.GetSizeBytes(), value})

	evicted := false
	for {
		evict := c.sizeBytes > c.maxSizeBytes
		if !evict {
			break
		}
		evicted = true
		c.removeOldest()
	}
	return evicted
}

// Get looks up a key's value from the cache.
func (c *fileCache) get(d *repb.Digest) (string, bool) {
	if ent, ok := c.items[d.GetHash()]; ok {
		c.evictList.MoveToFront(ent)
		if ent.Value.(*entry) == nil {
			return "", false
		}
		return ent.Value.(*entry).value, true
	}
	return "", false
}

// removeOldest removes the oldest item from the cache.
func (c *fileCache) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

func sizeOfEntry(e *entry) int64 {
	return e.sizeBytes
}

// removeElement is used to remove a given list element from the cache
func (c *fileCache) addElement(el *entry) {
	// Add new item
	ent := c.evictList.PushFront(el)
	c.items[el.key] = ent
	c.sizeBytes += sizeOfEntry(el)
}

// removeElement is used to remove a given list element from the cache
func (c *fileCache) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv, ok := e.Value.(*entry)
	if ok {
		delete(c.items, kv.key)
		c.sizeBytes -= sizeOfEntry(kv)
		syscall.Unlink(kv.value)
	}
}
