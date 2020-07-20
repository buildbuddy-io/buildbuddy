package memory_cache

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// lru implements a non-thread safe fixed size LRU cache
type lru struct {
	maxSizeBytes int64
	sizeBytes    int64
	evictList    *list.List
	items        map[string]*list.Element
}

// entry is used to hold a value in the evictList
type entry struct {
	key   string
	value []byte
}

// NewLRU constructs an lru with maxSize
func newlru(maxSizeBytes int64) (*lru, error) {
	if maxSizeBytes <= 0 {
		return nil, errors.New("Must provide a positive size")
	}
	c := &lru{
		sizeBytes:    0,
		maxSizeBytes: maxSizeBytes,
		evictList:    list.New(),
		items:        make(map[string]*list.Element),
	}
	return c, nil
}

// Purge is used to completely clear the cache.
func (c *lru) Purge() {
	for k := range c.items {
		delete(c.items, k)
	}
	c.evictList.Init()
}

// Add adds a value to the cache.  Returns true if an eviction occurred.
func (c *lru) Add(key string, value []byte) (evicted bool) {
	// Check for existing item. If it's there, re-up it and
	// overwrite the value.
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*entry).value = value
		return false
	}

	c.addElement(&entry{key, value})

	evicted = false
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
func (c *lru) Get(key string) (value []byte, ok bool) {
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		if ent.Value.(*entry) == nil {
			return nil, false
		}
		return ent.Value.(*entry).value, true
	}
	return
}

// Contains checks if a key is in the cache, without updating the recent-ness
// or deleting it for being stale.
func (c *lru) Contains(key string) (ok bool) {
	_, ok = c.items[key]
	return ok
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *lru) Remove(key string) (present bool) {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		return true
	}
	return false
}

// RemoveOldest removes the oldest item from the cache.
func (c *lru) RemoveOldest() (key string, value []byte, ok bool) {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
		kv := ent.Value.(*entry)
		return kv.key, kv.value, true
	}
	return "", nil, false
}

// Len returns the number of items in the cache.
func (c *lru) Len() int {
	return c.evictList.Len()
}

// Len returns the number of items in the cache.
func (c *lru) Size() int64 {
	return c.sizeBytes
}

// removeOldest removes the oldest item from the cache.
func (c *lru) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

func sizeOfEntry(e *entry) int64 {
	return int64(len([]byte(e.key)) + len(e.value))
}

// removeElement is used to remove a given list element from the cache
func (c *lru) addElement(el *entry) {
	// Add new item
	ent := c.evictList.PushFront(el)
	c.items[el.key] = ent
	c.sizeBytes += sizeOfEntry(el)
}

// removeElement is used to remove a given list element from the cache
func (c *lru) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*entry)
	delete(c.items, kv.key)
	c.sizeBytes -= sizeOfEntry(kv)
}

type MemoryCache struct {
	l      *lru
	lock   *sync.RWMutex
	prefix string
}

func NewMemoryCache(maxSizeBytes int64) (*MemoryCache, error) {
	lru, err := newlru(maxSizeBytes)
	if err != nil {
		return nil, err
	}
	return &MemoryCache{
		l:    lru,
		lock: &sync.RWMutex{},
	}, nil
}

func (m *MemoryCache) key(ctx context.Context, d *repb.Digest) (string, error) {
	hash, err := digest.Validate(d)
	if err != nil {
		return "", err
	}
	return perms.UserPrefixFromContext(ctx) + m.prefix + hash, nil
}

func (m *MemoryCache) WithPrefix(prefix string) interfaces.Cache {
	newPrefix := filepath.Join(append(filepath.SplitList(m.prefix), prefix)...)
	if len(newPrefix) > 0 && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}

	return &MemoryCache{
		l:      m.l,
		lock:   m.lock,
		prefix: newPrefix,
	}
}

func (m *MemoryCache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	k, err := m.key(ctx, d)
	if err != nil {
		return false, err
	}
	m.lock.Lock()
	contains := m.l.Contains(k)
	m.lock.Unlock()
	return contains, nil
}

func (m *MemoryCache) ContainsMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	foundMap := make(map[*repb.Digest]bool, len(digests))
	// No parallelism here either. Not necessary for an in-memory cache.
	for _, d := range digests {
		ok, err := m.Contains(ctx, d)
		if err != nil {
			return nil, err
		}
		foundMap[d] = ok
	}
	return foundMap, nil
}

func (m *MemoryCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	k, err := m.key(ctx, d)
	if err != nil {
		return nil, err
	}
	m.lock.Lock()
	value, ok := m.l.Get(k)
	m.lock.Unlock()
	if !ok {
		return nil, status.NotFoundError(fmt.Sprintf("Key %s not found", d))
	}
	return value, nil
}

func (m *MemoryCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	foundMap := make(map[*repb.Digest][]byte, len(digests))
	// No parallelism here either. Not necessary for an in-memory cache.
	for _, d := range digests {
		data, err := m.Get(ctx, d)
		if err != nil {
			return nil, err
		}
		foundMap[d] = data
	}
	return foundMap, nil
}

func (m *MemoryCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	k, err := m.key(ctx, d)
	if err != nil {
		return err
	}
	m.lock.Lock()
	m.l.Add(k, data)
	m.lock.Unlock()
	return nil
}

func (m *MemoryCache) Delete(ctx context.Context, d *repb.Digest) error {
	k, err := m.key(ctx, d)
	if err != nil {
		return err
	}
	m.lock.Lock()
	m.l.Remove(k)
	m.lock.Unlock()
	return nil
}

// Low level interface used for seeking and stream-writing.
func (m *MemoryCache) Reader(ctx context.Context, d *repb.Digest, offset int64) (io.Reader, error) {
	// Locking and key prefixing are handled in Get.
	buf, err := m.Get(ctx, d)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(buf)
	r.Seek(offset, 0)
	length := d.GetSizeBytes()
	if length > 0 {
		return io.LimitReader(r, length), nil
	}
	return r, nil
}

type closeFn func(b *bytes.Buffer) error
type setOnClose struct {
	*bytes.Buffer
	c closeFn
}

func (d *setOnClose) Close() error {
	return d.c(d.Buffer)
}

func (m *MemoryCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	var buffer bytes.Buffer
	return &setOnClose{
		Buffer: &buffer,
		c: func(b *bytes.Buffer) error {
			// Locking and key prefixing are handled in Set.
			return m.Set(ctx, d, b.Bytes())
		},
	}, nil
}

func (m *MemoryCache) Start() error {
	return nil
}

func (m *MemoryCache) Stop() error {
	return nil
}
